import httpx
from fastapi import FastAPI, Request, Response
import os
from urllib.parse import quote
from httpx_auth import AWS4Auth
import boto3
from datetime import datetime, timezone
import logging
import sys
import xml.etree.ElementTree as ET
from collections import defaultdict

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    stream=sys.stdout,
    format="%(asctime)s %(levelname)s %(name)s %(message)s"
)

app = FastAPI()

# Record proxy startup time (UTC)
START_TIME = datetime.now(timezone.utc)

# Configurable base URLs
OVERLAY_S3_URL = os.environ.get("OVERLAY_S3_URL", "http://overlay-s3.local")
ORIGIN_S3_URL = os.environ.get("ORIGIN_S3_URL", "https://s3.amazonaws.com")
OVERLAY_BUCKET = os.environ.get("OVERLAY_BUCKET", "overlay")

# Create a boto3 session (using default configuration)
session = boto3.Session()
default_credentials = session.get_credentials()

# For origin requests, always use the default credentials from boto3.
origin_credentials = default_credentials

# For overlay requests, check if environment variables prefixed with OVERLAY_AWS_ exist.
overlay_access_key = os.environ.get("OVERLAY_AWS_ACCESS_KEY_ID")
overlay_secret_key = os.environ.get("OVERLAY_AWS_SECRET_ACCESS_KEY")
overlay_session_token = os.environ.get("OVERLAY_AWS_SESSION_TOKEN")

if overlay_access_key and overlay_secret_key:
    # Use overlay credentials from the environment.
    # We mimic the structure of boto3's credentials by creating an object with access_key, secret_key, and token.
    class OverlayCredentials:
        pass
    overlay_creds = OverlayCredentials()
    overlay_creds.access_key = overlay_access_key
    overlay_creds.secret_key = overlay_secret_key
    overlay_creds.token = overlay_session_token
    overlay_credentials = overlay_creds
else:
    # Fallback to the default boto3 session credentials.
    overlay_credentials = default_credentials

# Build AWS4Auth objects.
origin_aws_auth = AWS4Auth(
    access_id=origin_credentials.access_key,
    secret_key=origin_credentials.secret_key,
    region=os.environ.get("AWS_REGION", "us-east-1"),
    service="s3",
    security_token=origin_credentials.token
)

overlay_aws_auth = AWS4Auth(
    access_id=overlay_credentials.access_key,
    secret_key=overlay_credentials.secret_key,
    region=os.environ.get("AWS_REGION", "us-east-1"),
    service="s3",
    security_token=overlay_credentials.token
)

# Unsigned client for origin S3 (or re-sign with origin_aws_auth when needed)
client = httpx.AsyncClient(follow_redirects=True)

# Signed client for overlay S3 using overlay_aws_auth
signed_client = httpx.AsyncClient(auth=overlay_aws_auth, follow_redirects=True)

def rewrite_overlay_path(original_path: str) -> str:
    parts = original_path.strip("/").split("/", 1)
    if len(parts) == 2:
        bucket, key = parts
    else:
        bucket, key = parts[0], ""
    return f"{OVERLAY_BUCKET}/{bucket}/{key}"

async def handle_delete_workaround(overlay_url: str, overlay_headers: dict, body: bytes) -> httpx.Response:
    """
    Handle DELETE requests using a facilitator object.
    1. Create a zero-length facilitator object with the header x-rtwa-delete-marker-facilitator.
    2. Then delete that object so only a delete marker remains.
    """
    facilitator_headers = overlay_headers.copy()
    facilitator_headers["x-rtwa-delete-marker-facilitator"] = "true"
    logging.info("Creating facilitator object for deletion marker workaround: PUT %s", overlay_url)
    facilitator_response = await signed_client.put(overlay_url, headers=facilitator_headers, content=b"")
    logging.info("Facilitator creation response status: %s", facilitator_response.status_code)
    
    logging.info("Deleting facilitator object: DELETE %s", overlay_url)
    response = await signed_client.request("DELETE", overlay_url, headers=overlay_headers, content=body)
    logging.info("Delete response (workaround) status: %s, headers: %s", response.status_code, dict(response.headers))
    return response

async def handle_precondition_failure(
    method: str, full_path: str, original_headers: dict, body: bytes, response: httpx.Response
) -> httpx.Response:
    """
    If the origin request returns a 412 Precondition Failed, use ListObjectVersions to determine whether
    a version of the object existed before START_TIME. If found, append the versionId as a subresource 
    to the object path and retry the request.
    """
    if response.status_code != 412:
        return response

    # Parse bucket and key from full_path (assumes format "bucket/key")
    parts = full_path.strip("/").split("/", 1)
    if len(parts) == 2:
        bucket, key = parts
    else:
        bucket, key = parts[0], ""

    logging.info("Received 412. Listing object versions for bucket: %s, key: %s", bucket, key)
    s3_client = boto3.client("s3")
    versions_response = s3_client.list_object_versions(Bucket=bucket, Prefix=key)
    
    candidate = None
    candidate_time = None
    if "Versions" in versions_response:
        for ver in versions_response["Versions"]:
            last_modified = ver["LastModified"]
            if last_modified < START_TIME:
                if candidate is None or last_modified > candidate_time:
                    candidate = ver
                    candidate_time = last_modified

    if candidate is None:
        logging.info("No matching version found for key %s before START_TIME. Returning 404.", key)
        return httpx.Response(status_code=404, content=b"")

    version_id = candidate["VersionId"]
    logging.info("Found version %s. Retrying origin request with version subresource.", version_id)
    origin_url = f"{ORIGIN_S3_URL}/{quote(full_path)}?versionId={version_id}"
    new_response = await client.request(
         method, origin_url, headers=original_headers, auth=origin_aws_auth, content=body
    )
    return new_response

async def handle_get_head_fallback(
    method: str,
    full_path: str,
    original_headers: dict,
    body: bytes,
    response: httpx.Response
) -> httpx.Response:
    """
    If the GET or HEAD request to the overlay bucket returns a 404 (and no delete marker),
    fall back to origin S3. Additionally, if the origin response is 412,
    try to recover by retrying with a specific version ID.
    """
    if method in {"GET", "HEAD"} and response.status_code == 404:
        if response.headers.get("x-amz-delete-marker", "false").lower() != "true":
            origin_url = f"{ORIGIN_S3_URL}/{quote(full_path)}"
            origin_headers = original_headers.copy()

            proxy_start_str = START_TIME.strftime("%a, %d %b %Y %H:%M:%S GMT")
            existing_ius = origin_headers.get("if-unmodified-since")
            if not existing_ius:
                origin_headers["If-Unmodified-Since"] = proxy_start_str
            else:
                try:
                    parsed_ius = datetime.strptime(existing_ius, "%a, %d %b %Y %H:%M:%S GMT").replace(tzinfo=timezone.utc)
                    if parsed_ius > START_TIME:
                        origin_headers["If-Unmodified-Since"] = proxy_start_str
                except ValueError:
                    origin_headers["If-Unmodified-Since"] = proxy_start_str

            logging.info("Fallback to origin S3: %s %s", method, origin_url)
            # It's not clear if we need to re-sign this request or not.
            # In my testing, the aws s3 client library did not include
            # If-Unmodified-Since in the signed headers. 
            new_response = await client.request(method, origin_url, headers=origin_headers, content=body)
            logging.info("Origin response status: %s", new_response.status_code)
            if new_response.status_code == 412:
                new_response = await handle_precondition_failure(method, full_path, original_headers, body, new_response)
            return new_response
    return response

def merged_list_to_xml(merged_list, bucket, prefix):
    """
    Convert the merged list of versions into an XML document that emulates S3's ListObjectVersionsResult.
    (This is a simplified schema; adjust fields as needed.)
    """
    root = ET.Element("ListVersionsResult")

    name = ET.SubElement(root, "Name")
    name.text = bucket

    pre = ET.SubElement(root, "Prefix")
    pre.text = prefix

    # Precompute the maximum LastModified for each key.
    latest_by_key = {}
    for item in merged_list:
        key = item.get("Key", "")
        lm = item.get("LastModified")
        if lm:
            if key not in latest_by_key or lm > latest_by_key[key]:
                latest_by_key[key] = lm

    # For each entry in the merged list, add a <Version> or <DeleteMarker> element depending on ItemType.
    for item in merged_list:
        item_type = item.get("ItemType", "Version")
        elem = ET.SubElement(root, item_type)
        key_elem = ET.SubElement(elem, "Key")
        key_elem.text = item.get("Key", "")

        version_id_elem = ET.SubElement(elem, "VersionId")
        version_id_elem.text = item.get("VersionId", "")

        is_latest_elem = ET.SubElement(elem, "IsLatest")
        # Mark as latest only if this item's LastModified equals the maximum for that key.
        last_modified = item.get("LastModified")
        key_val = item.get("Key", "")
        if last_modified and latest_by_key.get(key_val) == last_modified:
            is_latest_elem.text = "true"
        else:
            is_latest_elem.text = "false"

        last_modified_elem = ET.SubElement(elem, "LastModified")
        last_modified = item.get("LastModified")
        last_modified_elem.text = last_modified.isoformat() if last_modified else ""

    return ET.tostring(root, encoding="utf-8", method="xml")

@app.get("/{bucket}")
async def list_objects_handler(bucket: str, request: Request, prefix: str = ""):
    """
    Dispatch S3 list requests based on query parameters.

    - If query parameter list-type=2 is present, process as ListObjectsV2.
    - If query parameter versions is present, process as ListObjectVersions.
    - Otherwise, handle as a regular GET.
    """
    params = request.query_params
    list_type = params.get("list-type")
    versions = params.get("versions")
    delimiter = params.get("delimiter")


    if list_type == "2":
        # ----- ListObjectsV2 logic -----
        max_keys = int(params.get("max-keys", "1000"))
        continuation_token = params.get("continuation-token")

        # STEP 1: Get version information from origin.
        s3_client_origin = boto3.client("s3")  # Initialize the S3 client
        logging.info("Querying origin bucket with Prefix: %s and Delimiter: %s", prefix or "", delimiter)
        origin_params = {"Bucket": bucket, "Prefix": prefix or ""}
        if delimiter:
            origin_params["Delimiter"] = delimiter
        logging.info("Origin query parameters: %s", origin_params)
        origin_resp = s3_client_origin.list_object_versions(**origin_params)
        objects = {}  # key: object key, value: latest version dict.
        if "Versions" in origin_resp:
            for ver in origin_resp["Versions"]:
                if ver["LastModified"] < START_TIME:
                    ver["ItemType"] = "Version"
                    key = ver["Key"]
                    if key not in objects or ver["LastModified"] > objects[key]["LastModified"]:
                        objects[key] = ver

        # Process delete markers from origin
        if "DeleteMarkers" in origin_resp:
            for dm in origin_resp["DeleteMarkers"]:
                logging.info("Processing delete marker from origin: %s", dm["Key"])
                if dm["LastModified"] < START_TIME:
                    dm["ItemType"] = "DeleteMarker"
                    key = dm["Key"]
                    if key not in objects or dm["LastModified"] > objects[key]["LastModified"]:
                        objects[key] = dm

        logging.info("Origin bucket items: %s", [obj["Key"] for obj in origin_resp.get("Versions", [])])

        # STEP 2: Get version information from overlay.
        s3_client_overlay = boto3.client(
            "s3",
            aws_access_key_id=overlay_credentials.access_key,
            aws_secret_access_key=overlay_credentials.secret_key,
            aws_session_token=overlay_credentials.token,
            endpoint_url=OVERLAY_S3_URL  # Use the overlay S3 endpoint
        )
        overlay_prefix = f"{bucket}/{prefix}" if prefix else f"{bucket}/"
        logging.info("Querying overlay bucket with Prefix: %s and Delimiter: %s", overlay_prefix, delimiter)
        overlay_params = {"Bucket": OVERLAY_BUCKET, "Prefix": overlay_prefix}
        if delimiter:
            overlay_params["Delimiter"] = delimiter
        logging.info("Overlay query parameters: %s", overlay_params)
        overlay_resp = s3_client_overlay.list_object_versions(**overlay_params)
        logging.info("Overlay response: %s", overlay_resp)
        if "Versions" in overlay_resp:
            for ver in overlay_resp["Versions"]:
                ver["ItemType"] = "Version"
                key_val = ver["Key"]
                if key_val.startswith(f"{bucket}/"):
                    key_val = key_val[len(bucket)+1:]
                if key_val not in objects or ver["LastModified"] > objects[key_val]["LastModified"]:
                    objects[key_val] = ver

        # Process delete markers from overlay
        if "DeleteMarkers" in overlay_resp:
            for dm in overlay_resp["DeleteMarkers"]:
                logging.info("Processing delete marker from overlay: %s", dm["Key"])
                dm["ItemType"] = "DeleteMarker"
                key_val = dm["Key"]
                if key_val.startswith(f"{bucket}/"):
                    key_val = key_val[len(bucket)+1:]
                if key_val not in objects or dm["LastModified"] > objects[key_val]["LastModified"]:
                    objects[key_val] = dm

        logging.info("Overlay bucket items: %s", [obj["Key"] for obj in overlay_resp.get("Versions", [])])

        # Collect common prefixes from origin
        origin_common_prefixes = set()
        if "CommonPrefixes" in origin_resp:
            origin_common_prefixes.update(cp["Prefix"] for cp in origin_resp["CommonPrefixes"])

        # Collect common prefixes from overlay
        overlay_common_prefixes = set()
        if "CommonPrefixes" in overlay_resp:
            overlay_common_prefixes.update(cp["Prefix"] for cp in overlay_resp["CommonPrefixes"])

        # Merge common prefixes
        all_common_prefixes = origin_common_prefixes.union(overlay_common_prefixes)

        # STEP 3: Build final list, omitting keys whose latest version is a delete marker.
        final_objects = []
        for key, ver in objects.items():
            if ver["ItemType"] == "DeleteMarker":
                logging.info("Excluding object with delete marker: %s", key)
                continue
            final_objects.append(ver)

        # If a continuation token is provided, filter out objects with keys less than or equal to it.
        if continuation_token:
            final_objects = [obj for obj in final_objects if obj["Key"] > continuation_token]
        # Sort objects by Key lexicographically to produce stable pagination.
        final_objects.sort(key=lambda x: x["Key"])

        # STEP 4: Paginate.
        is_truncated = len(final_objects) > max_keys
        paginated = final_objects[:max_keys]
        next_token = paginated[-1]["Key"] if is_truncated else ""
        # Avoid returning the same token as received.
        if continuation_token and next_token == continuation_token:
            next_token = ""

        # STEP 5: Build XML per ListObjectsV2.
        root = ET.Element("ListBucketResult")
        name_elem = ET.SubElement(root, "Name")
        name_elem.text = bucket
        prefix_elem = ET.SubElement(root, "Prefix")
        prefix_elem.text = prefix
        keycount_elem = ET.SubElement(root, "KeyCount")
        keycount_elem.text = str(len(paginated))
        maxkeys_elem = ET.SubElement(root, "MaxKeys")
        maxkeys_elem.text = str(max_keys)
        trunc_elem = ET.SubElement(root, "IsTruncated")
        trunc_elem.text = "true" if is_truncated else "false"
        if is_truncated:
            token_elem = ET.SubElement(root, "NextContinuationToken")
            token_elem.text = next_token

        # Handle delimiter (common prefixes) and adjust displayed keys.
        delimiter = params.get("delimiter")
        common_prefixes = set()
        contents = []
        for obj in paginated:
            obj_key = obj["Key"]
            # Remove the prefix (if provided) for display.
            display_key = obj_key[len(prefix):] if prefix and obj_key.startswith(prefix) else obj_key
            if delimiter:
                pos = display_key.find(delimiter)
                # If the delimiter is present...
                if pos != -1:
                    # Compute the common prefix (everything up to and including the delimiter).
                    cp = display_key[:pos+1]
                    common_prefixes.add((prefix if prefix else "") + cp)
                    # If there are additional characters after the delimiter, group the key as a common prefix.
                    # Otherwise, include the object as content.
                    if len(display_key) > pos+1:
                        continue
            contents.append(obj)

        # Add CommonPrefixes to the XML response
        for cp in sorted(all_common_prefixes):
            cp_elem = ET.SubElement(root, "CommonPrefixes")
            prefix_elem_cp = ET.SubElement(cp_elem, "Prefix")
            prefix_elem_cp.text = cp

        # Add Contents to the XML response
        for obj in paginated:
            obj_key = obj["Key"]
            # Exclude objects grouped under CommonPrefixes
            if any(obj_key.startswith(cp) for cp in all_common_prefixes):
                continue
            cont_elem = ET.SubElement(root, "Contents")
            key_elem = ET.SubElement(cont_elem, "Key")
            key_elem.text = obj_key[len(prefix):] if prefix and obj_key.startswith(prefix) else obj_key
            lastmod_elem = ET.SubElement(cont_elem, "LastModified")
            lm = obj["LastModified"]
            lastmod_elem.text = lm.isoformat() if isinstance(lm, datetime) else str(lm)
            size_elem = ET.SubElement(cont_elem, "Size")
            size_elem.text = str(obj.get("Size", 0))
            storage_elem = ET.SubElement(cont_elem, "StorageClass")
            storage_elem.text = obj.get("StorageClass", "STANDARD")

        xml_response = ET.tostring(root, encoding="utf-8", method="xml")
        return Response(content=xml_response, media_type="application/xml")

    elif versions is not None:
        # ----- ListObjectVersions logic (existing) -----
        s3_client_origin = boto3.client("s3")
        origin_params = {"Bucket": bucket, "Prefix": prefix or ""}
        origin_resp = s3_client_origin.list_object_versions(**origin_params)
        origin_items = []
        if "Versions" in origin_resp:
            for ver in origin_resp["Versions"]:
                if ver["LastModified"] < START_TIME:
                    ver["ItemType"] = "Version"
                    origin_items.append(ver)
        if "DeleteMarkers" in origin_resp:
            for dm in origin_resp["DeleteMarkers"]:
                if dm["LastModified"] < START_TIME:
                    dm["ItemType"] = "DeleteMarker"
                    origin_items.append(dm)

        overlay_bucket = OVERLAY_BUCKET
        overlay_prefix = f"{bucket}{prefix}" if prefix else bucket
        s3_client_overlay = boto3.client(
            "s3",
            aws_access_key_id=overlay_credentials.access_key,
            aws_secret_access_key=overlay_credentials.secret_key,
            aws_session_token=overlay_credentials.token,
            endpoint_url=OVERLAY_S3_URL
        )
        overlay_params = {"Bucket": OVERLAY_BUCKET, "Prefix": overlay_prefix}
        overlay_resp = s3_client_overlay.list_object_versions(**overlay_params)
        merged_list = origin_items[:]
        if "Versions" in overlay_resp:
            for over in overlay_resp["Versions"]:
                over["ItemType"] = "Version"
                merged_list.append(over)
        if "DeleteMarkers" in overlay_resp:
            for dm in overlay_resp["DeleteMarkers"]:
                dm["ItemType"] = "DeleteMarker"
                merged_list.append(dm)

        merged_list.sort(key=lambda x: x["LastModified"], reverse=True)
        xml_response = merged_list_to_xml(merged_list, bucket, prefix)
        return Response(content=xml_response, media_type="application/xml")

    else:
        # Fallback: Regular GET on bucket.
        return {"message": f"Regular GET for bucket: {bucket} with prefix: {prefix}"}

@app.api_route("/{full_path:path}", methods=["GET", "PUT", "DELETE", "HEAD"])
async def proxy(full_path: str, request: Request):
    method = request.method
    original_headers = dict(request.headers)
    logging.info("Received %s request for %s", method, full_path)
    
    # Use filtered headers for overlay S3 request.
    overlay_headers = {
        k: v for k, v in original_headers.items() 
        if not k.lower().startswith("authorization") and not k.lower().startswith("x-amz")
    }
    body = await request.body()
    overlay_path = rewrite_overlay_path(full_path)
    overlay_url = f"{OVERLAY_S3_URL}/{quote(overlay_path)}"
    
    if method == "DELETE":
        response = await handle_delete_workaround(overlay_url, overlay_headers, body)
    else:
        logging.info("Sending overlay request: %s %s", method, overlay_url)
        response = await signed_client.request(method, overlay_url, headers=overlay_headers, content=body)
        logging.info("Overlay response status: %s, headers: %s", response.status_code, dict(response.headers))
    
    # For GET/HEAD, if the overlay response includes the facilitator header, treat it as delete marker.
    if method in {"GET", "HEAD"} and response.headers.get("x-rtwa-delete-marker-facilitator", "false").lower() == "true":
        logging.info("Overlay response includes facilitator header; treating as delete marker (404)")
        response = httpx.Response(status_code=404, content=b"", headers=response.headers)
    
    # Fallback to origin S3 if applicable, including 412 precondition handling.
    response = await handle_get_head_fallback(method, full_path, original_headers, body, response)
    
    return Response(
        content=response.content,
        status_code=response.status_code,
        headers={k: v for k, v in response.headers.items() if k.lower() not in {"content-encoding", "transfer-encoding"}}
    )
