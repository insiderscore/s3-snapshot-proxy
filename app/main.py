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
async def list_object_versions(bucket: str, prefix: str = "", versions: str = None):
    """
    Emulate S3 ListObjectVersions.

    If the query parameter 'versions' is present, this endpoint returns a merged list:
      - Origin: Lists object versions on the origin bucket filtered to those before START_TIME.
      - Overlay: Lists object versions in the overlay bucket (stored under "<bucket>/<key>") that override the origin.

    Otherwise, this route may be used for regular GET operations.
    """
    if versions is not None:
        # List object versions from origin bucket
        s3_client_origin = boto3.client("s3")
        origin_resp = s3_client_origin.list_object_versions(Bucket=bucket, Prefix=prefix)

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

        # List object versions from the overlay bucket.
        overlay_bucket = OVERLAY_BUCKET
        overlay_prefix = f"{bucket}{prefix}" if prefix else bucket
        s3_client_overlay = boto3.client(
            "s3",
            aws_access_key_id=overlay_credentials.access_key,
            aws_secret_access_key=overlay_credentials.secret_key,
            aws_session_token=overlay_credentials.token,
            endpoint_url=OVERLAY_S3_URL  # Use the overlay S3 endpoint
        )
        overlay_resp = s3_client_overlay.list_object_versions(Bucket=overlay_bucket, Prefix=overlay_prefix)

        merged_list = origin_items[:]  # start with all origin items
        if "Versions" in overlay_resp:
            for over in overlay_resp["Versions"]:
                over["ItemType"] = "Version"
                merged_list.append(over)
        if "DeleteMarkers" in overlay_resp:
            for dm in overlay_resp["DeleteMarkers"]:
                dm["ItemType"] = "DeleteMarker"
                merged_list.append(dm)

        # Optionally sort by LastModified descending.
        merged_list.sort(key=lambda x: x["LastModified"], reverse=True)

        xml_response = merged_list_to_xml(merged_list, bucket, prefix)
        return Response(content=xml_response, media_type="application/xml")

    # Fallback: if no 'versions' query parameter is passed, handle as a regular GET on the bucket.
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
