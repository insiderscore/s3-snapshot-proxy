import httpx
from fastapi import FastAPI, Request, Response
import os
import argparse
from urllib.parse import quote
from httpx_auth import AWS4Auth
import boto3
from datetime import datetime, timezone
import logging
import sys
import xml.etree.ElementTree as ET
from collections import defaultdict
from typing import Optional
import botocore.exceptions

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    stream=sys.stdout,
    format="%(asctime)s %(levelname)s %(name)s %(message)s"
)

# Determine START_TIME from environment variable instead of command line
start_time_str = os.environ.get("START_TIME")
if start_time_str:
    try:
        # Parse the provided start time
        START_TIME = datetime.fromisoformat(start_time_str.replace('Z', '+00:00'))
        
        # Check if time is in the future
        if START_TIME > datetime.now(timezone.utc):
            logging.error("Error: Cannot set START_TIME in the future.")
            logging.error("DMC-12 unavailable. Attempt with Cybertruck failed. (snapshot start time must be in the past.)")
            sys.exit(1)
            
        logging.info(f"Using custom START_TIME: {START_TIME.isoformat()}")
    except ValueError as e:
        logging.error(f"Invalid START_TIME format. Please use ISO-8601 format (YYYY-MM-DDTHH:MM:SSZ).")
        logging.error(f"Error: {e}")
        sys.exit(1)
else:
    # Use current time if no START_TIME provided
    START_TIME = datetime.now(timezone.utc)
    logging.info(f"Using current time as START_TIME: {START_TIME.isoformat()}")

app = FastAPI()

# Configurable base URLs
OVERLAY_S3_URL = os.environ.get("OVERLAY_S3_URL", "http://overlay-s3.local")
ORIGIN_S3_URL = os.environ.get("ORIGIN_S3_URL", "https://s3.amazonaws.com")
OVERLAY_BUCKET = os.environ.get("OVERLAY_BUCKET", "overlay")

# Add health check endpoints for different purposes
@app.get("/health")
async def health_check():
    """
    Simple health check endpoint that returns the service status
    and proxy start time.
    """
    return {
        "status": "healthy",
        "startTime": START_TIME.isoformat(),
        "overlayS3": OVERLAY_S3_URL,
        "originS3": ORIGIN_S3_URL,
        "overlayBucket": OVERLAY_BUCKET,
    }

@app.get("/livez")
async def liveness_probe():
    """
    Kubernetes liveness probe endpoint.
    Simple check that the application is running.
    """
    return {"status": "alive"}

@app.get("/readyz")
async def readiness_probe():
    """
    Kubernetes readiness probe endpoint.
    Verifies the application can connect to its dependencies.
    """
    status = {"ready": True, "components": {}}
    
    # Check overlay S3 connection
    try:
        s3_client_overlay = boto3.client(
            "s3",
            aws_access_key_id=overlay_credentials.access_key,
            aws_secret_access_key=overlay_credentials.secret_key,
            aws_session_token=overlay_credentials.token if hasattr(overlay_credentials, 'token') else None,
            endpoint_url=OVERLAY_S3_URL
        )
        # Check if overlay bucket exists
        s3_client_overlay.head_bucket(Bucket=OVERLAY_BUCKET)
        status["components"]["overlay_s3"] = "connected"
    except Exception as e:
        logging.warning(f"Overlay S3 connection failed: {str(e)}")
        status["ready"] = False
        status["components"]["overlay_s3"] = f"connection_failed: {str(e)}"
    
    # We don't strictly need to check origin S3 if we're just reading from overlay
    # But include a basic check that credentials are valid
    try:
        s3_client_origin = boto3.client(
            "s3",
            aws_access_key_id=origin_credentials.access_key,
            aws_secret_access_key=origin_credentials.secret_key,
            aws_session_token=origin_credentials.token if hasattr(origin_credentials, 'token') else None,
            endpoint_url=ORIGIN_S3_URL
        )
        # Just check if we can access the service
        s3_client_origin.list_buckets()
        status["components"]["origin_s3"] = "connected"
    except Exception as e:
        # Origin failure is non-fatal if we're operating in overlay-only mode
        logging.warning(f"Origin S3 connection check failed: {str(e)}")
        status["components"]["origin_s3"] = f"connection_failed: {str(e)}"
    
    if status["ready"]:
        return status
    else:
        return Response(status_code=503, content=str(status))

# Also provide a root endpoint for basic health checks
@app.get("/")
async def root():
    """
    Basic health check at the root path
    """
    return {"status": "healthy"}

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
    
    1. Check if this is a conditional DELETE - if so, return 501 Not Implemented
    2. Otherwise:
       a. Create a zero-length facilitator object with the header x-rtwa-delete-marker-facilitator
       b. Then delete that object so only a delete marker remains
    """
    # First check if this is a conditional DELETE request
    conditional_headers = ["if-match", "if-none-match", "if-modified-since", "if-unmodified-since"]
    
    for header in conditional_headers:
        if header in {k.lower() for k in overlay_headers.keys()}:
            logging.info("Conditional DELETE detected with header: %s. Returning 501 Not Implemented.", header)
            # Return 501 Not Implemented for conditional DELETE operations
            return httpx.Response(
                status_code=501,
                content=b"<Error><Code>NotImplemented</Code><Message>Conditional DELETE operations are not implemented</Message></Error>",
                headers={"Content-Type": "application/xml"}
            )
    
    # Standard DELETE operation - proceed with facilitator object pattern
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
    # Use the proper client with correct credentials and endpoint
    s3_client = get_origin_s3_client()
    versions_response = s3_client.list_object_versions(Bucket=bucket, Prefix=key)
    
    candidate = None
    candidate_time = None
    if "Versions" in versions_response:
        for ver in versions_response["Versions"]:
            # Use our factored function instead of repeating the condition
            if filter_version_by_start_time(ver, START_TIME):
                if candidate is None or ver["LastModified"] > candidate_time:
                    candidate = ver
                    candidate_time = ver["LastModified"]

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

# Factor out the S3 client creation
def get_origin_s3_client():
    """Create and return an S3 client for origin access"""
    return boto3.client(
        "s3",
        aws_access_key_id=origin_credentials.access_key,
        aws_secret_access_key=origin_credentials.secret_key,
        aws_session_token=origin_credentials.token if hasattr(origin_credentials, 'token') else None,
        endpoint_url=ORIGIN_S3_URL
    )

def get_overlay_s3_client():
    """Create and return an S3 client for overlay access"""
    return boto3.client(
        "s3",
        aws_access_key_id=overlay_credentials.access_key,
        aws_secret_access_key=overlay_credentials.secret_key,
        aws_session_token=overlay_credentials.token if hasattr(overlay_credentials, 'token') else None,
        endpoint_url=OVERLAY_S3_URL
    )

# Extract version filtering into a utility function
def filter_version_by_start_time(version, start_time):
    """Return True if this version is relevant (created before START_TIME)"""
    return version["LastModified"] < start_time

# Handle the versions logic
def process_list_versions(bucket, prefix, delimiter, key_marker, version_id_marker, max_keys):
    """
    Process ListObjectVersions request by merging results from origin and overlay
    with proper pagination and filtering
    """
    # STEP 1: Get versions from origin that were created before START_TIME
    s3_client_origin = get_origin_s3_client()
    origin_versions = []
    origin_delete_markers = []
    origin_common_prefixes = set()
    
    # Paginate through all object versions from origin
    is_truncated = True
    origin_key_marker = key_marker
    origin_version_id_marker = version_id_marker
    
    while is_truncated:
        origin_params = {
            "Bucket": bucket, 
            "Prefix": prefix or "",
            "MaxKeys": 1000  # Use maximum allowed for efficiency
        }
        
        if delimiter:
            origin_params["Delimiter"] = delimiter
            
        if origin_key_marker:
            origin_params["KeyMarker"] = origin_key_marker
            if origin_version_id_marker:
                origin_params["VersionIdMarker"] = origin_version_id_marker
        
        logging.info(f"Origin ListObjectVersions params: {origin_params}")
        origin_resp = s3_client_origin.list_object_versions(**origin_params)
        
        # Process versions that existed before START_TIME
        if "Versions" in origin_resp:
            for ver in origin_resp["Versions"]:
                if filter_version_by_start_time(ver, START_TIME):
                    # Annotate with source and add ItemType for XML generation
                    ver["Source"] = "origin"
                    ver["ItemType"] = "Version"
                    origin_versions.append(ver)
        
        # Process delete markers that existed before START_TIME
        if "DeleteMarkers" in origin_resp:
            for dm in origin_resp["DeleteMarkers"]:
                if filter_version_by_start_time(dm, START_TIME):
                    dm["Source"] = "origin"
                    dm["ItemType"] = "DeleteMarker"
                    origin_delete_markers.append(dm)
                    
        # Process common prefixes if delimiter is specified
        if "CommonPrefixes" in origin_resp:
            for cp in origin_resp["CommonPrefixes"]:
                origin_common_prefixes.add(cp["Prefix"])
        
        # Update markers for next iteration
        is_truncated = origin_resp.get('IsTruncated', False)
        if is_truncated:
            origin_key_marker = origin_resp.get('NextKeyMarker')
            origin_version_id_marker = origin_resp.get('NextVersionIdMarker')
        else:
            break
    
    # STEP 2: Get versions from overlay bucket
    s3_client_overlay = get_overlay_s3_client()
    overlay_versions = []
    overlay_delete_markers = []
    overlay_common_prefixes = set()
    
    # Calculate overlay prefix
    overlay_prefix = f"{bucket}/"
    if prefix:
        overlay_prefix = f"{bucket}/{prefix}"
    
    # Paginate through overlay versions
    is_truncated = True
    overlay_key_marker = None
    overlay_version_id_marker = None
    
    if key_marker:
        # Need to transform the key marker for overlay
        overlay_key_marker = f"{bucket}/{key_marker}"
    
    while is_truncated:
        overlay_params = {
            "Bucket": OVERLAY_BUCKET,
            "Prefix": overlay_prefix,
            "MaxKeys": 1000
        }
        
        if delimiter:
            # Need to adjust delimiter handling for the overlay bucket
            # because keys are prefixed with the bucket name
            overlay_params["Delimiter"] = delimiter
        
        if overlay_key_marker:
            overlay_params["KeyMarker"] = overlay_key_marker
            if overlay_version_id_marker:
                overlay_params["VersionIdMarker"] = overlay_version_id_marker
        
        logging.info(f"Overlay ListObjectVersions params: {overlay_params}")
        overlay_resp = s3_client_overlay.list_object_versions(**overlay_params)
        
        # Process overlay versions
        if "Versions" in overlay_resp:
            for ver in overlay_resp["Versions"]:
                # Strip bucket prefix from key for comparison
                original_key = ver["Key"]
                if original_key.startswith(f"{bucket}/"):
                    ver["Key"] = original_key[len(f"{bucket}/"):]
                    ver["Source"] = "overlay"
                    ver["ItemType"] = "Version"
                    ver["OriginalKey"] = original_key  # Keep original for reference
                    overlay_versions.append(ver)
        
        # Process overlay delete markers
        if "DeleteMarkers" in overlay_resp:
            for dm in overlay_resp["DeleteMarkers"]:
                original_key = dm["Key"]
                if original_key.startswith(f"{bucket}/"):
                    dm["Key"] = original_key[len(f"{bucket}/"):]
                    dm["Source"] = "overlay"
                    dm["ItemType"] = "DeleteMarker"
                    dm["OriginalKey"] = original_key
                    overlay_delete_markers.append(dm)
        
        # Process overlay common prefixes
        if "CommonPrefixes" in overlay_resp:
            for cp in overlay_resp["CommonPrefixes"]:
                prefix_val = cp["Prefix"]
                if prefix_val.startswith(f"{bucket}/"):
                    # Strip the bucket prefix for consistency
                    adjusted_prefix = prefix_val[len(f"{bucket}/"):]
                    overlay_common_prefixes.add(adjusted_prefix)
        
        # Update markers for next iteration
        is_truncated = overlay_resp.get('IsTruncated', False)
        if is_truncated:
            overlay_key_marker = overlay_resp.get('NextKeyMarker')
            overlay_version_id_marker = overlay_resp.get('NextVersionIdMarker')
        else:
            break
    
    # STEP 3: Merge results from origin and overlay
    # Create a dictionary to track the latest versions for each key
    all_versions = origin_versions + overlay_versions
    all_delete_markers = origin_delete_markers + overlay_delete_markers
    
    # Merge common prefixes
    all_common_prefixes = origin_common_prefixes.union(overlay_common_prefixes)
    
    # Build a comprehensive version history for each key
    key_versions = defaultdict(list)
    
    # Add all versions
    for ver in all_versions:
        key = ver["Key"]
        key_versions[key].append(ver)
    
    # Add all delete markers
    for dm in all_delete_markers:
        key = dm["Key"]
        key_versions[key].append(dm)
    
    # For each key, sort versions by LastModified (newest first)
    merged_list = []
    for key, versions in key_versions.items():
        versions.sort(key=lambda x: x["LastModified"], reverse=True)
        
        # Mark the newest version as IsLatest
        if versions:
            versions[0]["IsLatest"] = True
            for v in versions[1:]:
                v["IsLatest"] = False
            
        merged_list.extend(versions)
    
    # Sort the entire merged list by Key and then by LastModified (newest first)
    merged_list.sort(key=lambda x: (x["Key"], -x["LastModified"].timestamp() if isinstance(x["LastModified"], datetime) else 0))
    
    # STEP 4: Handle pagination
    if key_marker:
        # Find the position after key_marker to start returning results
        start_pos = 0
        for i, item in enumerate(merged_list):
            if item["Key"] > key_marker or (item["Key"] == key_marker and item.get("VersionId", "") > version_id_marker):
                start_pos = i
                break
        merged_list = merged_list[start_pos:]
    
    # Limit results to max_keys
    is_truncated = len(merged_list) > max_keys
    paginated_list = merged_list[:max_keys]
    
    # Get next markers if truncated
    next_key_marker = ""
    next_version_id_marker = ""
    if is_truncated and paginated_list:
        last_item = paginated_list[-1]
        next_key_marker = last_item["Key"]
        next_version_id_marker = last_item.get("VersionId", "")
    
    # STEP 5: Generate XML response
    root = ET.Element("ListVersionsResult")
    
    # Add required elements
    ET.SubElement(root, "Name").text = bucket
    ET.SubElement(root, "Prefix").text = prefix or ""
    if key_marker:
        ET.SubElement(root, "KeyMarker").text = key_marker
    if version_id_marker:
        ET.SubElement(root, "VersionIdMarker").text = version_id_marker
    if is_truncated:
        ET.SubElement(root, "NextKeyMarker").text = next_key_marker
        ET.SubElement(root, "NextVersionIdMarker").text = next_version_id_marker
    
    ET.SubElement(root, "MaxKeys").text = str(max_keys)
    ET.SubElement(root, "IsTruncated").text = "true" if is_truncated else "false"
    
    if delimiter:
        ET.SubElement(root, "Delimiter").text = delimiter
    
    # Add CommonPrefixes
    for prefix_val in sorted(all_common_prefixes):
        cp_elem = ET.SubElement(root, "CommonPrefixes")
        ET.SubElement(cp_elem, "Prefix").text = prefix_val
    
    # Add Versions and DeleteMarkers
    for item in paginated_list:
        if item["ItemType"] == "Version":
            ver_elem = ET.SubElement(root, "Version")
            ET.SubElement(ver_elem, "Key").text = item["Key"]
            ET.SubElement(ver_elem, "VersionId").text = item.get("VersionId", "")
            ET.SubElement(ver_elem, "IsLatest").text = "true" if item.get("IsLatest", False) else "false"
            ET.SubElement(ver_elem, "LastModified").text = item["LastModified"].isoformat() if isinstance(item["LastModified"], datetime) else str(item["LastModified"])
            ET.SubElement(ver_elem, "Size").text = str(item.get("Size", 0))
            ET.SubElement(ver_elem, "ETag").text = item.get("ETag", "")
            ET.SubElement(ver_elem, "StorageClass").text = item.get("StorageClass", "STANDARD")
        else:  # DeleteMarker
            dm_elem = ET.SubElement(root, "DeleteMarker")
            ET.SubElement(dm_elem, "Key").text = item["Key"]
            ET.SubElement(dm_elem, "VersionId").text = item.get("VersionId", "")
            ET.SubElement(dm_elem, "IsLatest").text = "true" if item.get("IsLatest", False) else "false"
            ET.SubElement(dm_elem, "LastModified").text = item["LastModified"].isoformat() if isinstance(item["LastModified"], datetime) else str(item["LastModified"])
    
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

        # STEP 1: Get ALL versions from origin to find the latest version before START_TIME
        s3_client_origin = boto3.client(
            "s3",
            aws_access_key_id=origin_credentials.access_key,
            aws_secret_access_key=origin_credentials.secret_key,
            aws_session_token=origin_credentials.token,
            endpoint_url=ORIGIN_S3_URL
        )
        logging.info("Querying origin versions with Prefix: %s", prefix or "")
        origin_params = {"Bucket": bucket, "Prefix": prefix or ""}
        if delimiter:
            origin_params["Delimiter"] = delimiter
        # Note: We can't use continuation_token here since list_object_versions uses different pagination      
        
        # Paginate through all object versions
        is_truncated = True
        key_marker = None
        version_id_marker = None
        
        # Initialize dictionaries to track objects and common prefixes
        objects = {}  # key: object key, value: latest version before START_TIME
        deleted_keys = set()  # Track keys that were deleted as of START_TIME
        
        while is_truncated:
            logging.info("Fetching paginated result for ListObjectVersions params: %s", origin_params)
            if key_marker:
                origin_params["KeyMarker"] = key_marker
                origin_params["VersionIdMarker"] = version_id_marker
                
            origin_resp = s3_client_origin.list_object_versions(**origin_params)
            
            # Process versions from origin, keeping only the latest version before START_TIME for each key
            if "Versions" in origin_resp:
                for ver in origin_resp["Versions"]:
                    key = ver["Key"]
                    # Skip keys that were known to be deleted
                    if key in deleted_keys:
                        continue
                    # Only consider versions from before START_TIME
                    if ver["LastModified"] < START_TIME:
                        # If we haven't seen this key before, or this version is newer than what we have
                        if key not in objects or ver["LastModified"] > objects[key]["LastModified"]:
                            objects[key] = {
                                "Key": key,
                                "LastModified": ver["LastModified"],
                                "ETag": ver.get("ETag", ""),
                                "Size": ver.get("Size", 0),
                                "StorageClass": ver.get("StorageClass", "STANDARD")
                            }
            
            # Process delete markers from origin
            if "DeleteMarkers" in origin_resp:
                for dm in origin_resp["DeleteMarkers"]:
                    key = dm["Key"]
                    # Only consider delete markers from before START_TIME
                    if dm["LastModified"] < START_TIME:
                        # If this delete marker is newer than any existing version we have for the key
                        # or if we haven't seen this key before, mark it as deleted
                        if key not in objects or dm["LastModified"] > objects[key]["LastModified"]:
                            deleted_keys.add(key)
                            if key in objects:
                                del objects[key]
            
            # Update markers for next iteration
            is_truncated = origin_resp.get('IsTruncated', False)
            if is_truncated:
                key_marker = origin_resp.get('NextKeyMarker')
                version_id_marker = origin_resp.get('NextVersionIdMarker')
            else:
                break
        
        # Process common prefixes from origin
        origin_common_prefixes = []
        if "CommonPrefixes" in origin_resp:
            origin_common_prefixes = origin_resp["CommonPrefixes"]

        # STEP 2: Get version information from overlay to handle overlays and deletes
        s3_client_overlay = boto3.client(
            "s3",
            aws_access_key_id=overlay_credentials.access_key,
            aws_secret_access_key=overlay_credentials.secret_key,
            aws_session_token=overlay_credentials.token,
            endpoint_url=OVERLAY_S3_URL
        )
        
        overlay_prefix = f"{bucket}/"
        if prefix:
            overlay_prefix = f"{bucket}/{prefix}"
            
        logging.info("Querying overlay bucket with Prefix: %s", overlay_prefix)
        overlay_params = {"Bucket": OVERLAY_BUCKET, "Prefix": overlay_prefix}
        if delimiter:
            overlay_params["Delimiter"] = delimiter
        
        logging.info("Overlay query parameters: %s", overlay_params)
        overlay_resp = s3_client_overlay.list_object_versions(**overlay_params)
        
        # Process versions from overlay
        if "Versions" in overlay_resp:
            for ver in overlay_resp["Versions"]:
                key_val = ver["Key"]
                if key_val.startswith(f"{bucket}/"):
                    key_val = key_val[len(f"{bucket}/"):]
                
                # Add new objects from overlay or replace origin objects
                objects[key_val] = {
                    "Key": key_val,
                    "LastModified": ver["LastModified"],
                    "ETag": ver.get("ETag", ""),
                    "Size": ver.get("Size", 0),
                    "StorageClass": ver.get("StorageClass", "STANDARD")
                }
        
        # Process delete markers from overlay
        if "DeleteMarkers" in overlay_resp:
            for dm in overlay_resp["DeleteMarkers"]:
                key_val = dm["Key"]
                if key_val.startswith(f"{bucket}/"):
                    key_val = key_val[len(f"{bucket}/"):]
                
                # Remove objects that have delete markers
                if key_val in objects:
                    del objects[key_val]
        
        # STEP 3: Handle common prefixes from overlay
        overlay_common_prefixes = []
        if "CommonPrefixes" in overlay_resp:
            for cp in overlay_resp["CommonPrefixes"]:
                prefix_val = cp["Prefix"]
                if prefix_val.startswith(f"{bucket}/"):
                    prefix_val = prefix_val[len(f"{bucket}/"):]
                overlay_common_prefixes.append({"Prefix": prefix_val})
        
        # Merge common prefixes
        all_common_prefixes = origin_common_prefixes + overlay_common_prefixes
        
        # STEP 4: Build final list and paginate
        final_objects = list(objects.values())
        
        # Sort objects by Key lexicographically (required for ListObjectsV2)
        final_objects.sort(key=lambda x: x["Key"])
        
        # Handle pagination
        if continuation_token:
            # Find start position based on continuation token
            start_pos = 0
            for i, obj in enumerate(final_objects):
                if obj["Key"] > continuation_token:
                    start_pos = i
                    break
            final_objects = final_objects[start_pos:]
        
        # Limit results
        is_truncated = len(final_objects) > max_keys
        paginated = final_objects[:max_keys]
        next_token = paginated[-1]["Key"] if is_truncated and paginated else ""
        
        # STEP 5: Build XML response per ListObjectsV2 schema
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
        
        if continuation_token:
            token_elem = ET.SubElement(root, "ContinuationToken")
            token_elem.text = continuation_token
        
        if is_truncated:
            next_token_elem = ET.SubElement(root, "NextContinuationToken")
            next_token_elem.text = next_token
        
        # Add CommonPrefixes
        for cp in all_common_prefixes:
            cp_elem = ET.SubElement(root, "CommonPrefixes")
            prefix_elem_cp = ET.SubElement(cp_elem, "Prefix")
            prefix_elem_cp.text = cp["Prefix"]
        
        # Add Contents
        for obj in paginated:
            cont_elem = ET.SubElement(root, "Contents")
            key_elem = ET.SubElement(cont_elem, "Key")
            key_elem.text = obj["Key"]
            lastmod_elem = ET.SubElement(cont_elem, "LastModified")
            lastmod_elem.text = obj["LastModified"].isoformat() if isinstance(obj["LastModified"], datetime) else str(obj["LastModified"])
            etag_elem = ET.SubElement(cont_elem, "ETag")
            etag_elem.text = obj.get("ETag", "")
            size_elem = ET.SubElement(cont_elem, "Size")
            size_elem.text = str(obj.get("Size", 0))
            storage_elem = ET.SubElement(cont_elem, "StorageClass")
            storage_elem.text = obj.get("StorageClass", "STANDARD")
        
        xml_response = ET.tostring(root, encoding="utf-8", method="xml")
        return Response(content=xml_response, media_type="application/xml")

    elif versions is not None:
        # ----- ListObjectVersions logic (improved) -----
        max_keys = int(params.get("max-keys", "1000"))
        key_marker = params.get("key-marker")
        version_id_marker = params.get("version-id-marker")
        
        xml_response = process_list_versions(
            bucket=bucket,
            prefix=prefix,
            delimiter=delimiter,
            key_marker=key_marker,
            version_id_marker=version_id_marker,
            max_keys=max_keys
        )
        
        return Response(content=xml_response, media_type="application/xml")
    
    else:
        # Fallback: Regular GET on bucket.
        return {"message": f"Regular GET for bucket: {bucket} with prefix: {prefix}"}

async def handle_conditional_mutation(
    method: str, full_path: str, original_headers: dict, body: bytes, response: httpx.Response
) -> httpx.Response:
    """
    Handle conditional write (PUT) and delete (DELETE) requests that fail with 412 Precondition Failed.
    
    If the overlay returns 412 and the key doesn't exist in overlay:
    1. Check if the condition would be satisfied against the origin object as of START_TIME
    2. If so, retry the request against overlay with modified conditions
    """
    if response.status_code != 412 or method not in {"PUT", "DELETE"}:
        return response
        
    # Parse bucket and key from full_path
    parts = full_path.strip("/").split("/", 1)
    if len(parts) == 2:
        bucket, key = parts
    else:
        bucket, key = parts[0], ""
    
    logging.info("Conditional mutation failed with 412. Checking if condition can be satisfied via origin.")
    
    # Check if object exists in overlay first (to avoid race conditions)
    s3_client_overlay = get_overlay_s3_client()
    try:
        overlay_path = f"{bucket}/{key}"
        s3_client_overlay.head_object(Bucket=OVERLAY_BUCKET, Key=overlay_path)
        # If we get here, object exists in overlay - respect the 412 from overlay
        logging.info("Object exists in overlay. Respecting 412 Precondition Failed.")
        return response
    except Exception:
        # Object doesn't exist in overlay, check origin
        pass
    
    # Check original request conditions against origin
    s3_client_origin = get_origin_s3_client()
    try:
        # We need to find the version of the object that was current at START_TIME,
        # not just the current version
        versions_response = s3_client_origin.list_object_versions(Bucket=bucket, Prefix=key)
        
        # Find the most recent version that existed before START_TIME
        candidate = None
        candidate_time = None
        if "Versions" in versions_response:
            for ver in versions_response["Versions"]:
                # Filter versions by START_TIME using our utility function
                if filter_version_by_start_time(ver, START_TIME):
                    if candidate is None or ver["LastModified"] > candidate_time:
                        candidate = ver
                        candidate_time = ver["LastModified"]
        
        # If no suitable version found, the object didn't exist at START_TIME
        if candidate is None:
            logging.info("No version of object existed at START_TIME, original 412 response is correct")
            return response
            
        # Get the specific version's complete metadata for condition checking
        version_id = candidate["VersionId"]
        origin_obj = s3_client_origin.head_object(
            Bucket=bucket, 
            Key=key, 
            VersionId=version_id
        )
        
        # Now check conditions against this point-in-time correct version
        # Extract relevant conditions from original headers
        if_match = original_headers.get("if-match")
        if_none_match = original_headers.get("if-none-match")
        
        # Check if the origin object satisfies these conditions
        satisfied = True
        
        # Check ETag conditions
        if if_match:
            etags = [tag.strip(' "') for tag in if_match.split(",")]
            origin_etag = origin_obj.get("ETag", "").strip('"')
            if origin_etag not in etags and "*" not in etags:
                satisfied = False
                logging.info(f"If-Match condition not satisfied: {if_match} vs origin ETag {origin_etag}")
                
        if if_none_match and satisfied:
            etags = [tag.strip(' "') for tag in if_none_match.split(",")]
            origin_etag = origin_obj.get("ETag", "").strip('"')
            key_exists = True  # We know the key exists in origin if we got here
            
            # For If-None-Match, the condition is NOT satisfied if:
            # 1. Any specified ETag matches the current ETag, OR
            # 2. "*" is specified and the object exists
            if origin_etag in etags or ("*" in etags and key_exists):
                satisfied = False
                logging.info(f"If-None-Match condition not satisfied: {if_none_match} vs origin ETag {origin_etag}")
        
        # If origin would satisfy the conditions, retry against overlay with simplified condition
        if satisfied:
            logging.info("Origin object at START_TIME satisfies the original conditions. Retrying with If-None-Match: *")
            # Create new headers without the original conditions
            modified_headers = {k: v for k, v in original_headers.items() 
                               if k.lower() not in {"if-match", "if-none-match"}}
            # Add condition that will allow write if object doesn't exist in overlay
            modified_headers["If-None-Match"] = "*"
            
            overlay_path = rewrite_overlay_path(full_path)
            overlay_url = f"{OVERLAY_S3_URL}/{quote(overlay_path)}"
            
            new_response = await signed_client.request(
                method, overlay_url, headers=modified_headers, content=body
            )
            
            logging.info("Conditional retry status: %s", new_response.status_code)
            return new_response
        
    except Exception as e:
        # Object doesn't exist in origin or other error
        logging.info("Error checking origin object: %s", str(e))
    
    # Default: return the original 412 response
    return response

def check_object_at_start_time(bucket: str, key: str):
    """
    Check if an object existed in origin at START_TIME and return its metadata.
    Returns None if the object didn't exist at START_TIME.
    """
    s3_client_origin = get_origin_s3_client()
    try:
        # Initialize variables for pagination
        is_truncated = True
        key_marker = None
        version_id_marker = None
        
        # Find the most recent version that existed before START_TIME
        candidate = None
        candidate_time = None
        latest_delete_marker = None
        latest_delete_marker_time = None
        
        # Paginate through all versions
        while is_truncated:
            # Build parameters for this request
            params = {
                "Bucket": bucket,
                "Prefix": key
            }
            
            # Add pagination markers if we have them
            if key_marker:
                params["KeyMarker"] = key_marker
                if version_id_marker:
                    params["VersionIdMarker"] = version_id_marker
            
            # List versions for this page
            versions_response = s3_client_origin.list_object_versions(**params)
            
            # Process regular versions
            if "Versions" in versions_response:
                for ver in versions_response["Versions"]:
                    # Only consider exact key matches (prefix could return other keys)
                    if ver["Key"] != key:
                        continue
                        
                    # Only consider versions before START_TIME
                    if filter_version_by_start_time(ver, START_TIME):
                        # Keep the version if it's the first one found or newer than what we have
                        if candidate is None or ver["LastModified"] > candidate_time:
                            candidate = ver
                            candidate_time = ver["LastModified"]
            
            # Process delete markers
            if "DeleteMarkers" in versions_response:
                for dm in versions_response["DeleteMarkers"]:
                    # Only consider exact key matches
                    if dm["Key"] != key:
                        continue
                    
                    # Only consider delete markers before START_TIME
                    if filter_version_by_start_time(dm, START_TIME):
                        # Track the newest delete marker
                        if latest_delete_marker is None or dm["LastModified"] > latest_delete_marker_time:
                            latest_delete_marker = dm
                            latest_delete_marker_time = dm["LastModified"]
            
            # Update pagination markers
            is_truncated = versions_response.get('IsTruncated', False)
            if is_truncated:
                key_marker = versions_response.get('NextKeyMarker')
                version_id_marker = versions_response.get('NextVersionIdMarker')
            else:
                break
        
        # If the most recent delete marker is newer than the most recent version,
        # or if no suitable version found, the object should be considered non-existent at START_TIME
        if candidate is None or (latest_delete_marker_time is not None and latest_delete_marker_time > candidate_time):
            return None
            
        # Get the specific version's complete metadata
        version_id = candidate["VersionId"]
        origin_obj = s3_client_origin.head_object(
            Bucket=bucket, 
            Key=key, 
            VersionId=version_id
        )
        
        return origin_obj
        
    except Exception as e:
        logging.info(f"Error checking object at START_TIME: {str(e)}")
        return None

async def handle_if_none_match_star_put(
    full_path: str, original_headers: dict
) -> Optional[Response]:
    """
    Handle special case for PUT requests with If-None-Match: * by checking both overlay and origin.
    Returns a Response object if the precondition is not satisfied, None if request should proceed.
    """
    parts = full_path.split('/', 1)
    bucket = parts[0]
    key = parts[1] if len(parts) > 1 else ""
    overlay_path = f"{bucket}/{key}"
    
    # Initialize the overlay S3 client
    s3_client_overlay = get_overlay_s3_client()
    
    # First check if object exists in overlay
    try:
        s3_client_overlay.head_object(Bucket=OVERLAY_BUCKET, Key=overlay_path)
        # Object exists in overlay, return 412
        logging.info(f"If-None-Match: * condition not satisfied - object exists in overlay: {overlay_path}")
        return Response(
            content=b"<Error><Code>PreconditionFailed</Code><Message>At least one of the pre-conditions you specified did not hold</Message></Error>",
            status_code=412,
            headers={"Content-Type": "application/xml"}
        )
    except botocore.exceptions.ClientError as e:
        error_code = e.response.get("Error", {}).get("Code", "")
        
        # Handle 404 specifically - object doesn't exist in overlay
        if error_code == "NoSuchKey" or error_code == "404":
            # Now check origin at START_TIME
            try:
                origin_obj = check_object_at_start_time(bucket, key)
                if origin_obj:
                    # Object exists in origin at START_TIME, return 412
                    logging.info(f"If-None-Match: * condition not satisfied - object exists in origin at START_TIME: {bucket}/{key}")
                    return Response(
                        content=b"<Error><Code>PreconditionFailed</Code><Message>At least one of the pre-conditions you specified did not hold</Message></Error>",
                        status_code=412,
                        headers={"Content-Type": "application/xml"}
                    )
            except Exception as ex:
                logging.error(f"Error checking origin for If-None-Match: * condition: {ex}")
                # Continue with regular request on error (safer than blocking)
        else:
            # For any other error from overlay check, pass through the error
            logging.error(f"Error checking overlay for If-None-Match: * condition: {e}")
            return Response(
                content=str(e).encode("utf-8"),
                status_code=e.response.get("ResponseMetadata", {}).get("HTTPStatusCode", 500),
                headers={"Content-Type": "text/plain"}
            )
    
    # All checks passed, proceed with regular request flow
    return None

@app.api_route("/{full_path:path}", methods=["GET", "PUT", "DELETE", "HEAD"])
async def proxy(full_path: str, request: Request):
    method = request.method
    original_headers = dict(request.headers)
    logging.info("Received %s request for %s", method, full_path)
    
    # Special case: PUT with If-None-Match
    if method == "PUT" and "if-none-match" in {k.lower() for k in original_headers.keys()}:
        if_none_match = original_headers.get("if-none-match")
        
        # Only If-None-Match: * is allowed for PUT, return 501 for any other value
        if if_none_match != "*":
            logging.info(f"Unsupported If-None-Match value for PUT: {if_none_match}")
            return Response(
                content=b"<Error><Code>NotImplemented</Code><Message>The If-None-Match header is only supported with value * for PUT operations</Message></Error>",
                status_code=501,
                headers={"Content-Type": "application/xml"}
            )
            
        # Special handling for If-None-Match: *
        # Pre-emptively check both overlay and origin before proceeding
        special_response = await handle_if_none_match_star_put(full_path, original_headers)
        if special_response:
            return special_response
    
    # Use filtered headers for overlay S3 request.
    overlay_headers = {
        k: v for k, v in original_headers.items() 
        if not k.lower().startswith("authorization") and not k.lower().startswith("x-amz")
    }
    body = await request.body()
    overlay_path = rewrite_overlay_path(full_path)
    overlay_url = f"{OVERLAY_S3_URL}/{quote(overlay_path)}"
    
    # Forward request to overlay
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
    
    # Handle conditional mutation failures (412 Precondition Failed)
    if response.status_code == 412 and method in {"PUT", "DELETE"}:
        response = await handle_conditional_mutation(method, full_path, original_headers, body, response)
    
    # Fallback to origin S3 for GET/HEAD if needed
    if method in {"GET", "HEAD"}:
        response = await handle_get_head_fallback(method, full_path, original_headers, body, response)
    
    return Response(
        content=response.content,
        status_code=response.status_code,
        headers={k: v for k, v in response.headers.items() if k.lower() not in {"content-encoding", "transfer-encoding"}}
    )
