import httpx
from fastapi import FastAPI, Request, Response
import os
from urllib.parse import quote
from httpx_auth import AWS4Auth
import boto3
from datetime import datetime, timezone
import logging
import sys

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

# Unsigned client for origin S3
client = httpx.AsyncClient(follow_redirects=True)

# Signed client for overlay S3
session = boto3.Session()
credentials = session.get_credentials()
if credentials is None:
    logging.warning("No AWS credentials found; signed overlay requests may fail.")
else:
    aws_auth = AWS4Auth(
        access_id=credentials.access_key,
        secret_key=credentials.secret_key,
        region=os.environ.get("AWS_REGION", "us-east-1"),
        service="s3",
        security_token=credentials.token
    )
    signed_client = httpx.AsyncClient(auth=aws_auth, follow_redirects=True)

def rewrite_overlay_path(original_path: str) -> str:
    parts = original_path.strip("/").split("/", 1)
    if len(parts) == 2:
        bucket, key = parts
    else:
        bucket, key = parts[0], ""
    return f"{OVERLAY_BUCKET}/{bucket}/{key}"

@app.api_route("/{full_path:path}", methods=["GET", "PUT", "DELETE", "HEAD"])
async def proxy(full_path: str, request: Request):
    method = request.method
    original_headers = dict(request.headers)
    logging.info("Received %s request for %s", method, full_path)
    
    # Use filtered headers for overlay S3 request
    overlay_headers = {
        k: v
        for k, v in original_headers.items()
        if not k.lower().startswith("authorization") and not k.lower().startswith("x-amz")
    }
    body = await request.body()

    overlay_path = rewrite_overlay_path(full_path)
    overlay_url = f"{OVERLAY_S3_URL}/{quote(overlay_path)}"
    
    # For DELETE requests, apply our facilitator workaround:
    if method == "DELETE":
        # Create a zero-length facilitator object so that a deletion marker will be produced.
        facilitator_headers = overlay_headers.copy()
        facilitator_headers["x-rtwa-delete-marker-facilitator"] = "true"
        logging.info("Creating facilitator object for deletion marker workaround: PUT %s", overlay_url)
        facilitator_response = await signed_client.put(overlay_url, headers=facilitator_headers, content=b"")
        logging.info("Facilitator creation response status: %s", facilitator_response.status_code)
        
        # Now delete that facilitator object, which should create a proper delete marker.
        logging.info("Deleting facilitator object: DELETE %s", overlay_url)
        response = await signed_client.request("DELETE", overlay_url, headers=overlay_headers, content=body)
        logging.info("Delete response (workaround) status: %s, headers: %s", response.status_code, dict(response.headers))
    else:
        logging.info("Sending overlay request: %s %s", method, overlay_url)
        response = await signed_client.request(method, overlay_url, headers=overlay_headers, content=body)
        logging.info("Overlay response status: %s, headers: %s", response.status_code, dict(response.headers))
    
    # For GET or HEAD, if the overlay response includes the facilitator header, treat it as a deletion marker.
    if method in {"GET", "HEAD"} and response.headers.get("x-rtwa-delete-marker-facilitator", "false").lower() == "true":
        logging.info("Overlay response includes facilitator header; treating as delete marker (404)")
        response = httpx.Response(status_code=404, content=b"", headers=response.headers)

    # Fallback: if GET or HEAD from overlay returns 404 without a delete marker indicator, try origin.
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
            response = await client.request(method, origin_url, headers=origin_headers, content=body)
            logging.info("Origin response status: %s", response.status_code)

    return Response(
        content=response.content,
        status_code=response.status_code,
        headers={k: v for k, v in response.headers.items() if k.lower() not in {"content-encoding", "transfer-encoding"}}
    )
