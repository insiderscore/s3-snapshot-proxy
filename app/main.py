import httpx
from fastapi import FastAPI, Request, Response
import os
from urllib.parse import quote
from httpx_auth import AWS4Auth
import boto3
from datetime import datetime, timezone
import logging

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
    # Use filtered headers for overlay S3 request
    overlay_headers = {
        k: v
        for k, v in original_headers.items()
        if not k.lower().startswith("authorization") and not k.lower().startswith("x-amz")
    }
    body = await request.body()

    overlay_path = rewrite_overlay_path(full_path)
    overlay_url = f"{OVERLAY_S3_URL}/{quote(overlay_path)}"

    # Forward request to overlay S3 using our signed client.
    response = await signed_client.request(method, overlay_url, headers=overlay_headers, content=body)

    # Fallback only on GET or HEAD 404 if no delete marker is involved
    if method in {"GET", "HEAD"} and response.status_code == 404:
        if response.headers.get("x-amz-delete-marker", "false").lower() != "true":
            origin_url = f"{ORIGIN_S3_URL}/{quote(full_path)}"
            # Use the original headers (including any auth headers) for origin S3
            origin_headers = original_headers.copy()

            # Adjust If-Unmodified-Since header if needed
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

            response = await client.request(method, origin_url, headers=origin_headers, content=body)

    return Response(
        content=response.content,
        status_code=response.status_code,
        headers={k: v for k, v in response.headers.items() if k.lower() not in {"content-encoding", "transfer-encoding"}}
    )
