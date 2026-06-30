import datetime as datetime_module
import asyncio
import hmac
import httpx
from fastapi import FastAPI, Request, Response
from fastapi.responses import StreamingResponse
import os
import argparse
from urllib.parse import parse_qsl, quote, unquote, urlsplit
from httpx_auth import AWS4Auth
from httpx_auth._aws import (
    _signing_key,
    _string_to_sign,
    canonical_and_signed_headers,
)
import boto3
from datetime import datetime, timezone
import logging
import sys
import xml.etree.ElementTree as ET
from typing import AsyncIterator, Optional
import botocore.exceptions
import hashlib

async def run_sync_s3(func, *args, **kwargs):
    return await asyncio.to_thread(func, *args, **kwargs)

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

DELETE_MARKER_FACILITATOR_METADATA = "s3-snapshot-proxy-delete-marker-facilitator"
DELETE_MARKER_FACILITATOR_BODY = b"s3-snapshot-proxy delete marker facilitator\n"
DELETE_MARKER_FACILITATOR_ETAG = '"' + hashlib.md5(DELETE_MARKER_FACILITATOR_BODY).hexdigest() + '"'
LEGACY_DELETE_MARKER_FACILITATOR_ETAG = '"' + hashlib.md5(b"").hexdigest() + '"'
TAG_FACILITATOR_METADATA = "s3-snapshot-proxy-tag-facilitator"
TAG_FACILITATOR_BODY = b"s3-snapshot-proxy tag facilitator\n"
TAG_FACILITATOR_ETAG = '"' + hashlib.md5(TAG_FACILITATOR_BODY).hexdigest() + '"'
MAX_CONTROL_BODY_BYTES = int(os.environ.get("MAX_CONTROL_BODY_BYTES", str(10 * 1024 * 1024)))
S3_SIGNED_PASSTHROUGH_HEADERS = {
    "cache-control",
    "content-disposition",
    "content-encoding",
    "content-language",
    "content-md5",
    "content-type",
    "expires",
}

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
        s3_client_overlay = await run_sync_s3(
            boto3.client,
            "s3",
            aws_access_key_id=overlay_credentials.access_key,
            aws_secret_access_key=overlay_credentials.secret_key,
            aws_session_token=overlay_credentials.token if hasattr(overlay_credentials, 'token') else None,
            endpoint_url=OVERLAY_S3_URL
        )
        # Check if overlay bucket exists
        await run_sync_s3(s3_client_overlay.head_bucket, Bucket=OVERLAY_BUCKET)
        status["components"]["overlay_s3"] = "connected"
    except Exception as e:
        logging.warning(f"Overlay S3 connection failed: {str(e)}")
        status["ready"] = False
        status["components"]["overlay_s3"] = f"connection_failed: {str(e)}"
    
    # We don't strictly need to check origin S3 if we're just reading from overlay
    # But include a basic check that credentials are valid
    try:
        s3_client_origin = await run_sync_s3(
            boto3.client,
            "s3",
            aws_access_key_id=origin_credentials.access_key,
            aws_secret_access_key=origin_credentials.secret_key,
            aws_session_token=origin_credentials.token if hasattr(origin_credentials, 'token') else None,
            endpoint_url=ORIGIN_S3_URL
        )
        # Just check if we can access the service
        await run_sync_s3(s3_client_origin.list_buckets)
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

class S3UnsignedPayloadAWS4Auth(AWS4Auth):
    requires_request_body = False

    def auth_flow(self, request):
        date = datetime_module.datetime.now(datetime_module.timezone.utc)
        request.headers["x-amz-date"] = date.strftime("%Y%m%dT%H%M%SZ")
        payload_hash = request.headers.get("x-amz-content-sha256", "")
        if not is_reusable_payload_hash(payload_hash):
            payload_hash = "UNSIGNED-PAYLOAD"
        request.headers["x-amz-content-sha256"] = payload_hash

        if self.security_token:
            request.headers["x-amz-security-token"] = self.security_token

        canonical_headers, signed_headers = canonical_and_signed_headers(
            request.headers,
            self.include_headers,
        )
        canonical_request = self._canonical_request(
            request,
            canonical_headers,
            signed_headers,
        )
        scope = f"{date.strftime('%Y%m%d')}/{self.region}/{self.service}/aws4_request"
        string_to_sign = _string_to_sign(request, canonical_request, scope)
        signing_key = _signing_key(
            self.secret_key,
            self.region,
            self.service,
            date.strftime("%Y%m%d"),
        )
        signature = hmac.new(
            signing_key,
            string_to_sign.encode("utf-8"),
            hashlib.sha256,
        ).hexdigest()

        auth_str = "AWS4-HMAC-SHA256 "
        auth_str += f"Credential={self.access_id}/{scope}, "
        auth_str += f"SignedHeaders={signed_headers}, "
        auth_str += f"Signature={signature}"
        request.headers["Authorization"] = auth_str
        yield request

# Build AWS4Auth objects.
origin_aws_auth = S3UnsignedPayloadAWS4Auth(
    access_id=origin_credentials.access_key,
    secret_key=origin_credentials.secret_key,
    region=os.environ.get("AWS_REGION", "us-east-1"),
    service="s3",
    security_token=origin_credentials.token,
    include_headers=S3_SIGNED_PASSTHROUGH_HEADERS,
)

overlay_aws_auth = S3UnsignedPayloadAWS4Auth(
    access_id=overlay_credentials.access_key,
    secret_key=overlay_credentials.secret_key,
    region=os.environ.get("AWS_REGION", "us-east-1"),
    service="s3",
    security_token=overlay_credentials.token,
    include_headers=S3_SIGNED_PASSTHROUGH_HEADERS,
)

# Unsigned client for origin S3 (or re-sign with origin_aws_auth when needed)
client = httpx.AsyncClient(follow_redirects=True)

# Signed client for overlay S3 using overlay_aws_auth
signed_client = httpx.AsyncClient(auth=overlay_aws_auth, follow_redirects=True)

async def forward_s3_request(
    http_client: httpx.AsyncClient,
    method: str,
    url: str,
    headers: Optional[dict] = None,
    content: bytes = b"",
    auth=None,
) -> httpx.Response:
    request = http_client.build_request(
        method,
        url,
        headers=headers,
        content=content,
    )
    send_kwargs = {"stream": True, "follow_redirects": True}
    if auth is not None:
        send_kwargs["auth"] = auth

    response = await http_client.send(request, **send_kwargs)
    try:
        raw_content = b"".join([chunk async for chunk in response.aiter_raw()])
    finally:
        await response.aclose()

    return httpx.Response(
        status_code=response.status_code,
        headers=response.headers,
        content=raw_content,
        request=response.request,
    )

async def open_s3_stream(
    http_client: httpx.AsyncClient,
    method: str,
    url: str,
    headers: Optional[dict] = None,
    content=None,
    auth=None,
) -> httpx.Response:
    request = http_client.build_request(
        method,
        url,
        headers=headers,
        content=content if content is not None else b"",
    )
    send_kwargs = {"stream": True, "follow_redirects": True}
    if auth is not None:
        send_kwargs["auth"] = auth
    return await http_client.send(request, **send_kwargs)

async def request_body_stream(request: Request) -> AsyncIterator[bytes]:
    async for chunk in request.stream():
        if chunk:
            yield chunk

class AWSChunkedBodyReader:
    def __init__(self, source: AsyncIterator[bytes]):
        self.source = source.__aiter__()
        self.buffer = bytearray()
        self.done = False

    async def fill(self):
        if self.done:
            return
        try:
            chunk = await self.source.__anext__()
        except StopAsyncIteration:
            self.done = True
            return
        if chunk:
            self.buffer.extend(chunk)

    async def read_line(self, limit: int = 8192) -> bytes:
        while True:
            line_end = self.buffer.find(b"\r\n")
            if line_end >= 0:
                line = bytes(self.buffer[:line_end])
                del self.buffer[:line_end + 2]
                return line
            if self.done:
                raise AWSChunkedDecodeError("Malformed aws-chunked body: missing line terminator")
            if len(self.buffer) > limit:
                raise AWSChunkedDecodeError("Malformed aws-chunked body: chunk header too large")
            await self.fill()

    async def iter_exact(self, size: int) -> AsyncIterator[bytes]:
        remaining = size
        while remaining:
            while not self.buffer and not self.done:
                await self.fill()
            if not self.buffer:
                raise AWSChunkedDecodeError("Malformed aws-chunked body: truncated chunk data")
            take = min(remaining, len(self.buffer))
            data = bytes(self.buffer[:take])
            del self.buffer[:take]
            remaining -= take
            yield data

    async def read_exact_bytes(self, size: int) -> bytes:
        return b"".join([chunk async for chunk in self.iter_exact(size)])

async def aws_chunked_body_stream(source: AsyncIterator[bytes]) -> AsyncIterator[bytes]:
    reader = AWSChunkedBodyReader(source)
    while True:
        line = await reader.read_line()
        size_token = line.split(b";", 1)[0].strip()
        try:
            chunk_size = int(size_token, 16)
        except ValueError as exc:
            raise AWSChunkedDecodeError("Malformed aws-chunked body: invalid chunk size") from exc

        if chunk_size == 0:
            while True:
                trailer_line = await reader.read_line()
                if trailer_line == b"":
                    return

        async for data in reader.iter_exact(chunk_size):
            yield data

        if await reader.read_exact_bytes(2) != b"\r\n":
            raise AWSChunkedDecodeError("Malformed aws-chunked body: missing chunk terminator")

class ControlBodyTooLarge(Exception):
    pass

class AWSChunkedDecodeError(Exception):
    pass

async def read_control_body(request: Request) -> bytes:
    chunks = []
    total = 0
    async for chunk in request.stream():
        total += len(chunk)
        if total > MAX_CONTROL_BODY_BYTES:
            raise ControlBodyTooLarge()
        chunks.append(chunk)
    return b"".join(chunks)

async def finalize_early_body_response(request: Request, response: Response) -> Response:
    length_header = get_header(request.headers, "content-length")
    try:
        content_length = int(length_header) if length_header is not None else None
    except ValueError:
        content_length = None

    if content_length is None or content_length > MAX_CONTROL_BODY_BYTES:
        response.headers["Connection"] = "close"
        return response

    try:
        async for _ in request.stream():
            pass
    except Exception:
        response.headers["Connection"] = "close"
    return response

def rewrite_overlay_path(original_path: str) -> str:
    bucket, key = split_bucket_key(original_path)
    if key:
        return f"{OVERLAY_BUCKET}/{bucket}/{key}"
    return f"{OVERLAY_BUCKET}/{bucket}"

def split_bucket_key(path: str) -> tuple[str, str]:
    parts = path.lstrip("/").split("/", 1)
    if len(parts) == 2:
        return parts[0], parts[1]
    return parts[0], ""

def append_query(url: str, query_string: str) -> str:
    if query_string:
        return f"{url}?{query_string}"
    return url

def query_has_param(query_string: str, param_name: str) -> bool:
    target = param_name.lower()
    return any(name.lower() == target for name, _ in parse_qsl(query_string, keep_blank_values=True))

def is_multipart_upload_subresource(query_string: str) -> bool:
    return query_has_param(query_string, "uploads") or query_has_param(query_string, "uploadId")

def get_header(headers: dict, name: str) -> Optional[str]:
    target = name.lower()
    for key, value in headers.items():
        if key.lower() == target:
            return value
    return None

def has_header(headers: dict, name: str) -> bool:
    target = name.lower()
    return any(key.lower() == target for key in headers)

def set_header(headers: dict, name: str, value: str):
    target = name.lower()
    for key in list(headers):
        if key.lower() == target:
            headers.pop(key, None)
    headers[name] = value

def is_reusable_payload_hash(value: str) -> bool:
    normalized = value.strip()
    if normalized == "UNSIGNED-PAYLOAD":
        return True
    return len(normalized) == 64 and all(ch in "0123456789abcdefABCDEF" for ch in normalized)

def content_encoding_values(headers: dict) -> list[str]:
    value = get_header(headers, "content-encoding") or ""
    return [item.strip().lower() for item in value.split(",") if item.strip()]

def request_uses_aws_chunked_payload(headers: dict) -> bool:
    payload_hash = (get_header(headers, "x-amz-content-sha256") or "").strip()
    return (
        "aws-chunked" in content_encoding_values(headers)
        or payload_hash.startswith("STREAMING-AWS4-")
    )

def precondition_failed_response() -> httpx.Response:
    return httpx.Response(
        status_code=412,
        content=b"<Error><Code>PreconditionFailed</Code><Message>At least one of the pre-conditions you specified did not hold</Message></Error>",
        headers={"Content-Type": "application/xml"},
    )

def not_implemented_response(message: str) -> Response:
    return Response(
        content=f"<Error><Code>NotImplemented</Code><Message>{message}</Message></Error>".encode("utf-8"),
        status_code=501,
        headers={"Content-Type": "application/xml"},
    )

def s3_error_response(code: str, message: str, status_code: int) -> Response:
    root = ET.Element("Error")
    ET.SubElement(root, "Code").text = code
    ET.SubElement(root, "Message").text = message
    return Response(
        content=ET.tostring(root, encoding="utf-8", method="xml"),
        status_code=status_code,
        headers={"Content-Type": "application/xml"},
    )

def filtered_response_headers(headers, preserve_content_length: bool = False) -> dict:
    excluded = {"connection", "transfer-encoding"}
    if not preserve_content_length:
        excluded.add("content-length")
    return {
        k: v
        for k, v in headers.items()
        if k.lower() not in excluded
    }

def response_from_httpx(response: httpx.Response) -> Response:
    return Response(
        content=response.content,
        status_code=response.status_code,
        headers=filtered_response_headers(response.headers),
    )

def streaming_response_from_httpx(response: httpx.Response) -> StreamingResponse:
    if (
        not isinstance(response.stream, httpx.AsyncByteStream)
        or getattr(response, "is_stream_consumed", False)
    ):
        return response_from_httpx(response)

    async def body_iter():
        try:
            async for chunk in response.aiter_raw():
                yield chunk
        finally:
            await response.aclose()

    return StreamingResponse(
        body_iter(),
        status_code=response.status_code,
        headers=filtered_response_headers(response.headers, preserve_content_length=True),
    )

def overlay_header_value(name: str, value: str, preserve_content_length: bool = False) -> Optional[str]:
    lower_name = name.lower()
    if lower_name in {"connection", "host", "transfer-encoding"}:
        return None
    if lower_name == "content-length" and not preserve_content_length:
        return None
    if lower_name == "content-encoding":
        encodings = [
            item.strip()
            for item in value.split(",")
            if item.strip() and item.strip().lower() != "aws-chunked"
        ]
        if not encodings:
            return None
        return ", ".join(encodings)
    if lower_name.startswith("authorization"):
        return None
    if lower_name.startswith("x-amz-meta-"):
        return value
    if lower_name == "x-amz-tagging":
        return value
    if lower_name == "x-amz-server-side-encryption":
        return value
    if lower_name == "x-amz-content-sha256" and is_reusable_payload_hash(value):
        return value
    if lower_name.startswith("x-amz"):
        return None
    return value

def should_forward_overlay_header(name: str) -> bool:
    return overlay_header_value(name, "placeholder") is not None

def prepare_overlay_headers(headers: dict, preserve_content_length: bool = False) -> dict:
    result = {}
    for key, value in headers.items():
        forwarded_value = overlay_header_value(
            key,
            value,
            preserve_content_length=preserve_content_length,
        )
        if forwarded_value is not None:
            result[key] = forwarded_value
    return result

def remove_body_integrity_headers(headers: dict):
    for key in list(headers):
        lower_key = key.lower()
        if (
            lower_key == "content-md5"
            or lower_key == "x-amz-content-sha256"
            or lower_key == "x-amz-sdk-checksum-algorithm"
            or lower_key.startswith("x-amz-checksum-")
        ):
            headers.pop(key, None)

def prepare_overlay_body_stream(
    original_headers: dict,
    overlay_headers: dict,
    body_stream: AsyncIterator[bytes],
) -> tuple[Optional[AsyncIterator[bytes]], Optional[Response]]:
    if not request_uses_aws_chunked_payload(original_headers):
        return body_stream, None

    decoded_content_length = get_header(original_headers, "x-amz-decoded-content-length")
    if not decoded_content_length or not decoded_content_length.strip().isdigit():
        return None, s3_error_response(
            "MissingContentLength",
            "aws-chunked uploads require x-amz-decoded-content-length.",
            411,
        )

    set_header(overlay_headers, "Content-Length", decoded_content_length.strip())
    return aws_chunked_body_stream(body_stream), None

def parse_copy_source(value: str) -> tuple[Optional[str], Optional[str], str]:
    source = value.strip()
    query = ""
    if source.startswith("http://") or source.startswith("https://"):
        parsed = urlsplit(source)
        source = parsed.path
        query = parsed.query
    elif "?" in source:
        source, query = source.split("?", 1)

    source = unquote(source.lstrip("/"))
    if "/" not in source:
        return None, None, query

    bucket, key = source.split("/", 1)
    if not bucket or not key:
        return None, None, query
    return bucket, key, query

def copy_source_has_version_id(query: str) -> bool:
    return any(name.lower() == "versionid" for name, _ in parse_qsl(query, keep_blank_values=True))

def copy_source_header_value(bucket: str, key: str) -> str:
    overlay_source = f"{bucket}/{key}"
    return f"/{quote(OVERLAY_BUCKET, safe='')}/{quote(overlay_source, safe='/')}"

async def handle_copy_object_request(
    full_path: str,
    query_string: str,
    original_headers: dict,
    overlay_url: str,
) -> Response:
    copy_source = get_header(original_headers, "x-amz-copy-source")
    if not copy_source:
        return not_implemented_response("CopyObject requires x-amz-copy-source")

    source_bucket, source_key, source_query = parse_copy_source(copy_source)
    if source_bucket is None or source_key is None:
        return s3_error_response("InvalidArgument", "Invalid x-amz-copy-source header.", 400)
    if copy_source_has_version_id(source_query):
        return not_implemented_response("Versioned CopyObject sources are not implemented")

    source_overlay_path = f"{source_bucket}/{source_key}"
    source_overlay_url = f"{OVERLAY_S3_URL}/{quote(OVERLAY_BUCKET, safe='')}/{quote(source_overlay_path, safe='/')}"
    source_head = await forward_s3_request(signed_client, "HEAD", source_overlay_url)
    if (
        source_head.status_code == 404
        or (get_header(source_head.headers, "x-amz-delete-marker") or "").lower() == "true"
        or response_has_facilitator_metadata(source_head.headers)
        or response_has_tag_facilitator_metadata(source_head.headers)
    ):
        return not_implemented_response("CopyObject from origin-backed objects is not implemented")
    if source_head.status_code >= 400:
        return response_from_httpx(source_head)

    copy_headers = prepare_overlay_headers(original_headers, preserve_content_length=False)
    remove_body_integrity_headers(copy_headers)
    set_header(copy_headers, "x-amz-copy-source", copy_source_header_value(source_bucket, source_key))

    response = await forward_s3_request(
        signed_client,
        "PUT",
        overlay_url,
        headers=copy_headers,
        content=b"",
    )
    return response_from_httpx(response)

def virtual_object_location(request: Request, bucket: str, key: str) -> str:
    quoted_bucket = quote(bucket, safe="")
    quoted_key = quote(key, safe="/")
    return f"{request.url.scheme}://{request.url.netloc}/{quoted_bucket}/{quoted_key}"

def rewrite_multipart_xml_response(
    response: httpx.Response,
    full_path: str,
    request: Request,
) -> httpx.Response:
    if response.status_code >= 400 or not response.content:
        return response

    try:
        root = ET.fromstring(response.content)
    except ET.ParseError:
        return response

    if root.tag.startswith("{"):
        namespace = root.tag[1:].split("}", 1)[0]
        ET.register_namespace("", namespace)

    bucket, key = split_bucket_key(full_path)
    location = virtual_object_location(request, bucket, key)
    changed = False

    for elem in root.iter():
        local_name = elem.tag.rsplit("}", 1)[-1]
        if local_name == "Bucket":
            elem.text = bucket
            changed = True
        elif local_name == "Key":
            elem.text = key
            changed = True
        elif local_name == "Location":
            elem.text = location
            changed = True

    if not changed:
        return response

    headers = dict(response.headers)
    headers.pop("content-length", None)
    return httpx.Response(
        status_code=response.status_code,
        headers=headers,
        content=ET.tostring(root, encoding="utf-8", method="xml"),
    )

COMPLETE_MULTIPART_CHECKSUM_ELEMENTS = {
    "ChecksumCRC32",
    "ChecksumCRC32C",
    "ChecksumCRC64NVME",
    "ChecksumSHA1",
    "ChecksumSHA256",
}

def strip_complete_multipart_upload_checksums(body: bytes) -> tuple[bytes, bool]:
    try:
        root = ET.fromstring(body)
    except ET.ParseError:
        return body, False

    if xml_local_name(root.tag) != "CompleteMultipartUpload":
        return body, False

    if root.tag.startswith("{"):
        namespace = root.tag[1:].split("}", 1)[0]
        ET.register_namespace("", namespace)

    changed = False
    for part in root:
        if xml_local_name(part.tag) != "Part":
            continue
        for child in list(part):
            if xml_local_name(child.tag) in COMPLETE_MULTIPART_CHECKSUM_ELEMENTS:
                part.remove(child)
                changed = True

    if not changed:
        return body, False
    return ET.tostring(root, encoding="utf-8", method="xml"), True

async def handle_delete_request(overlay_url: str, overlay_headers: dict, body: bytes) -> httpx.Response:
    """
    Handle DELETE requests against the versioned overlay bucket.
    """
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
    
    facilitator_headers = overlay_headers.copy()
    facilitator_headers.pop("content-length", None)
    facilitator_headers.pop("Content-Length", None)
    remove_body_integrity_headers(facilitator_headers)
    facilitator_headers[f"x-amz-meta-{DELETE_MARKER_FACILITATOR_METADATA}"] = "true"
    logging.info("Creating facilitator object for deletion marker compatibility: PUT %s", overlay_url)
    facilitator_response = await forward_s3_request(
        signed_client,
        "PUT",
        overlay_url,
        headers=facilitator_headers,
        content=DELETE_MARKER_FACILITATOR_BODY,
    )
    logging.info("Facilitator creation response status: %s", facilitator_response.status_code)
    if facilitator_response.status_code >= 400:
        return facilitator_response

    logging.info("Deleting facilitator object: DELETE %s", overlay_url)
    response = await forward_s3_request(signed_client, "DELETE", overlay_url, headers=overlay_headers, content=body)
    logging.info("Delete response status: %s, headers: %s", response.status_code, dict(response.headers))
    return response

def tagging_xml_response(tag_set) -> Response:
    root = ET.Element("Tagging")
    tagset_elem = ET.SubElement(root, "TagSet")
    for tag in tag_set:
        tag_elem = ET.SubElement(tagset_elem, "Tag")
        ET.SubElement(tag_elem, "Key").text = tag.get("Key", "")
        ET.SubElement(tag_elem, "Value").text = tag.get("Value", "")
    return Response(
        content=ET.tostring(root, encoding="utf-8", method="xml"),
        media_type="application/xml",
    )

def overlay_current_version_is_delete_marker(overlay_key: str) -> bool:
    s3_client_overlay = get_overlay_s3_client()
    try:
        s3_client_overlay.head_object(Bucket=OVERLAY_BUCKET, Key=overlay_key)
        return False
    except botocore.exceptions.ClientError as exc:
        headers = exc.response.get("ResponseMetadata", {}).get("HTTPHeaders", {})
        return (get_header(headers, "x-amz-delete-marker") or "").lower() == "true"

def head_overlay_object(overlay_key: str):
    return get_overlay_s3_client().head_object(Bucket=OVERLAY_BUCKET, Key=overlay_key)

async def create_tag_facilitator_object(overlay_object_url: str) -> httpx.Response:
    headers = {
        f"x-amz-meta-{TAG_FACILITATOR_METADATA}": "true",
    }
    logging.info("Creating tag facilitator object: PUT %s", overlay_object_url)
    return await forward_s3_request(
        signed_client,
        "PUT",
        overlay_object_url,
        headers=headers,
        content=TAG_FACILITATOR_BODY,
    )

async def handle_object_tagging_request(
    method: str,
    full_path: str,
    query_string: str,
    original_headers: dict,
    body: bytes,
) -> Response:
    bucket, key = split_bucket_key(full_path)
    if not key:
        return not_implemented_response("Bucket tagging is not implemented")

    overlay_headers = prepare_overlay_headers(
        original_headers,
        preserve_content_length=method == "PUT",
    )
    overlay_path = rewrite_overlay_path(full_path)
    overlay_object_url = f"{OVERLAY_S3_URL}/{quote(overlay_path)}"
    overlay_url = append_query(overlay_object_url, query_string)

    response = await forward_s3_request(signed_client, method, overlay_url, headers=overlay_headers, content=body)
    if response.status_code != 404 or query_has_param(query_string, "versionId"):
        return response_from_httpx(response)

    origin_obj = await run_sync_s3(check_object_at_start_time, bucket, key)
    overlay_is_delete_marker = await run_sync_s3(overlay_current_version_is_delete_marker, overlay_path)
    if origin_obj is None or overlay_is_delete_marker:
        return response_from_httpx(response)

    if method == "GET":
        try:
            tagging = await run_sync_s3(
                lambda: get_origin_s3_client().get_object_tagging(
                    Bucket=bucket,
                    Key=key,
                    VersionId=origin_obj["VersionId"],
                )
            )
        except botocore.exceptions.ClientError:
            return response_from_httpx(response)
        return tagging_xml_response(tagging.get("TagSet", []))

    if method not in {"PUT", "DELETE"}:
        return response_from_httpx(response)

    facilitator_response = await create_tag_facilitator_object(overlay_object_url)
    if facilitator_response.status_code >= 400:
        return response_from_httpx(facilitator_response)

    response = await forward_s3_request(signed_client, method, overlay_url, headers=overlay_headers, content=body)
    return response_from_httpx(response)

def xml_local_name(tag: str) -> str:
    return tag.rsplit("}", 1)[-1]

def child_text(parent: ET.Element, name: str) -> Optional[str]:
    for child in parent:
        if xml_local_name(child.tag) == name:
            return child.text or ""
    return None

def parse_delete_objects_request(body: bytes):
    try:
        root = ET.fromstring(body)
    except ET.ParseError:
        return None, s3_error_response(
            "MalformedXML",
            "The XML you provided was not well-formed or did not validate against our published schema",
            400,
        )

    if xml_local_name(root.tag) != "Delete":
        return None, s3_error_response(
            "MalformedXML",
            "The XML you provided was not well-formed or did not validate against our published schema",
            400,
        )

    quiet = (child_text(root, "Quiet") or "").strip().lower() == "true"
    objects = []
    for elem in root:
        if xml_local_name(elem.tag) != "Object":
            continue

        key = child_text(elem, "Key")
        if key is None:
            return None, s3_error_response(
                "MalformedXML",
                "The XML you provided was not well-formed or did not validate against our published schema",
                400,
            )

        version_id = child_text(elem, "VersionId")
        conditions = [
            xml_local_name(child.tag)
            for child in elem
            if xml_local_name(child.tag) in {"ETag", "LastModifiedTime", "Size"}
        ]
        objects.append({
            "Key": key,
            "VersionId": version_id if version_id else None,
            "Conditions": conditions,
        })

    if not objects or len(objects) > 1000:
        return None, s3_error_response(
            "MalformedXML",
            "The XML you provided was not well-formed or did not validate against our published schema",
            400,
        )

    return {"Quiet": quiet, "Objects": objects}, None

def append_delete_item_xml(root: ET.Element, tag: str, item: dict):
    elem = ET.SubElement(root, tag)
    ET.SubElement(elem, "Key").text = item.get("Key", "")
    if item.get("VersionId"):
        ET.SubElement(elem, "VersionId").text = item["VersionId"]
    if "DeleteMarker" in item:
        ET.SubElement(elem, "DeleteMarker").text = "true" if item["DeleteMarker"] else "false"
    if item.get("DeleteMarkerVersionId"):
        ET.SubElement(elem, "DeleteMarkerVersionId").text = item["DeleteMarkerVersionId"]
    if tag == "Error":
        ET.SubElement(elem, "Code").text = item.get("Code", "InternalError")
        ET.SubElement(elem, "Message").text = item.get("Message", "")

def delete_objects_result_xml(deleted: list[dict], errors: list[dict], quiet: bool) -> bytes:
    root = ET.Element("DeleteResult")
    if not quiet:
        for item in deleted:
            append_delete_item_xml(root, "Deleted", item)
    for item in errors:
        append_delete_item_xml(root, "Error", item)
    return ET.tostring(root, encoding="utf-8", method="xml")

def rewrite_delete_result_item(item: dict, bucket: str) -> dict:
    result = dict(item)
    hidden_prefix = f"{bucket}/"
    key = result.get("Key", "")
    if key.startswith(hidden_prefix):
        result["Key"] = key[len(hidden_prefix):]
    return result

def version_exists(client, bucket: str, key: str, version_id: str) -> bool:
    marker = {}
    while True:
        params = {"Bucket": bucket, "Prefix": key}
        params.update(marker)
        response = client.list_object_versions(**params)

        for group in ("Versions", "DeleteMarkers"):
            for item in response.get(group, []):
                if item.get("Key") == key and item.get("VersionId") == version_id:
                    return True

        if not response.get("IsTruncated"):
            return False

        marker = {"KeyMarker": response.get("NextKeyMarker")}
        if response.get("NextVersionIdMarker"):
            marker["VersionIdMarker"] = response["NextVersionIdMarker"]

def delete_objects_item_error(item: dict, code: str, message: str) -> dict:
    error = {
        "Key": item["Key"],
        "Code": code,
        "Message": message,
    }
    if item.get("VersionId"):
        error["VersionId"] = item["VersionId"]
    return error

def create_delete_marker_facilitator(s3_client_overlay, overlay_key: str):
    s3_client_overlay.put_object(
        Bucket=OVERLAY_BUCKET,
        Key=overlay_key,
        Body=DELETE_MARKER_FACILITATOR_BODY,
        Metadata={DELETE_MARKER_FACILITATOR_METADATA: "true"},
    )

def _handle_multi_object_delete_request_sync(full_path: str, body: bytes) -> Response:
    bucket, key = split_bucket_key(full_path)
    if key:
        return s3_error_response(
            "MalformedXML",
            "The XML you provided was not well-formed or did not validate against our published schema",
            400,
        )

    parsed, error_response = parse_delete_objects_request(body)
    if error_response:
        return error_response

    s3_client_overlay = get_overlay_s3_client()
    s3_client_origin = get_origin_s3_client()
    errors = []
    overlay_delete_objects = []

    for item in parsed["Objects"]:
        overlay_key = f"{bucket}/{item['Key']}"

        if item["Conditions"]:
            errors.append(delete_objects_item_error(
                item,
                "NotImplemented",
                "Conditional multi-object delete entries are not implemented",
            ))
            continue

        version_id = item.get("VersionId")
        if version_id:
            try:
                if version_exists(s3_client_overlay, OVERLAY_BUCKET, overlay_key, version_id):
                    overlay_delete_objects.append({"Key": overlay_key, "VersionId": version_id})
                    continue
                if version_exists(s3_client_origin, bucket, item["Key"], version_id):
                    errors.append(delete_objects_item_error(
                        item,
                        "AccessDenied",
                        "Cannot delete versions that exist only in the origin bucket",
                    ))
                    continue
            except botocore.exceptions.ClientError as exc:
                response_error = exc.response.get("Error", {})
                errors.append(delete_objects_item_error(
                    item,
                    response_error.get("Code", "InternalError"),
                    response_error.get("Message", str(exc)),
                ))
                continue

            overlay_delete_objects.append({"Key": overlay_key, "VersionId": version_id})
            continue

        try:
            create_delete_marker_facilitator(s3_client_overlay, overlay_key)
        except botocore.exceptions.ClientError as exc:
            response_error = exc.response.get("Error", {})
            errors.append(delete_objects_item_error(
                item,
                response_error.get("Code", "InternalError"),
                response_error.get("Message", str(exc)),
            ))
            continue

        overlay_delete_objects.append({"Key": overlay_key})

    deleted = []
    if overlay_delete_objects:
        try:
            response = s3_client_overlay.delete_objects(
                Bucket=OVERLAY_BUCKET,
                Delete={"Objects": overlay_delete_objects, "Quiet": False},
            )
        except botocore.exceptions.ClientError as exc:
            response_error = exc.response.get("Error", {})
            for object_ref in overlay_delete_objects:
                virtual_item = rewrite_delete_result_item(object_ref, bucket)
                errors.append(delete_objects_item_error(
                    virtual_item,
                    response_error.get("Code", "InternalError"),
                    response_error.get("Message", str(exc)),
                ))
        else:
            deleted.extend(
                rewrite_delete_result_item(item, bucket)
                for item in response.get("Deleted", [])
            )
            errors.extend(
                rewrite_delete_result_item(item, bucket)
                for item in response.get("Errors", [])
            )

    return Response(
        content=delete_objects_result_xml(deleted, errors, parsed["Quiet"]),
        media_type="application/xml",
    )

async def handle_multi_object_delete_request(full_path: str, body: bytes) -> Response:
    return await run_sync_s3(_handle_multi_object_delete_request_sync, full_path, body)

async def handle_precondition_failure(
    method: str,
    full_path: str,
    query_string: str,
    original_headers: dict,
    body: bytes,
    response: httpx.Response,
) -> httpx.Response:
    """
    Handle precondition failures by checking object state at START_TIME
    """
    if response.status_code != 412:
        return response

    bucket, key = split_bucket_key(full_path)
        
    if query_has_param(query_string, "versionId"):
        logging.info("Request already specifies a version ID. Respecting original 412 response.")
        return response

    logging.info("Received 412. Checking object state at START_TIME for: %s, key: %s", bucket, key)
    
    # Use our comprehensive function that properly handles delete markers and pagination
    origin_obj = await run_sync_s3(check_object_at_start_time, bucket, key)
    
    if origin_obj is None:
        logging.info("No matching version found for key %s before START_TIME. Returning 404.", key)
        return httpx.Response(status_code=404, content=b"")

    version_id = origin_obj.get("VersionId")
    logging.info("Found version %s. Retrying origin request with version subresource.", version_id)
    
    # Create the version parameter string
    version_param = f"versionId={version_id}"
    
    # Construct URL with minimal changes
    if query_string:
        # Use & to append to existing query parameters
        origin_url = f"{ORIGIN_S3_URL}/{bucket}/{quote(key)}?{query_string}&{version_param}"
    else:
        # No existing query params, use ?
        origin_url = f"{ORIGIN_S3_URL}/{bucket}/{quote(key)}?{version_param}"
    
    new_response = await forward_s3_request(
        client, method, origin_url, headers=original_headers, auth=origin_aws_auth, content=body
    )
    return new_response

async def handle_get_head_fallback(
    method: str,
    full_path: str,
    query_string: str,
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
            origin_url = append_query(f"{ORIGIN_S3_URL}/{quote(full_path)}", query_string)
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
            new_response = await forward_s3_request(client, method, origin_url, headers=origin_headers, content=body)
            logging.info("Origin response status: %s", new_response.status_code)
            if new_response.status_code == 412:
                new_response = await handle_precondition_failure(
                    method,
                    full_path,
                    query_string,
                    original_headers,
                    body,
                    new_response,
                )
            return new_response
    return response

async def open_precondition_version_stream(
    method: str,
    full_path: str,
    query_string: str,
    original_headers: dict,
) -> httpx.Response:
    bucket, key = split_bucket_key(full_path)
    if query_has_param(query_string, "versionId"):
        return httpx.Response(status_code=412, content=b"")

    logging.info("Received 412. Checking object state at START_TIME for: %s, key: %s", bucket, key)
    origin_obj = await run_sync_s3(check_object_at_start_time, bucket, key)
    if origin_obj is None:
        logging.info("No matching version found for key %s before START_TIME. Returning 404.", key)
        return httpx.Response(status_code=404, content=b"")

    version_id = origin_obj.get("VersionId")
    logging.info("Found version %s. Retrying origin request with version subresource.", version_id)
    version_param = f"versionId={version_id}"
    if query_string:
        origin_url = f"{ORIGIN_S3_URL}/{bucket}/{quote(key)}?{query_string}&{version_param}"
    else:
        origin_url = f"{ORIGIN_S3_URL}/{bucket}/{quote(key)}?{version_param}"

    return await open_s3_stream(
        client,
        method,
        origin_url,
        headers=original_headers,
        auth=origin_aws_auth,
    )

async def open_get_head_response(
    method: str,
    full_path: str,
    query_string: str,
    original_headers: dict,
    overlay_url: str,
    overlay_headers: dict,
) -> httpx.Response:
    logging.info("Sending overlay request: %s %s", method, overlay_url)
    response = await open_s3_stream(
        signed_client,
        method,
        overlay_url,
        headers=overlay_headers,
    )
    logging.info("Overlay response status: %s, headers: %s", response.status_code, dict(response.headers))

    overlay_miss = response.status_code == 404
    if response_has_facilitator_metadata(response.headers):
        logging.info("Overlay response includes facilitator metadata; treating as delete marker (404)")
        overlay_miss = True
    if response_has_tag_facilitator_metadata(response.headers):
        logging.info("Overlay response includes tag facilitator metadata; treating as overlay miss")
        overlay_miss = True

    if not overlay_miss or response.headers.get("x-amz-delete-marker", "false").lower() == "true":
        return response

    await response.aclose()
    origin_url = append_query(f"{ORIGIN_S3_URL}/{quote(full_path)}", query_string)
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
    origin_response = await open_s3_stream(client, method, origin_url, headers=origin_headers)
    logging.info("Origin response status: %s", origin_response.status_code)
    origin_current_delete_marker = (
        origin_response.status_code == 404
        and (get_header(origin_response.headers, "x-amz-delete-marker") or "").lower() == "true"
    )
    if origin_response.status_code == 412 or origin_current_delete_marker:
        await origin_response.aclose()
        return await open_precondition_version_stream(
            method,
            full_path,
            query_string,
            original_headers,
        )

    return origin_response

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

def response_has_facilitator_metadata(headers) -> bool:
    header_name = f"x-amz-meta-{DELETE_MARKER_FACILITATOR_METADATA}"
    legacy_header_name = "x-rtwa-delete-marker-facilitator"
    return (
        (get_header(headers, header_name) or "false").lower() == "true"
        or (get_header(headers, legacy_header_name) or "false").lower() == "true"
    )

def response_has_tag_facilitator_metadata(headers) -> bool:
    header_name = f"x-amz-meta-{TAG_FACILITATOR_METADATA}"
    return (get_header(headers, header_name) or "false").lower() == "true"

def is_facilitator_version(s3_client, bucket, key, version):
    possible_current = (
        version.get("Size") == len(DELETE_MARKER_FACILITATOR_BODY)
        and version.get("ETag") == DELETE_MARKER_FACILITATOR_ETAG
    )
    possible_legacy = (
        version.get("Size") == 0
        and version.get("ETag") == LEGACY_DELETE_MARKER_FACILITATOR_ETAG
    )
    if not possible_current and not possible_legacy:
        return False

    try:
        response = s3_client.head_object(
            Bucket=bucket,
            Key=key,
            VersionId=version.get("VersionId"),
        )
    except Exception as exc:
        logging.info("Unable to inspect possible facilitator version %s/%s: %s", bucket, key, exc)
        return False

    metadata = response.get("Metadata", {})
    return (
        metadata.get(DELETE_MARKER_FACILITATOR_METADATA, "false").lower() == "true"
        or response_has_facilitator_metadata(response.get("ResponseMetadata", {}).get("HTTPHeaders", {}))
    )

def is_tag_facilitator_version(s3_client, bucket, key, version):
    possible = (
        version.get("Size") == len(TAG_FACILITATOR_BODY)
        and version.get("ETag") == TAG_FACILITATOR_ETAG
    )
    if not possible:
        return False

    try:
        response = s3_client.head_object(
            Bucket=bucket,
            Key=key,
            VersionId=version.get("VersionId"),
        )
    except Exception as exc:
        logging.info("Unable to inspect possible tag facilitator version %s/%s: %s", bucket, key, exc)
        return False

    metadata = response.get("Metadata", {})
    return (
        metadata.get(TAG_FACILITATOR_METADATA, "false").lower() == "true"
        or response_has_tag_facilitator_metadata(response.get("ResponseMetadata", {}).get("HTTPHeaders", {}))
    )

def is_invalid_list_prefix_error(exc) -> bool:
    if not isinstance(exc, botocore.exceptions.ClientError):
        return False
    return exc.response.get("Error", {}).get("Code") == "XMinioInvalidObjectName"

def version_item_sort_key(item):
    last_modified = item.get("LastModified")
    timestamp = last_modified.timestamp() if isinstance(last_modified, datetime) else 0
    return (
        item["Key"],
        -timestamp,
        item.get("VersionId", ""),
        item.get("ItemType", ""),
        item.get("Source", ""),
    )

def choose_merged_version_item(origin_item, overlay_item):
    if origin_item is None and overlay_item is None:
        return None, False, False
    if origin_item is None:
        return overlay_item, False, True
    if overlay_item is None:
        return origin_item, True, False
    if version_item_sort_key(origin_item) <= version_item_sort_key(overlay_item):
        return origin_item, True, False
    return overlay_item, False, True

class VersionItemStream:
    def __init__(
        self,
        s3_client,
        bucket,
        prefix,
        key_transform,
        source,
        before=None,
        key_marker=None,
        version_id_marker=None,
        delimiter=None,
        skip_facilitators=False,
    ):
        self.s3_client = s3_client
        self.bucket = bucket
        self.prefix = prefix
        self.key_transform = key_transform
        self.source = source
        self.before = before
        self.key_marker = key_marker
        self.version_id_marker = version_id_marker
        self.delimiter = delimiter
        self.skip_facilitators = skip_facilitators
        self.exhausted = False
        self.buffer = []

    def next_item(self):
        while not self.buffer:
            if not self._fetch_next_page():
                return None
        return self.buffer.pop(0)

    def _fetch_next_page(self):
        if self.exhausted:
            return False

        request_params = {
            "Bucket": self.bucket,
            "Prefix": self.prefix,
            "MaxKeys": 1000,
        }
        if self.delimiter:
            request_params["Delimiter"] = self.delimiter
        if self.key_marker:
            request_params["KeyMarker"] = self.key_marker
            if self.version_id_marker:
                request_params["VersionIdMarker"] = self.version_id_marker

        logging.info("Fetching ListObjectVersions params: %s", request_params)
        try:
            response = self.s3_client.list_object_versions(**request_params)
        except botocore.exceptions.ClientError as exc:
            if is_invalid_list_prefix_error(exc):
                logging.info("Treating invalid ListObjectVersions prefix as empty: %s", request_params)
                self.exhausted = True
                return False
            raise
        self.key_marker = response.get("NextKeyMarker")
        self.version_id_marker = response.get("NextVersionIdMarker")
        self.exhausted = not response.get("IsTruncated", False)

        items = []
        for group, item_type in (("Versions", "Version"), ("DeleteMarkers", "DeleteMarker")):
            for item in response.get(group, []):
                original_key = item["Key"]
                key = self.key_transform(original_key)
                if key is None:
                    continue
                if self.before is not None and item["LastModified"] >= self.before:
                    continue
                if (
                    self.skip_facilitators
                    and item_type == "Version"
                    and (
                        is_facilitator_version(self.s3_client, self.bucket, original_key, item)
                        or is_tag_facilitator_version(self.s3_client, self.bucket, original_key, item)
                    )
                ):
                    continue

                transformed = dict(item)
                transformed["Key"] = key
                transformed["ItemType"] = item_type
                transformed["Source"] = self.source
                transformed["OriginalKey"] = original_key
                items.append(transformed)

        for item in response.get("CommonPrefixes", []):
            original_key = item["Prefix"]
            key = self.key_transform(original_key)
            if key is None:
                continue
            items.append({
                "Key": key,
                "ItemType": "CommonPrefix",
                "Source": self.source,
                "OriginalKey": original_key,
            })

        items.sort(key=version_item_sort_key)
        self.buffer.extend(items)
        return bool(self.buffer) or not self.exhausted

def next_version_item(stream):
    return stream.next_item() if stream is not None else None

def version_item_after_marker(item, key_marker, version_id_marker, marker_seen):
    if not key_marker:
        return True, marker_seen
    if item["Key"] < key_marker:
        return False, marker_seen
    if item["Key"] > key_marker:
        return True, marker_seen
    if not version_id_marker:
        return False, marker_seen
    if marker_seen:
        return True, marker_seen
    if item.get("VersionId", "") == version_id_marker:
        return False, True
    return False, marker_seen

def list_versions_entry_for_item(item, prefix, delimiter):
    if item["ItemType"] == "CommonPrefix":
        return {
            "Type": "CommonPrefix",
            "Name": item["Key"],
            "Prefix": item["Key"],
            "MarkerKey": item["Key"],
            "MarkerVersionId": "",
        }

    key = item["Key"]
    suffix = key[len(prefix):]
    if delimiter is not None and delimiter and delimiter in suffix:
        common_prefix = prefix + suffix.split(delimiter, 1)[0] + delimiter
        return {
            "Type": "CommonPrefix",
            "Name": common_prefix,
            "Prefix": common_prefix,
            "MarkerKey": common_prefix,
            "MarkerVersionId": "",
        }

    return {
        "Type": item["ItemType"],
        "Name": key,
        "Object": item,
        "MarkerKey": key,
        "MarkerVersionId": item.get("VersionId", ""),
    }

def collect_list_object_versions_page(bucket, prefix, delimiter, max_keys, key_marker, version_id_marker):
    if max_keys <= 0:
        return [], False, "", ""

    use_source_marker = key_marker if key_marker and not version_id_marker else None
    origin_stream = VersionItemStream(
        get_origin_s3_client(),
        bucket,
        prefix or "",
        lambda key: key,
        "origin",
        before=START_TIME,
        key_marker=use_source_marker,
        delimiter=delimiter,
    )

    overlay_root = f"{bucket}/"
    overlay_source_marker = f"{overlay_root}{use_source_marker}" if use_source_marker else None
    overlay_stream = VersionItemStream(
        get_overlay_s3_client(),
        OVERLAY_BUCKET,
        f"{overlay_root}{prefix or ''}",
        lambda key: key[len(overlay_root):] if key.startswith(overlay_root) else None,
        "overlay",
        key_marker=overlay_source_marker,
        delimiter=delimiter,
        skip_facilitators=True,
    )

    origin_item = next_version_item(origin_stream)
    overlay_item = next_version_item(overlay_stream)
    entries = []
    seen_common_prefixes = set()
    latest_seen_keys = {key_marker} if key_marker and version_id_marker else set()
    marker_seen = not key_marker
    next_key_marker = ""
    next_version_id_marker = ""

    while True:
        item, consume_origin, consume_overlay = choose_merged_version_item(origin_item, overlay_item)
        if item is None:
            return entries, False, "", ""

        after_marker, marker_seen = version_item_after_marker(
            item,
            key_marker,
            version_id_marker,
            marker_seen,
        )
        if not after_marker:
            latest_seen_keys.add(item["Key"])
            if consume_origin:
                origin_item = next_version_item(origin_stream)
            if consume_overlay:
                overlay_item = next_version_item(overlay_stream)
            continue

        entry = list_versions_entry_for_item(item, prefix or "", delimiter)
        if entry["Type"] == "CommonPrefix":
            if entry["Name"] in seen_common_prefixes:
                if consume_origin:
                    origin_item = next_version_item(origin_stream)
                if consume_overlay:
                    overlay_item = next_version_item(overlay_stream)
                continue
            seen_common_prefixes.add(entry["Name"])
        else:
            obj = entry["Object"]
            obj["IsLatest"] = obj["Key"] not in latest_seen_keys
            latest_seen_keys.add(obj["Key"])

        if len(entries) >= max_keys:
            return entries, True, next_key_marker, next_version_id_marker

        entries.append(entry)
        next_key_marker = entry["MarkerKey"]
        next_version_id_marker = entry["MarkerVersionId"]
        if consume_origin:
            origin_item = next_version_item(origin_stream)
        if consume_overlay:
            overlay_item = next_version_item(overlay_stream)

class VersionedKeyStream:
    def __init__(
        self,
        s3_client,
        bucket,
        prefix,
        key_transform,
        before=None,
        key_marker=None,
        delimiter=None,
        skip_facilitators=False,
    ):
        self.s3_client = s3_client
        self.bucket = bucket
        self.prefix = prefix
        self.key_transform = key_transform
        self.before = before
        self.key_marker = key_marker
        self.delimiter = delimiter
        self.skip_facilitators = skip_facilitators
        self.version_id_marker = None
        self.exhausted = False
        self.buffer = []
        self.current_key = None
        self.current_candidate = None
        self.skip_key = None
        self.pending_item = None

    def next_state(self):
        while True:
            item = self._next_item()
            if item is None:
                if self.current_key is None:
                    return None
                return self._finish_current_key()

            key = item["Key"]
            if item["ItemType"] == "CommonPrefix":
                if self.current_key is None:
                    return item
                self.pending_item = item
                result = self._finish_current_key()
                if result is not None:
                    return result
                continue

            if self.skip_key is not None:
                if key == self.skip_key:
                    continue
                self.skip_key = None

            if self.current_key is None:
                self.current_key = key
                self._consider_item(item)
                continue

            if key != self.current_key:
                result = self._finish_current_key()
                self.current_key = key
                self._consider_item(item)
                if result is not None:
                    return result
                continue

            self._consider_item(item)

    def _next_item(self):
        if self.pending_item is not None:
            item = self.pending_item
            self.pending_item = None
            return item

        while not self.buffer:
            if self.current_key is not None and self.current_candidate is not None:
                return None
            if not self._fetch_next_page():
                return None
        return self.buffer.pop(0)

    def _fetch_next_page(self):
        if self.exhausted:
            return False

        request_params = {
            "Bucket": self.bucket,
            "Prefix": self.prefix,
            "MaxKeys": 1000,
        }
        if self.delimiter:
            request_params["Delimiter"] = self.delimiter
        if self.key_marker:
            request_params["KeyMarker"] = self.key_marker
            if self.version_id_marker:
                request_params["VersionIdMarker"] = self.version_id_marker

        logging.info("Fetching ListObjectVersions params: %s", request_params)
        try:
            response = self.s3_client.list_object_versions(**request_params)
        except botocore.exceptions.ClientError as exc:
            if is_invalid_list_prefix_error(exc):
                logging.info("Treating invalid ListObjectVersions prefix as empty: %s", request_params)
                self.exhausted = True
                return False
            raise
        self.key_marker = response.get("NextKeyMarker")
        self.version_id_marker = response.get("NextVersionIdMarker")
        self.exhausted = not response.get("IsTruncated", False)

        items = []
        for group, item_type in (("Versions", "Version"), ("DeleteMarkers", "DeleteMarker")):
            for item in response.get(group, []):
                original_key = item["Key"]
                key = self.key_transform(original_key)
                if key is None:
                    continue
                if (
                    self.skip_facilitators
                    and item_type == "Version"
                    and (
                        is_facilitator_version(self.s3_client, self.bucket, original_key, item)
                        or is_tag_facilitator_version(self.s3_client, self.bucket, original_key, item)
                    )
                ):
                    continue

                transformed = dict(item)
                transformed["Key"] = key
                transformed["ItemType"] = item_type
                items.append(transformed)

        for item in response.get("CommonPrefixes", []):
            original_key = item["Prefix"]
            key = self.key_transform(original_key)
            if key is None:
                continue
            items.append({
                "Key": key,
                "ItemType": "CommonPrefix",
            })

        items.sort(key=versioned_key_item_sort_key)
        self.buffer.extend(items)
        return bool(self.buffer) or not self.exhausted

    def _consider_item(self, item):
        if self.before is not None and item["LastModified"] >= self.before:
            return

        if (
            self.current_candidate is None
            or item["LastModified"] > self.current_candidate["LastModified"]
        ):
            self.current_candidate = {
                "ItemType": item["ItemType"],
                "Key": item["Key"],
                "LastModified": item["LastModified"],
                "ETag": item.get("ETag", ""),
                "Size": item.get("Size", 0),
                "StorageClass": item.get("StorageClass", "STANDARD"),
            }

    def _finish_current_key(self):
        result = self.current_candidate
        if result is not None and not self.exhausted:
            self.skip_key = self.current_key
        self.current_key = None
        self.current_candidate = None
        return result

def list_v2_source_marker(prefix, marker):
    if not marker:
        return None
    if prefix and marker < prefix:
        return None
    return marker

def versioned_key_item_sort_key(item):
    last_modified = item.get("LastModified")
    timestamp = -last_modified.timestamp() if isinstance(last_modified, datetime) else 0
    return (item["Key"], timestamp, item.get("ItemType", ""))

def next_stream_state(stream):
    return stream.next_state() if stream is not None else None

def choose_merged_state(origin_state, overlay_state):
    if origin_state is None and overlay_state is None:
        return None, False, False

    if overlay_state is not None and (
        origin_state is None or overlay_state["Key"] < origin_state["Key"]
    ):
        return overlay_state, False, True

    if origin_state is not None and (
        overlay_state is None or origin_state["Key"] < overlay_state["Key"]
    ):
        return origin_state, True, False

    return overlay_state, True, True

def list_v2_entry_for_state(state, prefix, delimiter):
    if state["ItemType"] == "CommonPrefix":
        return {
            "Type": "CommonPrefix",
            "Name": state["Key"],
            "Prefix": state["Key"],
        }

    key = state["Key"]
    suffix = key[len(prefix):]
    if delimiter is not None and delimiter and delimiter in suffix:
        common_prefix = prefix + suffix.split(delimiter, 1)[0] + delimiter
        return {
            "Type": "CommonPrefix",
            "Name": common_prefix,
            "Prefix": common_prefix,
        }

    return {
        "Type": "Contents",
        "Name": key,
        "Object": state,
    }

def collect_list_objects_v2_page(bucket, prefix, delimiter, max_keys, marker):
    if max_keys <= 0:
        return [], False, ""

    source_marker = list_v2_source_marker(prefix, marker)
    origin_stream = VersionedKeyStream(
        get_origin_s3_client(),
        bucket,
        prefix,
        lambda key: key,
        before=START_TIME,
        key_marker=source_marker,
        delimiter=delimiter,
    )

    overlay_root = f"{bucket}/"
    overlay_source_marker = f"{overlay_root}{source_marker}" if source_marker else None
    overlay_stream = VersionedKeyStream(
        get_overlay_s3_client(),
        OVERLAY_BUCKET,
        f"{overlay_root}{prefix}",
        lambda key: key[len(overlay_root):] if key.startswith(overlay_root) else None,
        key_marker=overlay_source_marker,
        delimiter=delimiter,
        skip_facilitators=True,
    )

    origin_state = next_stream_state(origin_stream)
    overlay_state = next_stream_state(overlay_stream)
    entries = []
    seen_common_prefixes = set()
    next_token = ""

    while True:
        state, consume_origin, consume_overlay = choose_merged_state(origin_state, overlay_state)
        if state is None:
            return entries, False, ""

        if state["ItemType"] not in {"Version", "CommonPrefix"}:
            if consume_origin:
                origin_state = next_stream_state(origin_stream)
            if consume_overlay:
                overlay_state = next_stream_state(overlay_stream)
            continue

        entry = list_v2_entry_for_state(state, prefix, delimiter)
        if marker and entry["Name"] <= marker:
            if consume_origin:
                origin_state = next_stream_state(origin_stream)
            if consume_overlay:
                overlay_state = next_stream_state(overlay_stream)
            continue

        if entry["Type"] == "CommonPrefix":
            if entry["Name"] in seen_common_prefixes:
                if consume_origin:
                    origin_state = next_stream_state(origin_stream)
                if consume_overlay:
                    overlay_state = next_stream_state(overlay_stream)
                continue
            seen_common_prefixes.add(entry["Name"])

        if len(entries) >= max_keys:
            return entries, True, next_token

        entries.append(entry)
        next_token = entry["Name"]
        if consume_origin:
            origin_state = next_stream_state(origin_stream)
        if consume_overlay:
            overlay_state = next_stream_state(overlay_stream)

def parse_nonnegative_int(value: Optional[str], name: str, default: int):
    if value is None:
        return default, None
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        return None, s3_error_response(
            "InvalidArgument",
            f"Argument {name} must be an integer",
            400,
        )
    if parsed < 0:
        return None, s3_error_response(
            "InvalidArgument",
            f"Argument {name} must be a non-negative integer",
            400,
        )
    return parsed, None

def validate_encoding_type(encoding_type: Optional[str]):
    if encoding_type in {None, "url"}:
        return None
    return s3_error_response(
        "InvalidArgument",
        "Encoding type must be url",
        400,
    )

def encode_list_text(value, encoding_type: Optional[str]) -> str:
    text = "" if value is None else str(value)
    if encoding_type == "url":
        return quote(text, safe="/")
    return text

def append_list_contents_xml(root: ET.Element, obj: dict, encoding_type: Optional[str]):
    cont_elem = ET.SubElement(root, "Contents")
    ET.SubElement(cont_elem, "Key").text = encode_list_text(obj["Key"], encoding_type)
    last_modified = obj.get("LastModified")
    ET.SubElement(cont_elem, "LastModified").text = (
        last_modified.isoformat() if isinstance(last_modified, datetime) else str(last_modified)
    )
    ET.SubElement(cont_elem, "ETag").text = obj.get("ETag", "")
    ET.SubElement(cont_elem, "Size").text = str(obj.get("Size", 0))
    ET.SubElement(cont_elem, "StorageClass").text = obj.get("StorageClass", "STANDARD")

def append_common_prefix_xml(root: ET.Element, prefix: str, encoding_type: Optional[str]):
    cp_elem = ET.SubElement(root, "CommonPrefixes")
    ET.SubElement(cp_elem, "Prefix").text = encode_list_text(prefix, encoding_type)

def process_list_objects_v1(bucket, prefix, delimiter, marker, max_keys, encoding_type):
    entries, is_truncated, next_marker = collect_list_objects_v2_page(
        bucket,
        prefix,
        delimiter,
        max_keys,
        marker,
    )

    root = ET.Element("ListBucketResult")
    ET.SubElement(root, "Name").text = bucket
    ET.SubElement(root, "Prefix").text = prefix
    ET.SubElement(root, "Marker").text = marker or ""
    ET.SubElement(root, "MaxKeys").text = str(max_keys)
    if delimiter:
        ET.SubElement(root, "Delimiter").text = delimiter
    if encoding_type:
        ET.SubElement(root, "EncodingType").text = encoding_type
    ET.SubElement(root, "IsTruncated").text = "true" if is_truncated else "false"
    if is_truncated:
        ET.SubElement(root, "NextMarker").text = next_marker

    for entry in entries:
        if entry["Type"] == "CommonPrefix":
            append_common_prefix_xml(root, entry["Prefix"], encoding_type)
            continue
        append_list_contents_xml(root, entry["Object"], encoding_type)

    return ET.tostring(root, encoding="utf-8", method="xml")

# Handle the versions logic
def process_list_versions(bucket, prefix, delimiter, key_marker, version_id_marker, max_keys):
    """
    Process ListObjectVersions request by streaming merged origin and overlay entries.
    """
    entries, is_truncated, next_key_marker, next_version_id_marker = (
        collect_list_object_versions_page(
            bucket,
            prefix or "",
            delimiter,
            max_keys,
            key_marker,
            version_id_marker,
        )
    )

    root = ET.Element("ListVersionsResult")
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

    for entry in entries:
        if entry["Type"] == "CommonPrefix":
            cp_elem = ET.SubElement(root, "CommonPrefixes")
            ET.SubElement(cp_elem, "Prefix").text = entry["Prefix"]
            continue

        item = entry["Object"]
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

def upload_common_prefix(key: str, prefix: str, delimiter: Optional[str]) -> Optional[str]:
    if not delimiter:
        return None
    remainder = key[len(prefix):]
    if delimiter not in remainder:
        return None
    return f"{prefix}{remainder.split(delimiter, 1)[0]}{delimiter}"

def append_principal_xml(parent, tag, principal):
    elem = ET.SubElement(parent, tag)
    ET.SubElement(elem, "ID").text = principal.get("ID", "")
    ET.SubElement(elem, "DisplayName").text = principal.get("DisplayName", "")

def append_upload_xml(root, upload):
    upload_elem = ET.SubElement(root, "Upload")
    ET.SubElement(upload_elem, "Key").text = upload["Key"]
    ET.SubElement(upload_elem, "UploadId").text = upload["UploadId"]
    append_principal_xml(upload_elem, "Initiator", upload.get("Initiator", {}))
    append_principal_xml(upload_elem, "Owner", upload.get("Owner", {}))
    ET.SubElement(upload_elem, "StorageClass").text = upload.get("StorageClass", "STANDARD")
    initiated = upload.get("Initiated")
    if isinstance(initiated, datetime):
        initiated_text = initiated.isoformat()
    else:
        initiated_text = str(initiated or "")
    ET.SubElement(upload_elem, "Initiated").text = initiated_text

def collect_list_multipart_uploads_page(bucket, prefix, delimiter, max_uploads, key_marker, upload_id_marker):
    s3_client_overlay = get_overlay_s3_client()
    hidden_bucket_prefix = f"{bucket}/"
    hidden_prefix = f"{hidden_bucket_prefix}{prefix}"
    overlay_key_marker = f"{hidden_bucket_prefix}{key_marker}" if key_marker else None
    marker = {}
    if overlay_key_marker:
        marker["KeyMarker"] = overlay_key_marker
        if upload_id_marker:
            marker["UploadIdMarker"] = upload_id_marker

    uploads = []
    common_prefixes = []
    seen_common_prefixes = set()
    next_key_marker = ""
    next_upload_id_marker = ""

    while True:
        params = {"Bucket": OVERLAY_BUCKET, "MaxUploads": 1000}
        params.update(marker)
        response = s3_client_overlay.list_multipart_uploads(**params)
        stop_scan = False

        for upload in response.get("Uploads", []):
            overlay_key = upload["Key"]
            if not overlay_key.startswith(hidden_prefix):
                if overlay_key > hidden_prefix and overlay_key.startswith(hidden_bucket_prefix):
                    stop_scan = True
                    break
                if overlay_key > hidden_bucket_prefix and not overlay_key.startswith(hidden_bucket_prefix):
                    return uploads, common_prefixes, False, "", ""
                continue

            virtual_key = overlay_key[len(hidden_bucket_prefix):]
            if key_marker:
                if virtual_key < key_marker:
                    continue
                if virtual_key == key_marker:
                    if not upload_id_marker or upload["UploadId"] <= upload_id_marker:
                        continue

            common_prefix = upload_common_prefix(virtual_key, prefix, delimiter)
            if common_prefix:
                if common_prefix not in seen_common_prefixes:
                    seen_common_prefixes.add(common_prefix)
                    common_prefixes.append(common_prefix)
                continue

            if len(uploads) >= max_uploads:
                return uploads, common_prefixes, True, next_key_marker, next_upload_id_marker

            rewritten_upload = upload.copy()
            rewritten_upload["Key"] = virtual_key
            uploads.append(rewritten_upload)
            next_key_marker = virtual_key
            next_upload_id_marker = upload["UploadId"]

        if stop_scan:
            return uploads, common_prefixes, False, "", ""

        if not response.get("IsTruncated"):
            return uploads, common_prefixes, False, "", ""

        marker = {}
        if response.get("NextKeyMarker"):
            marker["KeyMarker"] = response["NextKeyMarker"]
        if response.get("NextUploadIdMarker"):
            marker["UploadIdMarker"] = response["NextUploadIdMarker"]

def process_list_multipart_uploads(bucket, prefix, delimiter, key_marker, upload_id_marker, max_uploads):
    uploads, common_prefixes, is_truncated, next_key_marker, next_upload_id_marker = (
        collect_list_multipart_uploads_page(
            bucket,
            prefix or "",
            delimiter,
            max_uploads,
            key_marker,
            upload_id_marker,
        )
    )

    root = ET.Element("ListMultipartUploadsResult")
    ET.SubElement(root, "Bucket").text = bucket
    ET.SubElement(root, "KeyMarker").text = key_marker or ""
    ET.SubElement(root, "UploadIdMarker").text = upload_id_marker or ""
    if is_truncated:
        ET.SubElement(root, "NextKeyMarker").text = next_key_marker
        ET.SubElement(root, "NextUploadIdMarker").text = next_upload_id_marker
    ET.SubElement(root, "Prefix").text = prefix or ""
    if delimiter:
        ET.SubElement(root, "Delimiter").text = delimiter
    ET.SubElement(root, "MaxUploads").text = str(max_uploads)
    ET.SubElement(root, "IsTruncated").text = "true" if is_truncated else "false"

    for upload in uploads:
        append_upload_xml(root, upload)

    for common_prefix in common_prefixes:
        cp_elem = ET.SubElement(root, "CommonPrefixes")
        ET.SubElement(cp_elem, "Prefix").text = common_prefix

    return ET.tostring(root, encoding="utf-8", method="xml")

@app.get("/{bucket}")
@app.get("/{bucket}/")
async def list_objects_handler(bucket: str, request: Request, prefix: str = ""):
    """
    Dispatch S3 list requests based on query parameters.

    - If query parameter list-type=2 is present, process as ListObjectsV2.
    - If query parameter versions is present, process as ListObjectVersions.
    - If query parameter uploads is present, process as ListMultipartUploads.
    - Otherwise, process as legacy ListObjects.
    """
    params = request.query_params
    list_type = params.get("list-type")
    versions = params.get("versions")
    delimiter = params.get("delimiter")
    encoding_type = params.get("encoding-type")

    encoding_error = validate_encoding_type(encoding_type)
    if encoding_error:
        return encoding_error

    if query_has_param(request.url.query, "tagging"):
        return not_implemented_response("Bucket tagging is not implemented")

    if query_has_param(request.url.query, "uploads"):
        list_uploads_prefix = params.get("prefix", prefix or "")
        max_uploads = int(params.get("max-uploads", "1000"))
        key_marker = params.get("key-marker")
        upload_id_marker = params.get("upload-id-marker")
        xml_response = await run_sync_s3(
            process_list_multipart_uploads,
            bucket=bucket,
            prefix=list_uploads_prefix,
            delimiter=delimiter,
            key_marker=key_marker,
            upload_id_marker=upload_id_marker,
            max_uploads=max_uploads,
        )
        return Response(content=xml_response, media_type="application/xml")

    if list_type == "2":
        # ----- ListObjectsV2 logic -----
        prefix = params.get("prefix", prefix or "")
        max_keys, max_keys_error = parse_nonnegative_int(params.get("max-keys"), "max-keys", 1000)
        if max_keys_error:
            return max_keys_error
        continuation_token = params.get("continuation-token")
        start_after = params.get("start-after")
        marker = continuation_token or start_after
        paginated, is_truncated, next_token = await run_sync_s3(
            collect_list_objects_v2_page,
            bucket,
            prefix,
            delimiter,
            max_keys,
            marker,
        )

        root = ET.Element("ListBucketResult")
        name_elem = ET.SubElement(root, "Name")
        name_elem.text = bucket
        prefix_elem = ET.SubElement(root, "Prefix")
        prefix_elem.text = prefix
        keycount_elem = ET.SubElement(root, "KeyCount")
        keycount_elem.text = str(len(paginated))
        maxkeys_elem = ET.SubElement(root, "MaxKeys")
        maxkeys_elem.text = str(max_keys)
        if delimiter is not None:
            delimiter_elem = ET.SubElement(root, "Delimiter")
            delimiter_elem.text = delimiter
        if encoding_type:
            ET.SubElement(root, "EncodingType").text = encoding_type
        trunc_elem = ET.SubElement(root, "IsTruncated")
        trunc_elem.text = "true" if is_truncated else "false"
        
        if start_after:
            start_after_elem = ET.SubElement(root, "StartAfter")
            start_after_elem.text = start_after

        if continuation_token:
            token_elem = ET.SubElement(root, "ContinuationToken")
            token_elem.text = continuation_token
        
        if is_truncated:
            next_token_elem = ET.SubElement(root, "NextContinuationToken")
            next_token_elem.text = next_token
        
        for entry in paginated:
            if entry["Type"] == "CommonPrefix":
                append_common_prefix_xml(root, entry["Prefix"], encoding_type)
                continue

            append_list_contents_xml(root, entry["Object"], encoding_type)
        
        xml_response = ET.tostring(root, encoding="utf-8", method="xml")
        return Response(content=xml_response, media_type="application/xml")

    elif versions is not None:
        # ----- ListObjectVersions logic (improved) -----
        max_keys, max_keys_error = parse_nonnegative_int(params.get("max-keys"), "max-keys", 1000)
        if max_keys_error:
            return max_keys_error
        key_marker = params.get("key-marker")
        version_id_marker = params.get("version-id-marker")
        
        xml_response = await run_sync_s3(
            process_list_versions,
            bucket=bucket,
            prefix=prefix,
            delimiter=delimiter,
            key_marker=key_marker,
            version_id_marker=version_id_marker,
            max_keys=max_keys
        )
        
        return Response(content=xml_response, media_type="application/xml")
    
    else:
        # ----- Legacy ListObjects logic -----
        prefix = params.get("prefix", prefix or "")
        marker = params.get("marker")
        max_keys, max_keys_error = parse_nonnegative_int(params.get("max-keys"), "max-keys", 1000)
        if max_keys_error:
            return max_keys_error
        xml_response = await run_sync_s3(
            process_list_objects_v1,
            bucket=bucket,
            prefix=prefix,
            delimiter=delimiter,
            marker=marker,
            max_keys=max_keys,
            encoding_type=encoding_type,
        )
        return Response(content=xml_response, media_type="application/xml")

async def handle_conditional_mutation(
    method: str,
    full_path: str,
    query_string: str,
    original_headers: dict,
    body: bytes,
    response: httpx.Response,
) -> httpx.Response:
    """
    Handle conditional write (PUT) and delete (DELETE) requests that fail with 412 Precondition Failed.
    
    If the overlay returns 412 or a conditional PUT misses the overlay object:
    1. Check if the condition would be satisfied against the origin object as of START_TIME
    2. If so, retry the request against overlay with modified conditions
    """
    if method not in {"PUT", "DELETE"}:
        return response

    if_match = get_header(original_headers, "if-match")
    if_none_match = get_header(original_headers, "if-none-match")
    conditional_overlay_miss = method == "PUT" and response.status_code in {400, 404} and if_match
    if response.status_code != 412 and not conditional_overlay_miss:
        return response
        
    bucket, key = split_bucket_key(full_path)
    
    logging.info("Conditional mutation failed with 412. Checking if condition can be satisfied via origin.")
    
    # Check if object exists in overlay first (to avoid race conditions)
    try:
        overlay_path = f"{bucket}/{key}"
        await run_sync_s3(head_overlay_object, overlay_path)
        # If we get here, object exists in overlay - respect the 412 from overlay
        logging.info("Object exists in overlay. Respecting 412 Precondition Failed.")
        return response
    except Exception:
        # Object doesn't exist in overlay, check origin
        pass
    
    try:
        origin_obj = await run_sync_s3(check_object_at_start_time, bucket, key)

        if origin_obj is None:
            logging.info("No version of object existed at START_TIME, original 412 response is correct")
            if response.status_code != 412 and if_match and if_match.strip() != "*":
                return precondition_failed_response()
            return response
        
        # Now check conditions against this point-in-time correct version
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
            overlay_url = append_query(f"{OVERLAY_S3_URL}/{quote(overlay_path)}", query_string)
            
            new_response = await forward_s3_request(
                signed_client,
                method, overlay_url, headers=modified_headers, content=body
            )
            
            logging.info("Conditional retry status: %s", new_response.status_code)
            return new_response

        if response.status_code != 412:
            return precondition_failed_response()
        
    except Exception as e:
        # Object doesn't exist in origin or other error
        logging.info("Error checking origin object: %s", str(e))
        if response.status_code != 412 and if_match and if_match.strip() != "*":
            return precondition_failed_response()
    
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
    bucket, key = split_bucket_key(full_path)
    overlay_path = f"{bucket}/{key}"
    
    # First check if object exists in overlay
    try:
        await run_sync_s3(head_overlay_object, overlay_path)
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
                origin_obj = await run_sync_s3(check_object_at_start_time, bucket, key)
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

async def prepare_streaming_put_conditions(
    method: str,
    full_path: str,
    query_string: str,
    original_headers: dict,
    overlay_headers: dict,
) -> tuple[Optional[Response], dict]:
    if method != "PUT" or is_multipart_upload_subresource(query_string):
        return None, overlay_headers

    if_none_match = get_header(original_headers, "if-none-match")
    if if_none_match:
        if if_none_match != "*":
            logging.info(f"Unsupported If-None-Match value for PUT: {if_none_match}")
            return Response(
                content=b"<Error><Code>NotImplemented</Code><Message>The If-None-Match header is only supported with value * for PUT operations</Message></Error>",
                status_code=501,
                headers={"Content-Type": "application/xml"}
            ), overlay_headers

        special_response = await handle_if_none_match_star_put(full_path, original_headers)
        if special_response:
            return special_response, overlay_headers

    if_match = get_header(original_headers, "if-match")
    if not if_match:
        return None, overlay_headers

    bucket, key = split_bucket_key(full_path)
    overlay_path = f"{bucket}/{key}"

    try:
        await run_sync_s3(head_overlay_object, overlay_path)
        return None, overlay_headers
    except botocore.exceptions.ClientError as exc:
        error_code = exc.response.get("Error", {}).get("Code")
        status_code = exc.response.get("ResponseMetadata", {}).get("HTTPStatusCode")
        if status_code != 404 and error_code not in {"404", "NoSuchKey", "NotFound"}:
            return Response(
                content=str(exc).encode("utf-8"),
                status_code=status_code or 500,
                headers={"Content-Type": "text/plain"},
            ), overlay_headers

    origin_obj = await run_sync_s3(check_object_at_start_time, bucket, key)
    if origin_obj is None:
        etags = [tag.strip(' "') for tag in if_match.split(",")]
        if "*" in etags:
            return s3_error_response("NoSuchKey", "The specified key does not exist.", 404), overlay_headers
        return response_from_httpx(precondition_failed_response()), overlay_headers

    etags = [tag.strip(' "') for tag in if_match.split(",")]
    origin_etag = origin_obj.get("ETag", "").strip('"')
    if origin_etag not in etags and "*" not in etags:
        logging.info(f"If-Match condition not satisfied: {if_match} vs origin ETag {origin_etag}")
        return response_from_httpx(precondition_failed_response()), overlay_headers

    modified_headers = {
        k: v for k, v in overlay_headers.items()
        if k.lower() not in {"if-match", "if-none-match"}
    }
    modified_headers["If-None-Match"] = "*"
    return None, modified_headers

@app.api_route("/{full_path:path}", methods=["GET", "PUT", "DELETE", "HEAD", "POST"])
async def proxy(full_path: str, request: Request):
    method = request.method
    query_string = request.url.query
    original_headers = dict(request.headers)
    multipart_subresource = is_multipart_upload_subresource(query_string)
    logging.info("Received %s request for %s", method, full_path)

    if method == "POST" and query_has_param(query_string, "delete"):
        try:
            body = await read_control_body(request)
        except ControlBodyTooLarge:
            return s3_error_response(
                "EntityTooLarge",
                "Control-plane request body exceeds the proxy limit",
                413,
            )
        return await handle_multi_object_delete_request(full_path, body)

    if query_has_param(query_string, "tagging"):
        try:
            body = await read_control_body(request)
        except ControlBodyTooLarge:
            return s3_error_response(
                "EntityTooLarge",
                "Control-plane request body exceeds the proxy limit",
                413,
            )
        return await handle_object_tagging_request(
            method,
            full_path,
            query_string,
            original_headers,
            body,
        )

    # Use filtered headers for overlay S3 request.
    overlay_headers = prepare_overlay_headers(
        original_headers,
        preserve_content_length=method == "PUT",
    )
    overlay_path = rewrite_overlay_path(full_path)
    overlay_url = append_query(f"{OVERLAY_S3_URL}/{quote(overlay_path)}", query_string)

    if has_header(original_headers, "x-amz-copy-source"):
        if multipart_subresource:
            return not_implemented_response("UploadPartCopy is not implemented")
        if method != "PUT":
            return s3_error_response("InvalidRequest", "CopyObject requires PUT.", 400)
        return await handle_copy_object_request(full_path, query_string, original_headers, overlay_url)

    if method in {"GET", "HEAD"} and not multipart_subresource:
        response = await open_get_head_response(
            method,
            full_path,
            query_string,
            original_headers,
            overlay_url,
            overlay_headers,
        )
        return streaming_response_from_httpx(response)

    conditional_response, overlay_headers = await prepare_streaming_put_conditions(
        method,
        full_path,
        query_string,
        original_headers,
        overlay_headers,
    )
    if conditional_response:
        return await finalize_early_body_response(request, conditional_response)

    overlay_body = None
    if method == "PUT":
        overlay_body, body_error = prepare_overlay_body_stream(
            original_headers,
            overlay_headers,
            request_body_stream(request),
        )
        if body_error:
            return await finalize_early_body_response(request, body_error)

    if method == "PUT" and not has_header(overlay_headers, "content-length"):
        return await finalize_early_body_response(request, s3_error_response(
            "MissingContentLength",
            "You must provide the Content-Length HTTP header.",
            411,
        ))

    if method == "PUT" and multipart_subresource and query_has_param(query_string, "uploadId"):
        logging.info("Sending streaming overlay request: %s %s", method, overlay_url)
        try:
            response = await open_s3_stream(
                signed_client,
                method,
                overlay_url,
                headers=overlay_headers,
                content=overlay_body,
            )
        except AWSChunkedDecodeError as exc:
            return s3_error_response("InvalidRequest", str(exc), 400)
        logging.info("Overlay response status: %s, headers: %s", response.status_code, dict(response.headers))
        return streaming_response_from_httpx(response)

    if multipart_subresource:
        try:
            body = await read_control_body(request)
        except ControlBodyTooLarge:
            return s3_error_response(
                "EntityTooLarge",
                "Control-plane request body exceeds the proxy limit",
                413,
            )

        if method == "POST" and query_has_param(query_string, "uploadId"):
            body, stripped_checksums = strip_complete_multipart_upload_checksums(body)
            if stripped_checksums:
                remove_body_integrity_headers(overlay_headers)
                logging.info("Removed CompleteMultipartUpload checksum elements before overlay forwarding")

        logging.info("Sending overlay request: %s %s", method, overlay_url)
        response = await forward_s3_request(signed_client, method, overlay_url, headers=overlay_headers, content=body)
        logging.info("Overlay response status: %s, headers: %s", response.status_code, dict(response.headers))
        response = rewrite_multipart_xml_response(response, full_path, request)
        return response_from_httpx(response)
    
    # Forward request to overlay
    if (
        method == "DELETE"
        and not query_has_param(query_string, "tagging")
        and not query_has_param(query_string, "versionId")
    ):
        response = await handle_delete_request(overlay_url, overlay_headers, b"")
        return response_from_httpx(response)
    else:
        content = overlay_body if method == "PUT" else request_body_stream(request) if method == "POST" else b""
        logging.info("Sending streaming overlay request: %s %s", method, overlay_url)
        try:
            response = await open_s3_stream(
                signed_client,
                method,
                overlay_url,
                headers=overlay_headers,
                content=content,
            )
        except AWSChunkedDecodeError as exc:
            return s3_error_response("InvalidRequest", str(exc), 400)
        logging.info("Overlay response status: %s, headers: %s", response.status_code, dict(response.headers))
        return streaming_response_from_httpx(response)
