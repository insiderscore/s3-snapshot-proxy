import os
import re
import sys
from pathlib import Path

import httpx

os.environ.setdefault("AWS_ACCESS_KEY_ID", "origin-access")
os.environ.setdefault("AWS_SECRET_ACCESS_KEY", "origin-secret")
os.environ.setdefault("OVERLAY_AWS_ACCESS_KEY_ID", "overlay-access")
os.environ.setdefault("OVERLAY_AWS_SECRET_ACCESS_KEY", "overlay-secret")
os.environ.setdefault("START_TIME", "2024-01-10T00:00:00Z")
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app import main


def signed_headers_for(request: httpx.Request) -> set[str]:
    signed_request = next(main.overlay_aws_auth.auth_flow(request))
    authorization = signed_request.headers["Authorization"]
    match = re.search(r"SignedHeaders=([^,]+)", authorization)
    assert match is not None
    return set(match.group(1).split(";"))


def test_overlay_signer_signs_forwarded_content_md5():
    request = httpx.Request(
        "PUT",
        "https://s3.us-east-1.amazonaws.com/overlay/bucket/key",
        headers={
            "Content-MD5": "XrY7u+Ae7tCTyyK7j1rNww==",
            "Expect": "100-continue",
        },
        content=b"hello",
    )

    assert "content-md5" in signed_headers_for(request)

