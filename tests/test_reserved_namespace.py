import asyncio
import os
import sys
import xml.etree.ElementTree as ET
from pathlib import Path

import pytest
from starlette.requests import Request

os.environ.setdefault("AWS_ACCESS_KEY_ID", "origin-access")
os.environ.setdefault("AWS_SECRET_ACCESS_KEY", "origin-secret")
os.environ.setdefault("OVERLAY_AWS_ACCESS_KEY_ID", "overlay-access")
os.environ.setdefault("OVERLAY_AWS_SECRET_ACCESS_KEY", "overlay-secret")
os.environ.setdefault("START_TIME", "2024-01-10T00:00:00Z")
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app import main, snapshot_time


def request_for(method: str, full_path: str, body: bytes = b"") -> Request:
    delivered = False

    async def receive():
        nonlocal delivered
        if delivered:
            return {"type": "http.disconnect"}
        delivered = True
        return {"type": "http.request", "body": body, "more_body": False}

    return Request(
        {
            "type": "http",
            "asgi": {"version": "3.0"},
            "http_version": "1.1",
            "method": method,
            "scheme": "http",
            "path": f"/{full_path}",
            "raw_path": f"/{full_path}".encode(),
            "query_string": b"",
            "headers": [(b"content-length", str(len(body)).encode())],
            "client": ("test", 1234),
            "server": ("proxy", 9000),
            "root_path": "",
        },
        receive,
    )


def response_error(response):
    root = ET.fromstring(response.body)
    return {
        child.tag.rsplit("}", 1)[-1]: child.text
        for child in root
    }


@pytest.mark.parametrize("method", ["PUT", "POST", "DELETE"])
def test_direct_reserved_namespace_mutations_are_denied(method):
    full_path = snapshot_time.SNAPSHOT_TIME_OBJECT_KEY
    request = request_for(method, full_path, b"replacement")

    response = asyncio.run(main.proxy(full_path, request))

    assert response.status_code == 403
    assert response_error(response)["Code"] == "AccessDenied"


def test_reserved_namespace_check_uses_the_overlay_key():
    assert main.request_mutates_reserved_namespace(
        "PUT",
        snapshot_time.SNAPSHOT_TIME_OBJECT_KEY,
    )
    assert not main.request_mutates_reserved_namespace(
        "GET",
        snapshot_time.SNAPSHOT_TIME_OBJECT_KEY,
    )
    assert not main.request_mutates_reserved_namespace(
        "PUT",
        f"ordinary-bucket/{snapshot_time.SNAPSHOT_TIME_OBJECT_KEY}",
    )


def test_multi_object_delete_denies_reserved_entries(monkeypatch):
    class NoMutationClient:
        def put_object(self, **kwargs):
            raise AssertionError(f"unexpected put_object: {kwargs}")

        def delete_objects(self, **kwargs):
            raise AssertionError(f"unexpected delete_objects: {kwargs}")

        def list_object_versions(self, **kwargs):
            raise AssertionError(f"unexpected list_object_versions: {kwargs}")

    monkeypatch.setattr(main, "get_overlay_s3_client", NoMutationClient)
    monkeypatch.setattr(main, "get_origin_s3_client", NoMutationClient)
    body = b"""
    <Delete>
      <Object><Key>snapshot-time</Key></Object>
      <Object><Key>future-control-object</Key><VersionId>version-1</VersionId></Object>
    </Delete>
    """

    response = main._handle_multi_object_delete_request_sync(
        snapshot_time.RESERVED_NAMESPACE.rstrip("/"),
        body,
    )

    assert response.status_code == 200
    root = ET.fromstring(response.body)
    errors = [
        {
            child.tag.rsplit("}", 1)[-1]: child.text
            for child in item
        }
        for item in root
        if item.tag.rsplit("}", 1)[-1] == "Error"
    ]
    assert errors == [
        {
            "Key": "snapshot-time",
            "Code": "AccessDenied",
            "Message": main.RESERVED_NAMESPACE_MUTATION_MESSAGE,
        },
        {
            "Key": "future-control-object",
            "VersionId": "version-1",
            "Code": "AccessDenied",
            "Message": main.RESERVED_NAMESPACE_MUTATION_MESSAGE,
        },
    ]
