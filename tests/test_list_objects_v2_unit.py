import os
import sys
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta, timezone
from pathlib import Path

import botocore.exceptions

os.environ.setdefault("AWS_ACCESS_KEY_ID", "origin-access")
os.environ.setdefault("AWS_SECRET_ACCESS_KEY", "origin-secret")
os.environ.setdefault("OVERLAY_AWS_ACCESS_KEY_ID", "overlay-access")
os.environ.setdefault("OVERLAY_AWS_SECRET_ACCESS_KEY", "overlay-secret")
os.environ.setdefault("START_TIME", "2024-01-10T00:00:00Z")
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app import main


BASE_TIME = datetime(2024, 1, 1, tzinfo=timezone.utc)


class FakeVersionClient:
    def __init__(self, pages, heads=None):
        self.pages = list(pages)
        self.heads = heads or {}
        self.calls = []
        self.head_calls = []

    def list_object_versions(self, **params):
        self.calls.append(dict(params))
        if len(self.calls) > len(self.pages):
            raise AssertionError(f"unexpected extra page fetch: {params}")
        page = self.pages[len(self.calls) - 1]
        if isinstance(page, Exception):
            raise page
        return page

    def head_object(self, **params):
        self.head_calls.append(dict(params))
        key = (params["Key"], params.get("VersionId"))
        if key not in self.heads:
            raise AssertionError(f"unexpected head_object call: {params}")
        return self.heads[key]


def version(key, seconds, size=1, version_id=None, etag=None):
    return {
        "Key": key,
        "LastModified": BASE_TIME + timedelta(seconds=seconds),
        "VersionId": version_id or f"version-{key}-{seconds}",
        "ETag": etag or f'"etag-{key}-{seconds}"',
        "Size": size,
        "StorageClass": "STANDARD",
    }


def delete_marker(key, seconds, version_id=None):
    return {
        "Key": key,
        "VersionId": version_id or f"delete-{key}-{seconds}",
        "LastModified": BASE_TIME + timedelta(seconds=seconds),
    }


def facilitator_version(key, seconds, version_id="facilitator-version"):
    return version(
        key,
        seconds,
        size=len(main.DELETE_MARKER_FACILITATOR_BODY),
        version_id=version_id,
        etag=main.DELETE_MARKER_FACILITATOR_ETAG,
    )


def facilitator_head():
    return {
        "Metadata": {
            main.DELETE_MARKER_FACILITATOR_METADATA: "true",
        },
        "ResponseMetadata": {"HTTPHeaders": {}},
    }


def tag_facilitator_version(key, seconds, version_id="tag-facilitator-version"):
    return version(
        key,
        seconds,
        size=len(main.TAG_FACILITATOR_BODY),
        version_id=version_id,
        etag=main.TAG_FACILITATOR_ETAG,
    )


def tag_facilitator_head():
    return {
        "Metadata": {
            main.TAG_FACILITATOR_METADATA: "true",
        },
        "ResponseMetadata": {"HTTPHeaders": {}},
    }


def invalid_object_name_error():
    return botocore.exceptions.ClientError(
        {
            "Error": {
                "Code": "XMinioInvalidObjectName",
                "Message": "Object name contains unsupported characters.",
            },
            "ResponseMetadata": {"HTTPStatusCode": 400},
        },
        "ListObjectVersions",
    )


def wire_clients(monkeypatch, origin_client, overlay_client):
    monkeypatch.setattr(main, "START_TIME", BASE_TIME + timedelta(days=10))
    monkeypatch.setattr(main, "OVERLAY_BUCKET", "overlay")
    monkeypatch.setattr(main, "get_origin_s3_client", lambda: origin_client)
    monkeypatch.setattr(main, "get_overlay_s3_client", lambda: overlay_client)


def test_rewrite_overlay_path_preserves_object_key_slashes(monkeypatch):
    monkeypatch.setattr(main, "OVERLAY_BUCKET", "overlay")

    assert main.split_bucket_key("bucket/asdf/") == ("bucket", "asdf/")
    assert main.split_bucket_key("/bucket//leading") == ("bucket", "/leading")
    assert main.rewrite_overlay_path("bucket/asdf/") == "overlay/bucket/asdf/"


def test_overlay_header_forwarding_allows_only_sse_s3():
    assert main.should_forward_overlay_header("x-amz-tagging")
    assert main.should_forward_overlay_header("x-amz-server-side-encryption")
    assert not main.should_forward_overlay_header("x-amz-server-side-encryption-aws-kms-key-id")
    assert not main.should_forward_overlay_header("x-amz-server-side-encryption-customer-key")


def test_list_v2_max_keys_zero_avoids_s3_calls(monkeypatch):
    origin_client = FakeVersionClient([{"Versions": [version("a", 1)]}])
    overlay_client = FakeVersionClient([{"Versions": [version("bucket/b", 1)]}])
    wire_clients(monkeypatch, origin_client, overlay_client)

    entries, is_truncated, next_token = main.collect_list_objects_v2_page(
        "bucket", "", None, 0, None
    )

    assert entries == []
    assert is_truncated is False
    assert next_token == ""
    assert origin_client.calls == []
    assert overlay_client.calls == []


def test_list_v2_small_page_uses_first_origin_page_only(monkeypatch):
    origin_client = FakeVersionClient([
        {
            "Versions": [version("a", 1), version("b", 1)],
            "IsTruncated": True,
            "NextKeyMarker": "b",
        }
    ])
    overlay_client = FakeVersionClient([{"IsTruncated": False}])
    wire_clients(monkeypatch, origin_client, overlay_client)

    entries, is_truncated, next_token = main.collect_list_objects_v2_page(
        "bucket", "", None, 1, None
    )

    assert [entry["Name"] for entry in entries] == ["a"]
    assert is_truncated is True
    assert next_token == "a"
    assert len(origin_client.calls) == 1
    assert len(overlay_client.calls) == 1


def test_list_v2_start_after_is_pushed_to_origin_and_overlay(monkeypatch):
    origin_client = FakeVersionClient([
        {"Versions": [version("prefix/item-10", 1), version("prefix/item-11", 1)]}
    ])
    overlay_client = FakeVersionClient([
        {"Versions": [version("bucket/prefix/item-12", 1)]}
    ])
    wire_clients(monkeypatch, origin_client, overlay_client)

    entries, is_truncated, next_token = main.collect_list_objects_v2_page(
        "bucket", "prefix/", None, 2, "prefix/item-09"
    )

    assert [entry["Name"] for entry in entries] == ["prefix/item-10", "prefix/item-11"]
    assert is_truncated is True
    assert next_token == "prefix/item-11"
    assert origin_client.calls[0]["KeyMarker"] == "prefix/item-09"
    assert overlay_client.calls[0]["KeyMarker"] == "bucket/prefix/item-09"


def test_list_v2_overlay_delete_marker_hides_origin_key(monkeypatch):
    origin_client = FakeVersionClient([
        {"Versions": [version("a", 1), version("b", 1)]}
    ])
    overlay_client = FakeVersionClient([
        {
            "Versions": [version("bucket/c", 1)],
            "DeleteMarkers": [delete_marker("bucket/a", 2)],
        }
    ])
    wire_clients(monkeypatch, origin_client, overlay_client)

    entries, is_truncated, next_token = main.collect_list_objects_v2_page(
        "bucket", "", None, 10, None
    )

    assert [entry["Name"] for entry in entries] == ["b", "c"]
    assert is_truncated is False
    assert next_token == ""


def test_list_v2_ignores_overlay_delete_marker_facilitator(monkeypatch):
    origin_client = FakeVersionClient([
        {"Versions": [version("a", 1)]}
    ])
    overlay_client = FakeVersionClient(
        [
            {
                "Versions": [facilitator_version("bucket/a", 3)],
                "DeleteMarkers": [delete_marker("bucket/a", 2)],
            }
        ],
        heads={("bucket/a", "facilitator-version"): facilitator_head()},
    )
    wire_clients(monkeypatch, origin_client, overlay_client)

    entries, is_truncated, next_token = main.collect_list_objects_v2_page(
        "bucket", "", None, 10, None
    )

    assert entries == []
    assert is_truncated is False
    assert next_token == ""
    assert len(overlay_client.head_calls) == 1


def test_list_v2_ignores_overlay_tag_facilitator(monkeypatch):
    origin_client = FakeVersionClient([
        {"Versions": [version("a", 1)]}
    ])
    overlay_client = FakeVersionClient(
        [
            {
                "Versions": [tag_facilitator_version("bucket/a", 3)],
            }
        ],
        heads={("bucket/a", "tag-facilitator-version"): tag_facilitator_head()},
    )
    wire_clients(monkeypatch, origin_client, overlay_client)

    entries, is_truncated, next_token = main.collect_list_objects_v2_page(
        "bucket", "", None, 10, None
    )

    assert [(entry["Type"], entry["Name"]) for entry in entries] == [("Contents", "a")]
    assert entries[0]["Object"]["ETag"] == '"etag-a-1"'
    assert is_truncated is False
    assert next_token == ""
    assert len(overlay_client.head_calls) == 1


def test_list_v2_delimiter_coalesces_common_prefixes(monkeypatch):
    origin_client = FakeVersionClient([
        {"Versions": [version("dir/a", 1), version("dir/b", 1), version("z", 1)]}
    ])
    overlay_client = FakeVersionClient([{"IsTruncated": False}])
    wire_clients(monkeypatch, origin_client, overlay_client)

    entries, is_truncated, next_token = main.collect_list_objects_v2_page(
        "bucket", "", "/", 10, None
    )

    assert [(entry["Type"], entry["Name"]) for entry in entries] == [
        ("CommonPrefix", "dir/"),
        ("Contents", "z"),
    ]
    assert is_truncated is False
    assert next_token == ""


def test_list_v2_treats_invalid_overlay_prefix_as_empty(monkeypatch):
    origin_client = FakeVersionClient([{"Versions": []}])
    overlay_client = FakeVersionClient([invalid_object_name_error()])
    wire_clients(monkeypatch, origin_client, overlay_client)

    entries, is_truncated, next_token = main.collect_list_objects_v2_page(
        "bucket", "/", None, 10, None
    )

    assert entries == []
    assert is_truncated is False
    assert next_token == ""
    assert overlay_client.calls[0]["Prefix"] == "bucket//"


def test_list_v1_renders_marker_and_next_marker(monkeypatch):
    origin_client = FakeVersionClient([
        {"Versions": [version("a", 1), version("b", 1)]}
    ])
    overlay_client = FakeVersionClient([{"IsTruncated": False}])
    wire_clients(monkeypatch, origin_client, overlay_client)

    xml_response = main.process_list_objects_v1("bucket", "", None, "", 1, None)
    root = ET.fromstring(xml_response)

    assert root.findtext("Marker") == ""
    assert root.findtext("MaxKeys") == "1"
    assert root.findtext("IsTruncated") == "true"
    assert root.findtext("NextMarker") == "a"
    assert [elem.findtext("Key") for elem in root.findall("Contents")] == ["a"]


def test_list_v1_url_encodes_keys_and_common_prefixes(monkeypatch):
    origin_client = FakeVersionClient([
        {
            "Versions": [
                version("foo+1/bar", 1),
                version("quux ab/thud", 1),
                version("asdf+b", 1),
            ]
        }
    ])
    overlay_client = FakeVersionClient([{"IsTruncated": False}])
    wire_clients(monkeypatch, origin_client, overlay_client)

    xml_response = main.process_list_objects_v1("bucket", "", "/", None, 10, "url")
    root = ET.fromstring(xml_response)

    assert root.findtext("EncodingType") == "url"
    assert [elem.findtext("Key") for elem in root.findall("Contents")] == ["asdf%2Bb"]
    assert [
        elem.findtext("Prefix") for elem in root.findall("CommonPrefixes")
    ] == ["foo%2B1/", "quux%20ab/"]


def test_list_v1_exact_key_ending_in_delimiter_is_content(monkeypatch):
    origin_client = FakeVersionClient([{"Versions": []}])
    overlay_client = FakeVersionClient([
        {"Versions": [version("bucket/asdf/", 1)]}
    ])
    wire_clients(monkeypatch, origin_client, overlay_client)

    xml_response = main.process_list_objects_v1("bucket", "asdf/", "/", None, 10, None)
    root = ET.fromstring(xml_response)

    assert [elem.findtext("Key") for elem in root.findall("Contents")] == ["asdf/"]
    assert root.findall("CommonPrefixes") == []


def test_list_v1_omits_empty_delimiter(monkeypatch):
    origin_client = FakeVersionClient([{"Versions": [version("a", 1)]}])
    overlay_client = FakeVersionClient([{"IsTruncated": False}])
    wire_clients(monkeypatch, origin_client, overlay_client)

    xml_response = main.process_list_objects_v1("bucket", "", "", None, 10, None)
    root = ET.fromstring(xml_response)

    assert root.find("Delimiter") is None


def test_list_versions_small_page_uses_first_origin_page_only(monkeypatch):
    origin_client = FakeVersionClient([
        {
            "Versions": [version("a", 1), version("b", 1)],
            "IsTruncated": True,
            "NextKeyMarker": "b",
        }
    ])
    overlay_client = FakeVersionClient([{"IsTruncated": False}])
    wire_clients(monkeypatch, origin_client, overlay_client)

    entries, is_truncated, next_key_marker, next_version_id_marker = (
        main.collect_list_object_versions_page("bucket", "", None, 1, None, None)
    )

    assert [entry["Name"] for entry in entries] == ["a"]
    assert is_truncated is True
    assert next_key_marker == "a"
    assert next_version_id_marker == "version-a-1"
    assert len(origin_client.calls) == 1
    assert len(overlay_client.calls) == 1


def test_list_versions_ignores_overlay_delete_marker_facilitator(monkeypatch):
    origin_client = FakeVersionClient([{"IsTruncated": False}])
    overlay_client = FakeVersionClient(
        [
            {
                "Versions": [facilitator_version("bucket/a", 3)],
                "DeleteMarkers": [delete_marker("bucket/a", 2)],
            }
        ],
        heads={("bucket/a", "facilitator-version"): facilitator_head()},
    )
    wire_clients(monkeypatch, origin_client, overlay_client)

    entries, is_truncated, next_key_marker, next_version_id_marker = (
        main.collect_list_object_versions_page("bucket", "", None, 10, None, None)
    )

    assert [(entry["Type"], entry["Name"]) for entry in entries] == [("DeleteMarker", "a")]
    assert entries[0]["Object"]["IsLatest"] is True
    assert is_truncated is False
    assert next_key_marker == ""
    assert next_version_id_marker == ""
    assert len(overlay_client.head_calls) == 1


def test_list_versions_ignores_overlay_tag_facilitator(monkeypatch):
    origin_client = FakeVersionClient([
        {"Versions": [version("a", 1)]}
    ])
    overlay_client = FakeVersionClient(
        [
            {
                "Versions": [tag_facilitator_version("bucket/a", 3)],
            }
        ],
        heads={("bucket/a", "tag-facilitator-version"): tag_facilitator_head()},
    )
    wire_clients(monkeypatch, origin_client, overlay_client)

    entries, is_truncated, next_key_marker, next_version_id_marker = (
        main.collect_list_object_versions_page("bucket", "", None, 10, None, None)
    )

    assert [(entry["Type"], entry["Name"]) for entry in entries] == [("Version", "a")]
    assert entries[0]["Object"]["ETag"] == '"etag-a-1"'
    assert is_truncated is False
    assert next_key_marker == ""
    assert next_version_id_marker == ""
    assert len(overlay_client.head_calls) == 1
