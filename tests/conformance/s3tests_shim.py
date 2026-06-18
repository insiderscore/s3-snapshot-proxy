import os
from pathlib import Path

import boto3
import botocore.exceptions
from botocore.client import Config


CREATED_BUCKETS = []


def _env(name, default=None):
    value = os.environ.get(name, default)
    if value is None:
        raise RuntimeError(f"missing required environment variable: {name}")
    return value


def _s3_client(endpoint, access_key, secret_key):
    return boto3.client(
        "s3",
        endpoint_url=endpoint,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        config=Config(signature_version="s3v4"),
    )


def _origin_client():
    access_key = os.environ.get("CONFORMANCE_ORIGIN_ACCESS_KEY") or _env("CONFORMANCE_MAIN_ACCESS_KEY")
    secret_key = os.environ.get("CONFORMANCE_ORIGIN_SECRET_KEY") or _env("CONFORMANCE_MAIN_SECRET_KEY")
    return _s3_client(
        _env("CONFORMANCE_ORIGIN_ENDPOINT"),
        access_key,
        secret_key,
    )


def _overlay_client():
    return _s3_client(
        _env("CONFORMANCE_OVERLAY_ENDPOINT"),
        _env("CONFORMANCE_OVERLAY_ACCESS_KEY"),
        _env("CONFORMANCE_OVERLAY_SECRET_KEY"),
    )


def _delete_versioned_objects(client, bucket, prefix=None):
    marker = {}
    while True:
        params = {"Bucket": bucket, "MaxKeys": 1000}
        if prefix is not None:
            params["Prefix"] = prefix
        params.update(marker)

        try:
            response = client.list_object_versions(**params)
        except botocore.exceptions.ClientError as exc:
            code = exc.response.get("Error", {}).get("Code")
            if code in {"NoSuchBucket", "404"}:
                return
            raise

        objects = []
        for group in ("Versions", "DeleteMarkers"):
            for item in response.get(group, []):
                objects.append({"Key": item["Key"], "VersionId": item["VersionId"]})

        for start in range(0, len(objects), 1000):
            client.delete_objects(
                Bucket=bucket,
                Delete={"Objects": objects[start:start + 1000], "Quiet": True},
            )

        if not response.get("IsTruncated"):
            return

        marker = {"KeyMarker": response.get("NextKeyMarker")}
        if response.get("NextVersionIdMarker"):
            marker["VersionIdMarker"] = response["NextVersionIdMarker"]


def _abort_multipart_uploads(client, bucket, prefix=None):
    marker = {}
    while True:
        params = {"Bucket": bucket, "MaxUploads": 1000}
        if prefix is not None:
            params["Prefix"] = prefix
        params.update(marker)

        try:
            response = client.list_multipart_uploads(**params)
        except botocore.exceptions.ClientError as exc:
            code = exc.response.get("Error", {}).get("Code")
            if code in {"NoSuchBucket", "404"}:
                return
            raise

        for upload in response.get("Uploads", []):
            try:
                client.abort_multipart_upload(
                    Bucket=bucket,
                    Key=upload["Key"],
                    UploadId=upload["UploadId"],
                )
            except botocore.exceptions.ClientError as exc:
                code = exc.response.get("Error", {}).get("Code")
                if code not in {"NoSuchUpload", "404"}:
                    raise

        if not response.get("IsTruncated"):
            return

        marker = {}
        if response.get("NextKeyMarker"):
            marker["KeyMarker"] = response["NextKeyMarker"]
        if response.get("NextUploadIdMarker"):
            marker["UploadIdMarker"] = response["NextUploadIdMarker"]


def _create_origin_bucket(name):
    client = _origin_client()
    try:
        client.create_bucket(Bucket=name)
    except botocore.exceptions.ClientError as exc:
        code = exc.response.get("Error", {}).get("Code")
        if code not in {"BucketAlreadyOwnedByYou", "BucketAlreadyExists"}:
            raise

    client.put_bucket_versioning(
        Bucket=name,
        VersioningConfiguration={"Status": "Enabled"},
    )

    if name not in CREATED_BUCKETS:
        CREATED_BUCKETS.append(name)
    return name

def _ensure_origin_versioning_enabled(name):
    _origin_client().put_bucket_versioning(
        Bucket=name,
        VersioningConfiguration={"Status": "Enabled"},
    )


def _cleanup_bucket(name):
    overlay_bucket = _env("CONFORMANCE_OVERLAY_BUCKET")
    _abort_multipart_uploads(_overlay_client(), overlay_bucket, prefix=f"{name}/")
    _delete_versioned_objects(_overlay_client(), overlay_bucket, prefix=f"{name}/")

    origin = _origin_client()
    _abort_multipart_uploads(origin, name)
    _delete_versioned_objects(origin, name)
    try:
        origin.delete_bucket(Bucket=name)
    except botocore.exceptions.ClientError as exc:
        code = exc.response.get("Error", {}).get("Code")
        if code not in {"NoSuchBucket", "404"}:
            raise


def _cleanup_created_buckets():
    errors = []
    while CREATED_BUCKETS:
        name = CREATED_BUCKETS.pop()
        try:
            _cleanup_bucket(name)
        except Exception as exc:
            errors.append(f"{name}: {exc}")
    if errors:
        raise RuntimeError("failed to clean conformance buckets: " + "; ".join(errors))


def _proxy_resource(sf, name):
    return boto3.resource(
        "s3",
        aws_access_key_id=sf.config.main_access_key,
        aws_secret_access_key=sf.config.main_secret_key,
        endpoint_url=sf.config.default_endpoint,
        use_ssl=sf.config.default_is_secure,
        verify=sf.config.default_ssl_verify,
        config=Config(signature_version="s3v4"),
    ).Bucket(name)


def install():
    import s3tests.functional as sf

    def patched_setup():
        _cleanup_created_buckets()

    def patched_teardown():
        _cleanup_created_buckets()

    def patched_get_new_bucket(client=None, name=None):
        if name is None:
            name = sf.get_new_bucket_name()
        return _create_origin_bucket(name)

    def patched_get_new_bucket_resource(name=None):
        if name is None:
            name = sf.get_new_bucket_name()
        _create_origin_bucket(name)
        return _proxy_resource(sf, name)

    def patched_nuke_prefixed_buckets(prefix, client=None):
        # The proxy intentionally does not implement ListBuckets/DeleteBucket.
        # Per-test cleanup is driven by CREATED_BUCKETS instead.
        return None

    def patched_get_buckets_list(client=None, prefix=None):
        if prefix is None:
            prefix = sf.get_prefix()
        return [name for name in CREATED_BUCKETS if prefix in name]

    sf.setup = patched_setup
    sf.teardown = patched_teardown
    sf.get_new_bucket = patched_get_new_bucket
    sf.get_new_bucket_resource = patched_get_new_bucket_resource
    sf.nuke_prefixed_buckets = patched_nuke_prefixed_buckets
    sf.get_buckets_list = patched_get_buckets_list

    # Leave a cheap breadcrumb in pytest output for debugging fixture issues.
    print(f"installed s3-snapshot-proxy conformance shim from {Path(__file__)}")


def patch_collected_tests():
    import sys

    test_s3 = sys.modules.get("s3tests.functional.test_s3")
    if test_s3 is None:
        return

    def patched_check_configure_versioning_retry(bucket_name, status, expected_string):
        if status != "Enabled" or expected_string != "Enabled":
            raise AssertionError(
                "proxy conformance fixture only supports mandatory Enabled origin versioning"
            )
        _ensure_origin_versioning_enabled(bucket_name)

    test_s3.check_configure_versioning_retry = patched_check_configure_versioning_retry
