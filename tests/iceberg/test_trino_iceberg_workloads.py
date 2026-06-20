import concurrent.futures
import os
import time
from datetime import datetime

import boto3
import pytest
import requests

from trino_helpers import query, run_ignoring_missing, scalar, wait_for_trino


BUCKET = os.environ.get("ICEBERG_ORIGIN_BUCKET", "iceberg-origin")
SCHEMA = os.environ.get("ICEBERG_SCHEMA", "lake")
TABLES = ["events_a", "events_b", "events_c", "events_d"]
SNAPSHOTS = [
    ("snap_a", "s3proxy-iceberg-a", 9000),
    ("snap_b", "s3proxy-iceberg-b", 9001),
    ("snap_c", "s3proxy-iceberg-c", 9002),
]


def s3_client(endpoint, access_key="origin-access", secret_key="origin-secret"):
    return boto3.client(
        "s3",
        endpoint_url=endpoint,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
    )


def wait_for_proxy(host, timeout_seconds=120):
    url = f"http://{host}:9000/health"
    deadline = datetime.now().timestamp() + timeout_seconds
    last_error = None
    while datetime.now().timestamp() < deadline:
        try:
            response = requests.get(url, timeout=5)
            if response.status_code == 200:
                data = response.json()
                return datetime.fromisoformat(data["startTime"])
            last_error = RuntimeError(f"{url} returned {response.status_code}")
        except Exception as exc:
            last_error = exc
        time.sleep(1)
    raise TimeoutError(f"Timed out waiting for {url}: {last_error}")


def table_location(table):
    return f"s3://{BUCKET}/warehouse/{SCHEMA}/{table}"


def metadata_prefix(table):
    return f"warehouse/{SCHEMA}/{table}/metadata/"


def latest_metadata_file(client, table):
    paginator = client.get_paginator("list_objects_v2")
    candidates = []
    for page in paginator.paginate(Bucket=BUCKET, Prefix=metadata_prefix(table)):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            if not key.endswith(".metadata.json"):
                continue
            candidates.append(obj)

    if not candidates:
        pytest.fail(f"No metadata file for {table}")

    latest = max(candidates, key=lambda item: item["LastModified"])
    return latest["Key"].split("/")[-1]


def register_table(catalog, table, metadata_file_name):
    query(
        f"""
        CREATE SCHEMA IF NOT EXISTS {catalog}.{SCHEMA}
        WITH (location = 's3://{BUCKET}/warehouse/{catalog}/{SCHEMA}')
        """
    )
    run_ignoring_missing(
        f"""
        CALL {catalog}.system.unregister_table(
            schema_name => '{SCHEMA}',
            table_name => '{table}'
        )
        """
    )
    query(
        f"""
        CALL {catalog}.system.register_table(
            schema_name => '{SCHEMA}',
            table_name => '{table}',
            table_location => '{table_location(table)}',
            metadata_file_name => '{metadata_file_name}'
        )
        """,
        retries=2,
    )


def register_catalog(catalog, endpoint):
    client = s3_client(endpoint)
    registered = {}
    for table in TABLES:
        metadata_file_name = latest_metadata_file(client, table)
        register_table(catalog, table, metadata_file_name)
        registered[table] = metadata_file_name
    return registered


def overlay_batch(catalog):
    return 9000 + SNAPSHOTS.index(next(item for item in SNAPSHOTS if item[0] == catalog))


def validate_catalog(catalog):
    results = {}

    for table in TABLES:
        row_count = scalar(f"SELECT count(*) FROM {catalog}.{SCHEMA}.{table}", retries=2)
        file_count = scalar(f'SELECT count(*) FROM {catalog}.{SCHEMA}."{table}$files"', retries=2)
        snapshot_count = scalar(f'SELECT count(*) FROM {catalog}.{SCHEMA}."{table}$snapshots"', retries=2)

        assert row_count is not None and row_count > 0
        assert file_count is not None and file_count > 0
        assert snapshot_count is not None and snapshot_count > 0

        results[table] = {
            "rows": row_count,
            "files": file_count,
            "snapshots": snapshot_count,
        }

    return results


def test_trino_iceberg_snapshot_read_write_workloads():
    wait_for_trino()

    proxy_start_times = {
        catalog: wait_for_proxy(host)
        for catalog, host, _ in SNAPSHOTS
    }
    assert proxy_start_times["snap_a"] <= proxy_start_times["snap_b"]
    assert proxy_start_times["snap_b"] <= proxy_start_times["snap_c"]

    registered_origin = register_catalog("origin", os.environ.get("ICEBERG_ORIGIN_ENDPOINT", "http://minio-origin:9000"))
    registered = {
        catalog: register_catalog(catalog, f"http://{host}:9000")
        for catalog, host, _ in SNAPSHOTS
    }
    print(f"origin registered metadata: {registered_origin}")
    for catalog, tables in registered.items():
        print(f"{catalog} registered metadata: {tables}")

    with concurrent.futures.ThreadPoolExecutor(max_workers=len(SNAPSHOTS)) as executor:
        futures = {
            executor.submit(validate_catalog, catalog): catalog
            for catalog, _, _ in [("origin", "minio-origin", 9000), *SNAPSHOTS]
        }
        validation_results = {
            futures[future]: future.result()
            for future in concurrent.futures.as_completed(futures)
        }

    for catalog, table_results in validation_results.items():
        print(f"{catalog} validation results: {table_results}")

    for catalog, _, _ in SNAPSHOTS:
        batch = overlay_batch(catalog)
        for table in TABLES:
            origin_count = scalar(
                f"SELECT count(*) FROM origin.{SCHEMA}.{table} WHERE batch = {batch}",
                retries=2,
            )
            assert origin_count == 0

            self_count = scalar(
                f"SELECT count(*) FROM {catalog}.{SCHEMA}.{table} WHERE batch = {batch}",
                retries=2,
            )
            assert self_count == 3

            for other_catalog, _, _ in SNAPSHOTS:
                if other_catalog == catalog:
                    continue
                other_count = scalar(
                    f"SELECT count(*) FROM {other_catalog}.{SCHEMA}.{table} WHERE batch = {batch}",
                    retries=2,
                )
                assert other_count == 0
