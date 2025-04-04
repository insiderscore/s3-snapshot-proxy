import boto3
import random
import string
from tqdm import tqdm
import argparse
import os

# Helper to generate random object keys with varying depth
def random_key(prefix, size=10, max_depth=13):
    depth = random.randint(1, max_depth)  # Random depth between 1 and max_depth
    parts = [prefix] + [
        "".join(random.choices(string.ascii_letters + string.digits, k=size))
        for _ in range(depth)
    ]
    return "/".join(parts)

# Populate origin buckets
def populate_origin(bucket_name, s3_client, num_objects):
    print(f"Populating origin bucket: {bucket_name}")
    for i in tqdm(range(num_objects), desc=f"Origin: {bucket_name}"):
        key = random_key("origin")
        s3_client.put_object(Bucket=bucket_name, Key=key, Body=f"Origin content {i}")

# Populate overlay bucket via proxy
def populate_overlay_via_proxy(proxy_client, overlay_bucket, origin_keys, num_new, num_overlay, num_deleted):
    print(f"Populating overlay bucket: {overlay_bucket}")
    # Add new objects
    for i in tqdm(range(num_new), desc="New objects"):
        key = random_key("new")
        proxy_client.put_object(Bucket=overlay_bucket, Key=key, Body=f"New overlay content {i}")

    # Add overlay objects
    for i, key in enumerate(tqdm(random.sample(origin_keys, num_overlay), desc="Overlay objects")):
        proxy_client.put_object(Bucket=overlay_bucket, Key=key, Body=f"Overlay content {i}")

    # Add delete markers
    for key in tqdm(random.sample(origin_keys, num_deleted), desc="Delete markers"):
        proxy_client.delete_object(Bucket=overlay_bucket, Key=key)

# Test the proxy
def test_proxy(scale_factor):
    # Setup clients
    origin_client = boto3.client(
        "s3",
        endpoint_url="http://minio-origin:9000",
        aws_access_key_id="origin-access",
        aws_secret_access_key="origin-secret"
    )
    proxy_client = boto3.client(
        "s3",
        endpoint_url="http://s3proxy:9000",
        aws_access_key_id="origin-access",  # Use credentials with access to origin buckets
        aws_secret_access_key="origin-secret"
    )

    # Scale the number of objects and operations
    base_num_origin_objects = 10
    base_num_new = 5
    base_num_overlay = 4
    base_num_deleted = 1

    num_origin_objects = int(base_num_origin_objects * scale_factor)
    num_new = int(base_num_new * scale_factor)
    num_overlay = int(base_num_overlay * scale_factor)
    num_deleted = int(base_num_deleted * scale_factor)

    # Populate origin buckets
    origin_buckets = ["origin-bucket1", "origin-bucket2", "origin-bucket3"]
    for bucket in origin_buckets:
        populate_origin(bucket, origin_client, num_origin_objects)

    # Get all origin keys
    origin_keys = []
    for bucket in tqdm(origin_buckets, desc="Fetching origin keys"):
        paginator = origin_client.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=bucket):
            origin_keys.extend([obj["Key"] for obj in page.get("Contents", [])])

    # Populate overlay bucket via proxy
    overlay_bucket = "overlay"
    populate_overlay_via_proxy(proxy_client, overlay_bucket, origin_keys, num_new, num_overlay, num_deleted)

    # Test proxy results
    proxy_keys = []
    for bucket in tqdm(origin_buckets, desc="Testing proxy results"):
        paginator = proxy_client.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=bucket):
            proxy_keys.extend([obj["Key"] for obj in page.get("Contents", [])])

    # Assertions
    assert len(proxy_keys) == len(origin_keys) + num_new - num_deleted
    print("Proxy test passed!")

if __name__ == "__main__":
    # Parse command-line arguments
    parser = argparse.ArgumentParser(description="Run S3 proxy regression tests.")
    parser.add_argument(
        "--scale-factor",
        type=float,
        default=float(os.environ.get("SCALE_FACTOR", 1)),
        help="Scale factor for the test (default: 1 or value from SCALE_FACTOR environment variable)."
    )
    args = parser.parse_args()

    # Run the test with the provided scale factor
    test_proxy(scale_factor=args.scale_factor)