import boto3
import random
import string
from tqdm import tqdm
import argparse
import os
import pytest
import botocore.exceptions
import json
import requests
from datetime import datetime, timezone

# Helper to generate random object keys with varying depth
def random_key(prefix, size=10, max_depth=5):
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
def populate_overlay_via_proxy(proxy_client, bucket_names, origin_keys, num_new, num_overlay, num_deleted):
    print(f"Populating overlay via proxy")
    new_keys = []
    overlaid_keys = []
    deleted_keys = []
    
    # Add new objects
    for i in tqdm(range(num_new), desc="New objects"):
        key = random_key("new")
        # Pick a random bucket for new objects
        bucket = random.choice(bucket_names)
        proxy_client.put_object(Bucket=bucket, Key=key, Body=f"New overlay content {i}")
        new_keys.append((bucket, key))

    # Add overlay objects (modify existing objects)
    if origin_keys and num_overlay > 0:
        samples = random.sample(origin_keys, min(num_overlay, len(origin_keys)))
        for i, (bucket, key) in enumerate(tqdm(samples, desc="Overlay objects")):
            proxy_client.put_object(Bucket=bucket, Key=key, Body=f"Overlay content {i}")
            overlaid_keys.append((bucket, key))

    # Add delete markers
    if origin_keys and num_deleted > 0:
        # Filter out keys we've already overlaid
        available_keys = [k for k in origin_keys if k not in overlaid_keys]
        if available_keys:
            samples = random.sample(available_keys, min(num_deleted, len(available_keys)))
            for bucket, key in tqdm(samples, desc="Delete markers"):
                proxy_client.delete_object(Bucket=bucket, Key=key)
                deleted_keys.append((bucket, key))
    
    return new_keys, overlaid_keys, deleted_keys

# Test the proxy
def test_proxy(scale_factor):
    # Get the proxy's START_TIME from the health endpoint
    proxy_host = "s3proxy"  # Docker service name
    proxy_port = 9000       # Container port where proxy is running
    
    # Fetch the health endpoint to get the startTime
    print("Getting proxy's START_TIME from health endpoint...")
    health_url = f"http://{proxy_host}:{proxy_port}/health"
    response = requests.get(health_url)
    health_data = response.json()
    
    if 'startTime' not in health_data:
        pytest.fail("Health endpoint does not include startTime")
        
    start_time_str = health_data['startTime']
    start_time = datetime.fromisoformat(start_time_str)
    print(f"Proxy START_TIME is: {start_time}")
    
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
        aws_access_key_id="origin-access",
        aws_secret_access_key="origin-secret"
    )

    # Scale the number of objects and operations
    base_num_new = 5
    base_num_overlay = 4
    base_num_deleted = 1

    num_new = int(base_num_new * scale_factor)
    num_overlay = int(base_num_overlay * scale_factor)
    num_deleted = int(base_num_deleted * scale_factor)

    origin_buckets = ["origin-bucket1", "origin-bucket2", "origin-bucket3"]
    
    # Get all origin keys with their buckets
    print("Fetching origin keys and their metadata...")
    origin_keys = []
    origin_versions = {}  # To store version info for each key
    
    for bucket in tqdm(origin_buckets, desc="Fetching origin object versions"):
        paginator = origin_client.get_paginator("list_object_versions")
        for page in paginator.paginate(Bucket=bucket):
            if 'Versions' in page:
                for version in page['Versions']:
                    key = version['Key']
                    last_modified = version['LastModified']
                    
                    # This helps us track which versions should be visible through the proxy
                    if (bucket, key) not in origin_versions or last_modified > origin_versions[(bucket, key)]['LastModified']:
                        origin_versions[(bucket, key)] = {
                            'LastModified': last_modified,
                            'VersionId': version['VersionId'],
                            'Size': version['Size'],
                            'ETag': version['ETag'],
                            'ShouldBeVisible': last_modified < start_time
                        }
    
    # Filter to get keys that should be visible through the proxy (created before START_TIME)
    expected_visible_keys = [(bucket, key) for (bucket, key), info in origin_versions.items() 
                           if info['ShouldBeVisible']]
    
    expected_invisible_keys = [(bucket, key) for (bucket, key), info in origin_versions.items() 
                             if not info['ShouldBeVisible']]
    
    print(f"Found {len(origin_versions)} objects in origin")
    print(f"Of these, {len(expected_visible_keys)} should be visible through the proxy (created before START_TIME)")
    print(f"And {len(expected_invisible_keys)} should be invisible (created after START_TIME)")
    
    # Use expected visible keys for overlay operations
    origin_keys = expected_visible_keys
    
    # Populate overlay bucket via proxy
    new_keys, overlaid_keys, deleted_keys = populate_overlay_via_proxy(
        proxy_client, origin_buckets, origin_keys, num_new, num_overlay, num_deleted
    )

    # Test proxy results
    print("Testing proxy functionality...")
    
    # 1. Verify new objects are visible through the proxy
    print("Testing visibility of new objects...")
    for bucket, key in tqdm(random.sample(new_keys, min(10, len(new_keys))), desc="New objects visibility"):
        try:
            response = proxy_client.head_object(Bucket=bucket, Key=key)
            # Success is expected
        except Exception as e:
            pytest.fail(f"New object {bucket}/{key} not found through proxy: {e}")

    # 2. Verify overlay objects have the updated content
    print("Testing overlay objects have updated content...")
    for bucket, key in tqdm(random.sample(overlaid_keys, min(10, len(overlaid_keys))), desc="Overlay verification"):
        origin_content = origin_client.get_object(Bucket=bucket, Key=key)["Body"].read()
        proxy_content = proxy_client.get_object(Bucket=bucket, Key=key)["Body"].read()
        assert proxy_content.startswith(b"Overlay content"), f"Overlay didn't replace origin for key: {bucket}/{key}"
        assert origin_content != proxy_content, f"Overlay content identical to origin for key: {bucket}/{key}"

    # 3. Verify delete markers work
    print("Testing delete markers...")
    for bucket, key in tqdm(random.sample(deleted_keys, min(10, len(deleted_keys))), desc="Delete verification"):
        with pytest.raises(botocore.exceptions.ClientError) as e:
            proxy_client.get_object(Bucket=bucket, Key=key)
        assert "NoSuchKey" in str(e.value)

    # 4. Verify that objects created after START_TIME are not visible
    print("Testing invisibility of objects created after START_TIME...")
    if expected_invisible_keys:
        for bucket, key in tqdm(random.sample(expected_invisible_keys, min(10, len(expected_invisible_keys))), 
                               desc="Post-START_TIME visibility"):
            with pytest.raises(botocore.exceptions.ClientError) as e:
                proxy_client.head_object(Bucket=bucket, Key=key)
            assert "404" in str(e.value), f"Object {bucket}/{key} created after START_TIME is incorrectly visible"

    print("Proxy test passed! All behaviors verified.")

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