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
from datetime import datetime, timezone, timedelta
import time

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

    # Add after all other tests
    print("\n=== Testing Conditional Requests ===\n")
    test_conditional_requests(scale_factor)

def test_conditional_requests(scale_factor):
    """Test conditional requests against the S3 overlay proxy"""
    print("Testing conditional request handling...")
    
    # Set up clients
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
    
    bucket = "origin-bucket1"  # Use the first bucket for conditional tests
    
    # 1. Set up test objects
    print("Setting up test objects for conditional requests...")
    
    # Create an object directly in origin
    origin_key = f"origin-conditional-{random.randint(1000, 9999)}"
    origin_content = f"Origin conditional test content {random.randint(1, 1000)}"
    origin_client.put_object(Bucket=bucket, Key=origin_key, Body=origin_content)
    
    # Get its ETag and LastModified
    origin_meta = origin_client.head_object(Bucket=bucket, Key=origin_key)
    origin_etag = origin_meta['ETag']
    origin_last_modified = origin_meta['LastModified']
    print(f"Created origin object {bucket}/{origin_key} with ETag {origin_etag}")
    
    # Create an object via proxy
    proxy_key = f"proxy-conditional-{random.randint(1000, 9999)}"
    proxy_content = f"Proxy conditional test content {random.randint(1, 1000)}"
    proxy_client.put_object(Bucket=bucket, Key=proxy_key, Body=proxy_content)
    
    # Get its ETag and LastModified
    proxy_meta = proxy_client.head_object(Bucket=bucket, Key=proxy_key)
    proxy_etag = proxy_meta['ETag']
    print(f"Created proxy object {bucket}/{proxy_key} with ETag {proxy_etag}")
    
    # Wait a moment to ensure timestamps differ
    time.sleep(1)
    
    # 2. Test If-Match conditions
    print("Testing If-Match conditions...")
    
    # 2.1 If-Match with correct ETag should succeed
    try:
        response = proxy_client.put_object(
            Bucket=bucket, 
            Key=origin_key, 
            Body="Updated via If-Match",
            IfMatch=origin_etag
        )
        print("✓ If-Match with correct ETag succeeded")
        
        # Get the updated ETag after modification
        updated_meta = proxy_client.head_object(Bucket=bucket, Key=origin_key)
        updated_etag = updated_meta['ETag']
        print(f"Object updated, new ETag is {updated_etag}")
        
    except botocore.exceptions.ClientError as e:
        if '412' in str(e):
            pytest.fail(f"If-Match with correct ETag should succeed but got 412: {e}")
        else:
            pytest.fail(f"If-Match with correct ETag failed with unexpected error: {e}")
            
    # 2.2 If-Match with incorrect ETag should fail with 412
    incorrect_etag = '"00000000000000000000000000000000"'
    try:
        response = proxy_client.put_object(
            Bucket=bucket, 
            Key=origin_key, 
            Body="Should not update",
            IfMatch=incorrect_etag
        )
        pytest.fail(f"If-Match with incorrect ETag should fail but succeeded")
    except botocore.exceptions.ClientError as e:
        if '412' in str(e) or 'PreconditionFailed' in str(e):
            print("✓ If-Match with incorrect ETag correctly failed with precondition error")
        else:
            pytest.fail(f"If-Match with incorrect ETag failed with wrong error: {e}")
    
    # 3. Test If-None-Match conditions
    print("Testing If-None-Match conditions...")
    
    # 3.1 If-None-Match with different ETag should succeed
    try:
        new_key = f"if-none-match-{random.randint(1000, 9999)}"
        response = proxy_client.put_object(
            Bucket=bucket, 
            Key=new_key, 
            Body="Created with If-None-Match",
            IfNoneMatch=origin_etag  # Using ETag from a different object
        )
        print("✓ If-None-Match with different ETag succeeded")
    except botocore.exceptions.ClientError as e:
        pytest.fail(f"If-None-Match with different ETag should succeed but failed: {e}")
    
    # 3.2 If-None-Match with matching ETag should fail with 412
    try:
        response = proxy_client.put_object(
            Bucket=bucket, 
            Key=proxy_key, 
            Body="Should not update",
            IfNoneMatch=proxy_etag
        )
        pytest.fail(f"If-None-Match with matching ETag should fail but succeeded")
    except botocore.exceptions.ClientError as e:
        if '412' in str(e) or 'PreconditionFailed' in str(e):
            print("✓ If-None-Match with matching ETag correctly failed with precondition error")
        else:
            pytest.fail(f"If-None-Match with matching ETag failed with wrong error: {e}")
    
    # 3.3 If-None-Match='*' for existing object should fail with 412
    try:
        response = proxy_client.put_object(
            Bucket=bucket, 
            Key=proxy_key, 
            Body="Should not update",
            IfNoneMatch="*"
        )
        pytest.fail(f"If-None-Match='*' for existing object should fail but succeeded")
    except botocore.exceptions.ClientError as e:
        if '412' in str(e) or 'PreconditionFailed' in str(e):
            print("✓ If-None-Match='*' for existing object correctly failed with precondition error")
        else:
            pytest.fail(f"If-None-Match='*' for existing object failed with wrong error: {e}")
    
    # 3.4 If-None-Match='*' for new object should succeed
    try:
        new_key = f"if-none-match-star-{random.randint(1000, 9999)}"
        response = proxy_client.put_object(
            Bucket=bucket, 
            Key=new_key, 
            Body="Created with If-None-Match='*'",
            IfNoneMatch="*"
        )
        print("✓ If-None-Match='*' for new object succeeded")
    except botocore.exceptions.ClientError as e:
        pytest.fail(f"If-None-Match='*' for new object should succeed but failed: {e}")
    
    # Note: Skipping time-based preconditions (IfModifiedSince/IfUnmodifiedSince) 
    # with PUT operations as they're not supported by the S3 API
    
    # 4. Test time-based conditions with GET
    print("Testing time-based conditions with GET...")
    
    # Format timestamps for HTTP headers
    format_time = lambda dt: dt.strftime('%a, %d %b %Y %H:%M:%S GMT')
    past_time = format_time(datetime.now(timezone.utc) - timedelta(hours=1))
    future_time = format_time(datetime.now(timezone.utc) + timedelta(hours=1))
    
    # 4.1 If-Modified-Since with past date should succeed (return the object)
    try:
        response = proxy_client.get_object(
            Bucket=bucket, 
            Key=origin_key, 
            IfModifiedSince=past_time
        )
        print("✓ GET with If-Modified-Since in past succeeded")
    except botocore.exceptions.ClientError as e:
        pytest.fail(f"GET with If-Modified-Since in past should succeed but failed: {e}")
    
    # 4.2 If-Modified-Since with future date should fail with 304 Not Modified
    try:
        response = proxy_client.get_object(
            Bucket=bucket, 
            Key=origin_key, 
            IfModifiedSince=future_time
        )
        pytest.fail("GET with If-Modified-Since in future should fail but succeeded")
    except botocore.exceptions.ClientError as e:
        if '304' in str(e) or 'Not Modified' in str(e):
            print("✓ GET with If-Modified-Since in future correctly returned 304 Not Modified")
        else:
            pytest.fail(f"GET with If-Modified-Since in future failed with wrong error: {e}")
    
    # 4.3 If-Unmodified-Since with future date should succeed
    try:
        response = proxy_client.get_object(
            Bucket=bucket, 
            Key=origin_key,
            IfUnmodifiedSince=future_time
        )
        print("✓ GET with If-Unmodified-Since in future succeeded")
    except botocore.exceptions.ClientError as e:
        pytest.fail(f"GET with If-Unmodified-Since in future should succeed but failed: {e}")
    
    # 4.4 If-Unmodified-Since with past date should fail with 412
    # (Assuming the object was modified after the past_time)
    try:
        response = proxy_client.get_object(
            Bucket=bucket, 
            Key=origin_key,
            IfUnmodifiedSince=past_time
        )
        pytest.fail("GET with If-Unmodified-Since in past should fail but succeeded")
    except botocore.exceptions.ClientError as e:
        if '412' in str(e) or 'PreconditionFailed' in str(e):
            print("✓ GET with If-Unmodified-Since in past correctly failed with 412")
        else:
            pytest.fail(f"GET with If-Unmodified-Since in past failed with wrong error: {e}")
    
    # 5. Complex test: conditional delete based on ETag from origin
    print("Testing complex conditional scenario...")
    
    # Create new object in origin to test conditional delete
    delete_key = f"conditional-delete-{random.randint(1000, 9999)}"
    origin_client.put_object(Bucket=bucket, Key=delete_key, Body="To be conditionally deleted")
    delete_meta = origin_client.head_object(Bucket=bucket, Key=delete_key)
    delete_etag = delete_meta['ETag']
    
    # Delete should succeed with correct ETag
    try:
        response = proxy_client.delete_object(
            Bucket=bucket, 
            Key=delete_key, 
            IfMatch=delete_etag
        )
        print("✓ Conditional delete with correct ETag succeeded")
    except botocore.exceptions.ClientError as e:
        pytest.fail(f"Conditional delete with correct ETag should succeed but failed: {e}")
    
    # Verify the object is deleted
    try:
        proxy_client.head_object(Bucket=bucket, Key=delete_key)
        pytest.fail("Object should be deleted but still exists")
    except botocore.exceptions.ClientError as e:
        if '404' in str(e):
            print("✓ Object was correctly deleted")
        else:
            pytest.fail(f"Expected 404 for deleted object but got: {e}")
    
    # 6. Test HEAD method with conditional headers
    print("Testing conditional HEAD requests...")
    
    # 6.1 HEAD with If-Match
    try:
        response = proxy_client.head_object(
            Bucket=bucket, 
            Key=origin_key,
            IfMatch=updated_etag  # Use the new ETag, not the original one
        )
        print("✓ HEAD with If-Match succeeded")
    except botocore.exceptions.ClientError as e:
        pytest.fail(f"HEAD with If-Match should succeed but failed: {e}")
    
    # 6.2 HEAD with If-None-Match (should fail for matching ETag)
    try:
        response = proxy_client.head_object(
            Bucket=bucket, 
            Key=origin_key,
            IfNoneMatch=updated_etag  # Use the current ETag, not the original one
        )
        pytest.fail("HEAD with If-None-Match matching ETag should fail but succeeded")
    except botocore.exceptions.ClientError as e:
        if '304' in str(e) or 'Not Modified' in str(e):
            print("✓ HEAD with If-None-Match correctly returned 304 Not Modified")
        else:
            pytest.fail(f"HEAD with If-None-Match failed with wrong error: {e}")
    
    # 7. Test GET with ETag conditions
    print("Testing GET with ETag conditions...")
    
    # 7.1 GET with If-Match
    try:
        response = proxy_client.get_object(
            Bucket=bucket, 
            Key=origin_key,
            IfMatch=updated_etag  # Use the new ETag here too
        )
        print("✓ GET with If-Match succeeded")
    except botocore.exceptions.ClientError as e:
        pytest.fail(f"GET with If-Match should succeed but failed: {e}")
    
    # 7.2 GET with If-None-Match (should fail for matching ETag)
    try:
        response = proxy_client.get_object(
            Bucket=bucket, 
            Key=origin_key,
            IfNoneMatch=updated_etag  # Use the current ETag, not the original one
        )
        pytest.fail("GET with If-None-Match matching ETag should fail but succeeded")
    except botocore.exceptions.ClientError as e:
        if '304' in str(e) or 'Not Modified' in str(e):
            print("✓ GET with If-None-Match correctly returned 304 Not Modified")
        else:
            pytest.fail(f"GET with If-None-Match failed with wrong error: {e}")
    
    # 8. Test DELETE with supported conditional headers
    print("Testing DELETE with proper conditional headers...")
    
    # Create objects for conditional DELETE tests
    delete_match_key = f"if-match-delete-{random.randint(1000, 9999)}"
    proxy_client.put_object(Bucket=bucket, Key=delete_match_key, Body="To be conditionally deleted")
    delete_meta = proxy_client.head_object(Bucket=bucket, Key=delete_match_key)
    delete_match_etag = delete_meta['ETag']
    last_modified = delete_meta['LastModified']
    
    # Wait briefly to ensure consistency
    time.sleep(1)
    
    # 8.1 DELETE with If-Match correct ETag (should succeed)
    try:
        response = proxy_client.delete_object(
            Bucket=bucket, 
            Key=delete_match_key,
            IfMatch=delete_match_etag
        )
        print("✓ DELETE with If-Match correct ETag succeeded")
    except botocore.exceptions.ClientError as e:
        pytest.fail(f"DELETE with If-Match correct ETag should succeed but failed: {e}")
    
    # Verify object is deleted
    try:
        proxy_client.head_object(Bucket=bucket, Key=delete_match_key)
        pytest.fail("Object should be deleted but still exists (If-Match delete)")
    except botocore.exceptions.ClientError as e:
        if '404' in str(e):
            print("✓ Object was correctly deleted with If-Match condition")
        else:
            pytest.fail(f"Expected 404 for deleted object but got: {e}")
    
    # We'll skip testing IfMatchLastModifiedTime and IfMatchSize as they're S3-specific
    # headers that our proxy might not fully implement yet

    print("All conditional request tests passed!")

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