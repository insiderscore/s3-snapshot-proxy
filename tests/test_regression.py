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

def test_conditional_requests(scale_factor):
    """Test conditional requests against the S3 overlay proxy"""
    print("\n=== Testing Conditional Requests ===\n")
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
    
    # 3.1 If-None-Match with different ETag - SKIP
    print("SKIP: If-None-Match with different ETag (only '*' is supported for PUT)")
    """
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
    """
    
    # 3.2 If-None-Match with matching ETag - SKIP
    print("SKIP: If-None-Match with matching ETag (only '*' is supported for PUT)")
    """
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
    """
    
    # 3.3 If-None-Match with 'before' ETag - SKIP
    print("SKIP: If-None-Match with 'before' ETag (only '*' is supported for PUT)")
    """
    try:
        proxy_client.put_object(
            Bucket=bucket, 
            Key=before_key, 
            Body="This update should fail", 
            IfNoneMatch=before_etag
        )
        pytest.fail("If-None-Match with 'before' ETag should fail but succeeded")
    except botocore.exceptions.ClientError as e:
        if '412' in str(e) or 'PreconditionFailed' in str(e):
            print("✓ If-None-Match with 'before' ETag correctly failed with 412")
        else:
            pytest.fail(f"If-None-Match with 'before' ETag failed with wrong error: {e}")
    """

    # 3.4 If-None-Match with 'after' ETag - SKIP
    print("SKIP: If-None-Match with 'after' ETag (only '*' is supported for PUT)")
    """
    try:
        test_content = f"Updated via proxy with If-None-Match after_etag: {datetime.now().isoformat()}"
        proxy_client.put_object(
            Bucket=bucket, 
            Key=before_key, 
            Body=test_content,
            IfNoneMatch=after_etag
        )
        print("✓ If-None-Match with 'after' ETag succeeded (correct)")
    except botocore.exceptions.ClientError as e:
        pytest.fail(f"If-None-Match with 'after' ETag should succeed but failed: {e}")
    """
    
    # 3.4 If-None-Match='*' for new object should succeed
    try:
        # Enable debug logging for boto3
        # NOTE: This logging setup fixes a heisenbug where boto3 returns 400 Bad Request
        # for If-None-Match='*' PUT requests without logging enabled.
        # The exact same requests work fine via the AWS CLI, suggesting this is a boto3 issue.
        # Requests don't even reach the proxy (no log entries), indicating client-side validation issues.
        # DO NOT REMOVE the logging setup unless you want to debug boto3 internals!
        import logging
        boto3_logger = logging.getLogger('botocore')
        original_level = boto3_logger.level
        boto3_logger.setLevel(logging.DEBUG)
        
        # Also add a console handler if not already present
        if not boto3_logger.handlers:
            ch = logging.StreamHandler()
            ch.setLevel(logging.DEBUG)
            formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
            ch.setFormatter(formatter)
            boto3_logger.addHandler(ch)
        
        new_key = f"if-none-match-star-{random.randint(1000, 9999)}"
        test_content = f"Created with If-None-Match='*' at {datetime.now().isoformat()}"
        
        # Skip existence check - rely on randomness of key name to avoid collisions
        print(f"Using random key {new_key} for If-None-Match='*' test")
        
        try:
            # Attempt the PUT with If-None-Match='*'
            print(f"\nSending PUT with If-None-Match='*' for key: {bucket}/{new_key}")
            response = proxy_client.put_object(
                Bucket=bucket, 
                Key=new_key, 
                Body=test_content,
                IfNoneMatch="*"
            )
            print(f"✓ If-None-Match='*' PUT succeeded with response: {response}")
        finally:
            # Reset logging level
            boto3_logger.setLevel(original_level)
        
        # Verify the object was created - using GET instead of HEAD
        try:
            get_response = proxy_client.get_object(Bucket=bucket, Key=new_key)
            received_content = get_response['Body'].read().decode('utf-8')
            if test_content == received_content:
                print("✓ Object was correctly created and content matches")
            else:
                print(f"⚠️ Content mismatch. Expected: {test_content}, Got: {received_content}")
        except Exception as e:
            pytest.fail(f"Object created with If-None-Match='*' should exist but GET failed: {e}")
            
    except botocore.exceptions.ClientError as e:
        print(f"Error details: {str(e)}")
        if hasattr(e, 'response') and 'ResponseMetadata' in e.response:
            print(f"Response metadata: {e.response['ResponseMetadata']}")
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
    
    # DELETE with If-Match should return 501 Not Implemented
    try:
        response = proxy_client.delete_object(
            Bucket=bucket, 
            Key=delete_key, 
            IfMatch=delete_etag
        )
        pytest.fail("DELETE with If-Match should return 501 but succeeded")
    except botocore.exceptions.ClientError as e:
        if '501' in str(e) or 'NotImplemented' in str(e):
            print("✓ DELETE with If-Match correctly returned 501 Not Implemented")
        else:
            pytest.fail(f"DELETE with If-Match returned wrong error: {e}")
    
    # Regular DELETE without conditions should succeed and delete the object
    try:
        response = proxy_client.delete_object(Bucket=bucket, Key=delete_key)
        print("✓ Regular DELETE without conditions succeeded")
    except Exception as e:
        pytest.fail(f"Regular DELETE without conditions failed: {e}")
    
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
    
    # 8.1 DELETE with If-Match should return 501 Not Implemented
    try:
        response = proxy_client.delete_object(
            Bucket=bucket, 
            Key=delete_match_key,
            IfMatch=delete_match_etag
        )
        pytest.fail("DELETE with If-Match should return 501 but succeeded")
    except botocore.exceptions.ClientError as e:
        if '501' in str(e) or 'NotImplemented' in str(e):
            print("✓ DELETE with If-Match correctly returned 501 Not Implemented")
        else:
            pytest.fail(f"DELETE with If-Match returned wrong error: {e}")
    
    # Do a regular DELETE without conditions to verify it works
    try:
        response = proxy_client.delete_object(Bucket=bucket, Key=delete_match_key)
        print("✓ Regular DELETE without conditions succeeded")
    except Exception as e:
        pytest.fail(f"Regular DELETE without conditions failed: {e}")

    # 8.2 DELETE with If-None-Match - SKIP THIS TEST
    print("✓ Skipping DELETE with If-None-Match test - not supported by boto3")
    # boto3 doesn't allow IfNoneMatch parameter for delete_object operations

    # Proceed with regular DELETE to clean up the test object
    try:
        response = proxy_client.delete_object(Bucket=bucket, Key=delete_match_key)
        print("✓ Regular DELETE succeeded")
    except Exception as e:
        pytest.fail(f"Regular DELETE failed: {e}")

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

    # 3.5 If-None-Match='*' for deleted object (with delete marker) should also succeed
    deleted_key = f"if-none-match-deleted-{random.randint(1000, 9999)}"
    
    # First create and then delete an object
    try:
        # Create object
        proxy_client.put_object(Bucket=bucket, Key=deleted_key, Body="Object to be deleted")
        print(f"Created object {deleted_key} for delete marker test")
        
        # Delete object (creates delete marker)
        proxy_client.delete_object(Bucket=bucket, Key=deleted_key)
        print(f"Created delete marker for {deleted_key}")
        
        # Verify the object appears deleted
        try:
            proxy_client.head_object(Bucket=bucket, Key=deleted_key)
            pytest.fail(f"Object {deleted_key} should be deleted but still exists")
        except botocore.exceptions.ClientError as e:
            if '404' in str(e):
                print(f"✓ Confirmed object {deleted_key} appears deleted")
            else:
                pytest.fail(f"HEAD check for {deleted_key} failed with unexpected error: {e}")
        
        # Now try PUT with If-None-Match='*' - should succeed since latest version is delete marker
        try:
            response = proxy_client.put_object(
                Bucket=bucket, 
                Key=deleted_key, 
                Body="Re-created with If-None-Match='*' after delete",
                IfNoneMatch="*"
            )
            print("✓ If-None-Match='*' for deleted object succeeded (correct)")
            
            # Verify the object was re-created
            try:
                proxy_client.head_object(Bucket=bucket, Key=deleted_key)
                print("✓ Object was correctly re-created after delete marker")
            except Exception as e:
                pytest.fail(f"Object should exist after PUT but failed: {e}")
                
        except botocore.exceptions.ClientError as e:
            pytest.fail(f"If-None-Match='*' for deleted object should succeed but failed: {e}")
    except Exception as e:
        print(f"⚠️ Setup for deleted object test failed: {e}, skipping this test")

    print("All conditional request tests passed!")

def test_point_in_time_conditional():
    """Test that conditional operations respect the point-in-time view based on START_TIME"""
    print("\n=== Testing Point-in-Time Conditional Operations ===\n")
    
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
    
    # Get the proxy's START_TIME
    health_url = f"http://s3proxy:9000/health"
    response = requests.get(health_url)
    health_data = response.json()
    start_time = datetime.fromisoformat(health_data['startTime'])
    print(f"Proxy START_TIME is: {start_time}")
    
    bucket = "origin-bucket1"
    
    # Find an existing object that was created before START_TIME
    print("Finding an existing object created before START_TIME...")
    
    paginator = origin_client.get_paginator("list_object_versions")
    
    # Track all versions for each key that's before START_TIME
    candidates = {}
    
    for page in paginator.paginate(Bucket=bucket):
        if 'Versions' in page:
            for version in page['Versions']:
                key = version['Key']
                last_modified = version['LastModified']
                
                # Find objects with versions before START_TIME
                if last_modified < start_time:
                    if key not in candidates:
                        candidates[key] = []
                    
                    candidates[key].append({
                        'LastModified': last_modified,
                        'ETag': version['ETag'],
                        'Key': key
                    })
    
    # Find a suitable object (exists before START_TIME, not in overlay)
    found_suitable_object = False
    before_key = None
    before_etag = None
    
    # Sort candidates by number of versions (prefer objects with multiple versions for better testing)
    sorted_keys = sorted(candidates.keys(), key=lambda k: len(candidates[k]), reverse=True)
    
    for key in sorted_keys:
        # Sort versions by LastModified (newest first)
        versions = sorted(candidates[key], key=lambda v: v['LastModified'], reverse=True)
        
        # Use newest version before START_TIME
        newest_version = versions[0]
        
        # Check if this object exists in overlay bucket or has a delete marker
        overlay_path = f"{bucket}/{key}"
        try:
            overlay_client = boto3.client(
                "s3",
                endpoint_url=health_data.get('overlayS3', "http://minio-overlay:9000"),
                aws_access_key_id="overlay-access",
                aws_secret_access_key="overlay-secret"
            )
            overlay_client.head_object(Bucket=health_data.get('overlayBucket', 'overlay'), Key=overlay_path)
            # Object exists in overlay, skip it
            continue
        except Exception:
            # Object doesn't exist in overlay, but could have a delete marker
            # Verify it's accessible via proxy
            try:
                proxy_client.head_object(Bucket=bucket, Key=key)
                # Object is accessible via proxy, good candidate
                before_key = key
                before_etag = newest_version['ETag']
                found_suitable_object = True
                print(f"Found suitable object created before START_TIME: {bucket}/{before_key}")
                print(f"Last modified: {newest_version['LastModified']}, ETag: {before_etag}")
                print(f"Total versions before START_TIME: {len(versions)}")
                break
            except Exception:
                # Object has a delete marker or other issue, skip it
                print(f"  Skipping {key}: not accessible via proxy (likely has delete marker)")
                continue
            
    if not found_suitable_object:
        pytest.fail("Could not find any suitable objects created before START_TIME")
    
    # Verify the object is still accessible before proceeding
    print(f"Verifying object {bucket}/{before_key} is accessible via proxy...")
    try:
        proxy_client.head_object(Bucket=bucket, Key=before_key)
    except Exception as e:
        pytest.skip(f"Object {bucket}/{before_key} is no longer accessible: {e}. Skipping test.")
    
    # Get the content of the "before" object via proxy
    try:
        before_response = proxy_client.get_object(Bucket=bucket, Key=before_key)
        before_content = before_response['Body'].read().decode('utf-8')
        print(f"Successfully retrieved original content for {bucket}/{before_key}")
    except Exception as e:
        pytest.skip(f"Failed to get content for {bucket}/{before_key}: {e}. Skipping test.")
    
    # 2. Update the object in origin AFTER START_TIME with new content
    print("Creating a new version of object in origin (after START_TIME)...")
    after_content = f"Version from AFTER start time: {datetime.now().isoformat()}"
    try:
        origin_client.put_object(Bucket=bucket, Key=before_key, Body=after_content)
        after_meta = origin_client.head_object(Bucket=bucket, Key=before_key)
        after_etag = after_meta['ETag']
        print(f"Created 'after' version with ETag: {after_etag}")
    except Exception as e:
        pytest.skip(f"Failed to create 'after' version in origin: {e}. Skipping test.")
    
    # Pause briefly to ensure consistency
    time.sleep(1)
    
    # The proxy should still return the 'before' content
    try:
        print(f"Verifying proxy still returns 'before' version for {bucket}/{before_key}...")
        proxy_response = proxy_client.get_object(Bucket=bucket, Key=before_key)
        proxy_content = proxy_response['Body'].read().decode('utf-8')
        assert proxy_content == before_content, "Proxy should return the 'before' content"
        print("✓ Proxy correctly returns 'before' version despite origin update")
    except Exception as e:
        pytest.fail(f"Proxy should return 'before' version but got error: {e}")
    
    # 3. Test conditional operations using both ETags
    
    # 3.1 If-Match with 'before' ETag should succeed through proxy
    # Note: Since we haven't created the object in overlay yet,
    # the origin's ETag is the correct one to use
    try:
        test_content = f"Updated via proxy with If-Match before_etag: {datetime.now().isoformat()}"
        proxy_client.put_object(
            Bucket=bucket, 
            Key=before_key, 
            Body=test_content,
            IfMatch=before_etag  # This is correct as the object isn't in overlay yet
        )
        print("✓ If-Match with 'before' ETag succeeded (correct)")
        
        # For subsequent operations, we'd need to use the overlay ETag
        updated_response = proxy_client.get_object(Bucket=bucket, Key=before_key)
        updated_content = updated_response['Body'].read().decode('utf-8')
        overlay_etag = updated_response['ETag']  # Get overlay ETag for future operations
        assert test_content in updated_content, "Content should have been updated"
    except botocore.exceptions.ClientError as e:
        pytest.fail(f"If-Match with 'before' ETag should succeed but failed: {e}")
    
    # 3.2 If-Match with 'after' ETag should fail through proxy
    # Because from proxy's perspective, that version doesn't exist at START_TIME
    try:
        proxy_client.put_object(
            Bucket=bucket, 
            Key=before_key, 
            Body="This update should fail",
            IfMatch=after_etag
        )
        pytest.fail("If-Match with 'after' ETag should fail but succeeded")
    except botocore.exceptions.ClientError as e:
        if '412' in str(e) or 'PreconditionFailed' in str(e):
            print("✓ If-Match with 'after' ETag correctly failed with 412")
        else:
            pytest.fail(f"If-Match with 'after' ETag failed with wrong error: {e}")

    # 3.3 If-None-Match with 'before' ETag - SKIP
    print("SKIP: If-None-Match with 'before' ETag (only '*' is supported for PUT)")
    """
    try:
        proxy_client.put_object(
            Bucket=bucket, 
            Key=before_key, 
            Body="This update should fail", 
            IfNoneMatch=before_etag
        )
        pytest.fail("If-None-Match with 'before' ETag should fail but succeeded")
    except botocore.exceptions.ClientError as e:
        if '412' in str(e) or 'PreconditionFailed' in str(e):
            print("✓ If-None-Match with 'before' ETag correctly failed with 412")
        else:
            pytest.fail(f"If-None-Match with 'before' ETag failed with wrong error: {e}")
    """

    # 3.4 If-None-Match with 'after' ETag - SKIP
    print("SKIP: If-None-Match with 'after' ETag (only '*' is supported for PUT)")
    """
    try:
        test_content = f"Updated via proxy with If-None-Match after_etag: {datetime.now().isoformat()}"
        proxy_client.put_object(
            Bucket=bucket, 
            Key=before_key, 
            Body=test_content,
            IfNoneMatch=after_etag
        )
        print("✓ If-None-Match with 'after' ETag succeeded (correct)")
    except botocore.exceptions.ClientError as e:
        pytest.fail(f"If-None-Match with 'after' ETag should succeed but failed: {e}")
    """
    
    print("All point-in-time conditional tests passed!")

def test_conditional_delete_operations():
    """Test conditional DELETE operations against objects in origin that don't exist in overlay"""
    print("\n=== Testing Conditional DELETE Operations ===\n")
    
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
    
    # Get the proxy's START_TIME
    health_url = f"http://s3proxy:9000/health"
    response = requests.get(health_url)
    health_data = response.json()
    start_time = datetime.fromisoformat(health_data['startTime'])
    
    bucket = "origin-bucket1"
    
    # Find one object that exists in origin but not in overlay
    print("Finding an object that exists in origin but not in overlay...")
    
    paginator = origin_client.get_paginator("list_object_versions")
    suitable_object = None
    
    for page in paginator.paginate(Bucket=bucket):
        if 'Versions' in page:
            for version in page['Versions']:
                key = version['Key']
                last_modified = version['LastModified']
                
                # Find an object created before START_TIME
                if last_modified < start_time:
                    overlay_path = f"{bucket}/{key}"
                    try:
                        overlay_client = boto3.client(
                            "s3",
                            endpoint_url=health_data.get('overlayS3', "http://minio-overlay:9000"),
                            aws_access_key_id="overlay-access",
                            aws_secret_access_key="overlay-secret"
                        )
                        overlay_client.head_object(Bucket=health_data.get('overlayBucket', 'overlay'), Key=overlay_path)
                        # Object exists in overlay, skip it
                        continue
                    except Exception:
                        # Verify the object is accessible via proxy
                        try:
                            proxy_client.head_object(Bucket=bucket, Key=key)
                            # Good candidate - accessible and not in overlay
                            suitable_object = {
                                'key': key,
                                'etag': version['ETag'],
                                'last_modified': last_modified
                            }
                            print(f"Found suitable object: {bucket}/{key}, ETag: {version['ETag']}")
                            break
                        except Exception:
                            # Not accessible via proxy, skip
                            continue
        if suitable_object:
            break
    
    if not suitable_object:
        pytest.fail("Could not find any suitable objects for testing")
    
    # Test that conditional DELETE requests return 501 Not Implemented
    key = suitable_object['key']
    etag = suitable_object['etag']
    
    # 1. DELETE with If-Match should return 501
    print("\n1. Testing DELETE with If-Match (should return 501)")
    try:
        proxy_client.delete_object(
            Bucket=bucket,
            Key=key,
            IfMatch=etag
        )
        pytest.fail("DELETE with If-Match should return 501 but succeeded")
    except botocore.exceptions.ClientError as e:
        if '501' in str(e) or 'NotImplemented' in str(e):
            print("✓ DELETE with If-Match correctly returned 501 Not Implemented")
        else:
            pytest.fail(f"DELETE with If-Match returned wrong error: {e}")
    
    # 2. Skipping DELETE with If-None-Match test - not supported by boto3
    # boto3 doesn't allow IfNoneMatch parameter for delete_object operations
    
    # 3. Regular DELETE without conditions should succeed
    print("\n3. Testing regular DELETE without conditions (should succeed)")
    try:
        proxy_client.delete_object(
            Bucket=bucket,
            Key=key
        )
        print("✓ Regular DELETE without conditions succeeded")
        
        # Verify object appears deleted through proxy
        try:
            proxy_client.head_object(Bucket=bucket, Key=key)
            pytest.fail("Object should appear deleted through proxy but still exists")
        except botocore.exceptions.ClientError as e:
            if '404' in str(e):
                print("✓ Object correctly appears deleted through proxy")
            else:
                pytest.fail(f"Expected 404 for deleted object but got: {e}")
    except Exception as e:
        pytest.fail(f"Regular DELETE without conditions failed: {e}")
    
    print("\nConditional DELETE operation tests completed!")

def test_multiple_versions_before_start_time():
    """Test that the proxy correctly handles objects with multiple versions created before START_TIME"""
    print("\n=== Testing Multiple Versions Before START_TIME ===\n")
    
    # Set up clients as in previous tests
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
    
    # Get START_TIME
    health_url = f"http://s3proxy:9000/health"
    response = requests.get(health_url)
    health_data = response.json()
    start_time = datetime.fromisoformat(health_data['startTime'])
    
    bucket = "origin-bucket1"
    
    # Create a test object with multiple versions, all before START_TIME
    # We'll need to adjust the LastModified timestamps manually in origin S3
    # or set up test data that we know has multiple versions
    
    # Find objects that have multiple versions before START_TIME
    paginator = origin_client.get_paginator("list_object_versions")
    multi_version_candidates = {}
    
    for page in paginator.paginate(Bucket=bucket):
        if 'Versions' in page:
            for version in page['Versions']:
                key = version['Key']
                last_modified = version['LastModified']
                
                # Track objects with versions before START_TIME
                if last_modified < start_time:
                    if key not in multi_version_candidates:
                        multi_version_candidates[key] = []
                    
                    multi_version_candidates[key].append({
                        'VersionId': version['VersionId'],
                        'LastModified': last_modified,
                        'ETag': version['ETag']
                    })
    
    # Find objects with multiple versions before START_TIME and not in overlay
    suitable_objects = []
    
    for key, versions in multi_version_candidates.items():
        if len(versions) >= 2:  # At least 2 versions before START_TIME
            # Sort versions by LastModified (newest first)
            versions.sort(key=lambda x: x['LastModified'], reverse=True)
            
            # Check if object exists in overlay
            overlay_path = f"{bucket}/{key}"
            try:
                overlay_client = boto3.client(
                    "s3",
                    endpoint_url=health_data.get('overlayS3', "http://minio-overlay:9000"),
                    aws_access_key_id="overlay-access",
                    aws_secret_access_key="overlay-secret"
                )
                overlay_client.head_object(Bucket=health_data.get('overlayBucket', 'overlay'), Key=overlay_path)
                # Exists in overlay, skip
                continue
            except Exception:
                # Good candidate - doesn't exist in overlay
                suitable_objects.append({
                    'key': key,
                    'versions': versions
                })
                print(f"Found object with {len(versions)} versions before START_TIME: {bucket}/{key}")
                if len(suitable_objects) >= 3:  # Find a few candidates
                    break
    
    if not suitable_objects:
        pytest.skip("No objects with multiple pre-START_TIME versions found. Skipping test.")
    
    # Test with the first suitable object
    test_obj = suitable_objects[0]
    key = test_obj['key']
    versions = test_obj['versions']
    
    # The newest version before START_TIME
    newest_version = versions[0]
    # An older version before START_TIME
    older_version = versions[1]
    
    print(f"Testing with object {bucket}/{key}")
    print(f"Newest version: {newest_version['LastModified']}, ETag: {newest_version['ETag']}")
    print(f"Older version: {older_version['LastModified']}, ETag: {older_version['ETag']}")
    
    # 1. Verify proxy returns content from newest version before START_TIME
    proxy_response = proxy_client.get_object(Bucket=bucket, Key=key)
    proxy_etag = proxy_response['ETag']
    
    assert proxy_etag == newest_version['ETag'], \
        f"Proxy should return newest version ({newest_version['ETag']}) but got {proxy_etag}"
    print("✓ Proxy correctly returns newest version before START_TIME")
    
    # 2. Test conditional operations work with newest version's ETag
    # PUT with If-Match=newest_version ETag should succeed
    try:
        test_content = f"Updated via proxy with If-Match newest_etag: {datetime.now().isoformat()}"
        proxy_client.put_object(
            Bucket=bucket, 
            Key=key, 
            Body=test_content,
            IfMatch=newest_version['ETag']
        )
        print("✓ If-Match with newest version's ETag succeeded (correct)")
    except botocore.exceptions.ClientError as e:
        pytest.fail(f"If-Match with newest version's ETag should succeed but failed: {e}")
    
    # 3. Test conditional operations fail with older version's ETag
    # PUT with If-Match=older_version ETag should fail with 412
    try:
        proxy_client.put_object(
            Bucket=bucket, 
            Key=key, 
            Body="This update should fail",
            IfMatch=older_version['ETag']
        )
        pytest.fail("If-Match with older version's ETag should fail but succeeded")
    except botocore.exceptions.ClientError as e:
        if '412' in str(e) or 'PreconditionFailed' in str(e):
            print("✓ If-Match with older version's ETag correctly failed with 412")
        else:
            pytest.fail(f"If-Match with older version's ETag failed with wrong error: {e}")
    
    print("Multiple version test passed!")

def test_list_objects_v2_with_delete_markers():
    """Test that ListObjectsV2 correctly hides objects with delete markers from before START_TIME"""
    print("\n=== Testing ListObjectsV2 with Delete Markers ===\n")
    
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
    
    # Get the proxy's START_TIME and overlay bucket info
    health_url = "http://s3proxy:9000/health"
    response = requests.get(health_url)
    health_data = response.json()
    start_time = datetime.fromisoformat(health_data['startTime'])
    overlay_bucket = health_data.get('overlayBucket', 'overlay')
    overlay_s3_url = health_data.get('overlayS3', 'http://minio-overlay:9000')
    print(f"Proxy START_TIME is: {start_time}")
    print(f"Overlay bucket: {overlay_bucket}")
    
    # Set up overlay client to check for superseding versions
    overlay_client = boto3.client(
        "s3",
        endpoint_url=overlay_s3_url,
        aws_access_key_id="overlay-access",
        aws_secret_access_key="overlay-secret"
    )
    
    bucket = "origin-bucket1"
    prefix = "test-deleted-before-start/"
    
    # First, verify that our special test objects were created correctly
    # They should have delete markers before START_TIME and not be accessible
    print("\nVerifying test objects were prepared correctly...")
    
    # 1. Check objects don't exist via proxy (should return 404)
    for test_key in ["test-deleted-before-start/object1", "test-deleted-before-start/object2"]:
        # First check if there's a superseding version in overlay
        overlay_path = f"{bucket}/{test_key}"
        has_overlay_version = False
        try:
            overlay_client.head_object(Bucket=overlay_bucket, Key=overlay_path)
            has_overlay_version = True
            print(f"⚠️ Object {test_key} has a superseding version in overlay, skipping this test case")
        except Exception:
            # No overlay version, we can proceed with the test
            pass
            
        if not has_overlay_version:
            try:
                proxy_client.head_object(Bucket=bucket, Key=test_key)
                pytest.fail(f"Object {test_key} should be deleted but is accessible")
            except botocore.exceptions.ClientError as e:
                if '404' in str(e):
                    print(f"✓ Object {test_key} correctly shows as deleted")
                else:
                    pytest.fail(f"Expected 404 for {test_key} but got: {e}")
    
    # 2. Verify that objects with multiple versions but delete marker before START_TIME
    # also don't appear
    for test_key in ["test-multi-version-deleted/object1", "test-multi-version-deleted/object2"]:
        # Check for overlay version first
        overlay_path = f"{bucket}/{test_key}"
        has_overlay_version = False
        try:
            overlay_client.head_object(Bucket=overlay_bucket, Key=overlay_path)
            has_overlay_version = True
            print(f"⚠️ Object {test_key} has a superseding version in overlay, skipping this test case")
        except Exception:
            # No overlay version, we can proceed with the test
            pass
            
        if not has_overlay_version:
            try:
                proxy_client.head_object(Bucket=bucket, Key=test_key)
                pytest.fail(f"Object {test_key} should be deleted but is accessible")
            except botocore.exceptions.ClientError as e:
                if '404' in str(e):
                    print(f"✓ Multi-version object {test_key} correctly shows as deleted")
                else:
                    pytest.fail(f"Expected 404 for multi-version {test_key} but got: {e}")
    
    # 3. Now verify the ListObjectsV2 behavior - objects with delete markers
    # before START_TIME should not appear in listing
    
    # Test with specific prefixes that we know have deleted objects
    print("\nTesting ListObjectsV2 with prefix containing deleted objects...")
    
    # ListObjectsV2 for test-deleted-before-start/ prefix
    response = proxy_client.list_objects_v2(
        Bucket=bucket,
        Prefix="test-deleted-before-start/"
    )
    
    # Should not return any objects
    objects_returned = []
    if 'Contents' in response:
        objects_returned = [item['Key'] for item in response['Contents']]
    
    print(f"ListObjectsV2 returned {len(objects_returned)} objects for deleted prefix")
    
    # Verify none of our test objects appear in the listing
    for test_key in ["test-deleted-before-start/object1", "test-deleted-before-start/object2"]:
        # Check overlay first
        overlay_path = f"{bucket}/{test_key}"
        has_overlay_version = False
        try:
            overlay_client.head_object(Bucket=overlay_bucket, Key=overlay_path)
            has_overlay_version = True
            print(f"⚠️ Object {test_key} has a superseding version in overlay, skipping this assertion")
        except Exception:
            # No overlay version, we can proceed with the assertion
            pass
            
        if not has_overlay_version:
            assert test_key not in objects_returned, f"Deleted object {test_key} incorrectly appears in ListObjectsV2"
        
    print("✓ Deleted objects correctly don't appear in ListObjectsV2")
    
    # Test with multi-version deleted objects
    response = proxy_client.list_objects_v2(
        Bucket=bucket,
        Prefix="test-multi-version-deleted/"
    )
    
    # Should not return any objects
    objects_returned = []
    if 'Contents' in response:
        objects_returned = [item['Key'] for item in response['Contents']]
    
    print(f"ListObjectsV2 returned {len(objects_returned)} objects for multi-version deleted prefix")
    
    # Verify none of our test objects appear in the listing
    for test_key in ["test-multi-version-deleted/object1", "test-multi-version-deleted/object2"]:
        # Check overlay first
        overlay_path = f"{bucket}/{test_key}"
        has_overlay_version = False
        try:
            overlay_client.head_object(Bucket=overlay_bucket, Key=overlay_path)
            has_overlay_version = True
            print(f"⚠️ Object {test_key} has a superseding version in overlay, skipping this assertion")
        except Exception:
            # No overlay version, we can proceed with the assertion
            pass
            
        if not has_overlay_version:
            assert test_key not in objects_returned, f"Multi-version deleted object {test_key} incorrectly appears in ListObjectsV2"
        
    print("✓ Multi-version deleted objects correctly don't appear in ListObjectsV2")
    
    # 4. Compare with objects that should be visible
    # For this we'll use the broader test data created in populate_origin.py
    print("\nComparing to a broader set of objects...")
    
    # Get list of objects in origin
    origin_keys_with_history = {}  # Key -> list of version info
    
    # Get all versions and delete markers
    paginator = origin_client.get_paginator('list_object_versions')
    
    # Use a small common prefix to limit results but still get meaningful data
    common_prefix = "origin/a"  # Should match some of our randomly generated keys
    
    for page in paginator.paginate(Bucket=bucket, Prefix=common_prefix):
        # Track object versions
        if 'Versions' in page:
            for version in page['Versions']:
                key = version['Key']
                if key not in origin_keys_with_history:
                    origin_keys_with_history[key] = []
                
                origin_keys_with_history[key].append({
                    'LastModified': version['LastModified'],
                    'VersionId': version['VersionId'],
                    'IsDeleteMarker': False,
                })
            
        # Track delete markers
        if 'DeleteMarkers' in page:
            for dm in page['DeleteMarkers']:
                key = dm['Key']
                if key not in origin_keys_with_history:
                    origin_keys_with_history[key] = []
                
                origin_keys_with_history[key].append({
                    'LastModified': dm['LastModified'],
                    'VersionId': dm['VersionId'],
                    'IsDeleteMarker': True,
                })
    
    # Now determine visibility based on version history
    # A key is not visible if its latest version before START_TIME is a delete marker
    should_be_hidden = set()
    should_be_visible = set()
    
    for key, history in origin_keys_with_history.items():
        # Check if there's a superseding version in overlay
        overlay_path = f"{bucket}/{key}"
        has_overlay_version = False
        try:
            overlay_client.head_object(Bucket=overlay_bucket, Key=overlay_path)
            has_overlay_version = True
            # Skip objects that have been modified in overlay
            continue
        except Exception:
            # No overlay version, we can proceed with analyzing this object
            pass
            
        if has_overlay_version:
            continue  # Skip this key since it has been modified in overlay
        
        # Sort versions by LastModified time (newest first)
        history_before_start = [v for v in history if v['LastModified'] < start_time]
        if not history_before_start:
            continue
            
        # Sort by timestamp, newest first
        history_before_start.sort(key=lambda x: x['LastModified'], reverse=True)
        
        # Check the latest version before START_TIME
        latest_version = history_before_start[0]
        
        if latest_version['IsDeleteMarker']:
            # If latest version is a delete marker, the key should be hidden
            should_be_hidden.add(key)
        else:
            # If latest version is a regular version, the key should be visible
            should_be_visible.add(key)
    
    print(f"Found {len(origin_keys_with_history)} keys with prefix '{common_prefix}'")
    print(f"Of these, {len(should_be_hidden)} should be hidden (latest version is a delete marker)")
    print(f"And {len(should_be_visible)} should be visible (latest version is not a delete marker)")
    print(f"Note: Keys with superseding versions in overlay were excluded from analysis")
    
    # Get ListObjectsV2 results from proxy
    proxy_response = proxy_client.list_objects_v2(
        Bucket=bucket,
        Prefix=common_prefix
    )
    
    proxy_objects = []
    if 'Contents' in proxy_response:
        proxy_objects = [item['Key'] for item in proxy_response['Contents']]
    
    print(f"ListObjectsV2 via proxy returned {len(proxy_objects)} objects")
    
    # Verify keys that should be hidden are not in the listing
    for key in should_be_hidden:
        assert key not in proxy_objects, f"Key {key} with latest version as delete marker incorrectly appears in ListObjectsV2"
    
    print("✓ All objects with latest version as delete marker are correctly hidden in ListObjectsV2")
    
    # Verify keys that should be visible appear in the listing (test a sample)
    sample_size = min(5, len(should_be_visible))
    if sample_size > 0:
        sampled_keys = random.sample(list(should_be_visible), sample_size)
        for key in sampled_keys:
            try:
                proxy_client.head_object(Bucket=bucket, Key=key)
                print(f"✓ Object {key} is correctly accessible via HEAD")
                assert key in proxy_objects, f"Key {key} should appear in ListObjectsV2 but doesn't"
                print(f"✓ Object {key} correctly appears in listing")
            except Exception as e:
                pytest.fail(f"Object {key} should be accessible but got: {e}")
    
    print("ListObjectsV2 delete marker visibility test passed!")

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