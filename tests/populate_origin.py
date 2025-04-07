#!/usr/bin/env python

import boto3
import random
import string
import os
import time
import concurrent.futures
from tqdm import tqdm
from botocore.config import Config

def random_key(prefix, size=10, max_depth=5):
    depth = random.randint(1, max_depth)
    parts = [prefix]
    for _ in range(depth):
        parts.append("".join(random.choices(string.ascii_letters + string.digits, k=size)))
    return "/".join(parts)

def upload_object(args):
    """Upload a single object to S3"""
    s3_client, bucket, i = args
    key = random_key("origin")
    s3_client.put_object(Bucket=bucket, Key=key, Body=f"Origin content {i}")
    return (bucket, key)

def populate_bucket(s3_client, bucket, num_objects, max_workers=32):
    """Populate a bucket with objects using parallel uploads"""
    print(f"Populating {bucket} with {num_objects} objects using {max_workers} workers")
    
    # Create arguments list for each upload
    upload_args = [(s3_client, bucket, i) for i in range(num_objects)]
    
    # Use ThreadPoolExecutor for parallel uploads
    uploaded_keys = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        for key in tqdm(executor.map(upload_object, upload_args), total=num_objects, desc=f"Bucket: {bucket}"):
            uploaded_keys.append(key)
            
    return uploaded_keys

def main():
    # Configure boto3 for performance
    boto_config = Config(
        max_pool_connections=50,  # Increase connection pool
        retries={"max_attempts": 2, "mode": "standard"},  # Reduce retry overhead
    )
    
    # Set up S3 client with optimized config
    s3 = boto3.client(
        "s3",
        endpoint_url="http://minio-origin:9000",
        aws_access_key_id="origin-access",
        aws_secret_access_key="origin-secret",
        config=boto_config
    )
    
    # Scale factor from environment
    scale_factor = int(os.environ.get("SCALE_FACTOR", "100"))
    num_objects = 30 * scale_factor  # 3000 objects at scale factor 100
    
    # Calculate optimal worker count (adjust based on CPU count)
    max_workers = min(32, os.cpu_count() * 4)
    
    # Populate all buckets and collect the keys
    buckets = ["origin-bucket1", "origin-bucket2", "origin-bucket3"]
    all_keys = []
    
    for bucket in buckets:
        keys = populate_bucket(s3, bucket, num_objects, max_workers)
        all_keys.extend(keys)
        print(f"Finished populating {bucket} with initial versions")
    
    print(f"Created {len(all_keys)} initial objects across {len(buckets)} buckets")
    
    # Now let's create second versions and delete markers
    # - 20% of objects get a second version
    # - 15% of objects get a delete marker
    # - Half of the delete markers come before their second version (if any)
    
    # Step 1: Randomly select 20% of objects for second versions
    second_version_count = int(len(all_keys) * 0.20)
    second_version_keys = random.sample(all_keys, second_version_count)
    
    print(f"Creating second versions for {second_version_count} objects ({20}% of total)")
    
    for bucket, key in tqdm(second_version_keys, desc="Creating second versions"):
        # Add a small delay to ensure versions have different timestamps
        time.sleep(0.01)  # 10ms delay
        s3.put_object(Bucket=bucket, Key=key, Body=f"Second version of {key}")
    
    # Step 2: Select 15% of objects for delete markers
    delete_marker_count = int(len(all_keys) * 0.15)
    delete_marker_candidates = random.sample(all_keys, delete_marker_count)
    
    # Step 3: Split delete markers - half before second version, half after
    delete_markers_before = []
    delete_markers_after = []
    
    for i, (bucket, key) in enumerate(delete_marker_candidates):
        if key in [k for b, k in second_version_keys]:
            # This key has a second version
            if i % 2 == 0:  # Half before second version
                delete_markers_before.append((bucket, key))
            else:  # Half after second version
                delete_markers_after.append((bucket, key))
        else:
            # This key doesn't have a second version, just add to one group
            delete_markers_before.append((bucket, key))
    
    # Step 4: Create delete markers that should come BEFORE second versions
    print(f"Creating {len(delete_markers_before)} delete markers before second versions")
    for bucket, key in tqdm(delete_markers_before, desc="Delete markers (before)"):
        s3.delete_object(Bucket=bucket, Key=key)
    
    # Wait a moment to ensure timestamp separation
    time.sleep(1)
    
    # Step 5: Update second versions for keys with delete markers before
    # These second versions won't be visible due to delete markers!
    print("Creating second versions for objects with delete markers (these shouldn't be visible)")
    deleted_with_second_version = [(b, k) for b, k in second_version_keys if k in [k for b, k in delete_markers_before]]
    
    for bucket, key in tqdm(deleted_with_second_version, desc="Second versions after delete"):
        s3.put_object(Bucket=bucket, Key=key, Body=f"Second version after delete marker for {key}")
    
    # Step 6: Create remaining delete markers (that come after second versions)
    print(f"Creating {len(delete_markers_after)} delete markers after second versions")
    for bucket, key in tqdm(delete_markers_after, desc="Delete markers (after)"):
        s3.delete_object(Bucket=bucket, Key=key)
    
    # Summary of what we've done
    print("\nSummary of test data preparation:")
    print(f"Total objects created: {len(all_keys)}")
    print(f"Objects with second versions: {len(second_version_keys)} ({len(second_version_keys)/len(all_keys)*100:.1f}%)")
    print(f"Objects with delete markers: {len(delete_markers_before) + len(delete_markers_after)} " + 
          f"({(len(delete_markers_before) + len(delete_markers_after))/len(all_keys)*100:.1f}%)")
    print(f"  - Delete markers before second version: {len(delete_markers_before)}")
    print(f"  - Delete markers after second version: {len(delete_markers_after)}")
    print(f"Objects with both delete marker and second version: {len(deleted_with_second_version)}")
    
    # Create some special test cases for easier identification in tests
    print("\nCreating special test cases with predictable keys...")
    
    special_cases = [
        ("origin-bucket1", "test-deleted-before-start/object1", "Object deleted before START_TIME"),
        ("origin-bucket1", "test-deleted-before-start/object2", "Object deleted before START_TIME"),
        ("origin-bucket1", "test-multi-version-deleted/object1", "First version to be deleted"),
        ("origin-bucket1", "test-multi-version-deleted/object2", "First version to be deleted")
    ]
    
    for bucket, key, content in special_cases:
        # Create initial version
        s3.put_object(Bucket=bucket, Key=key, Body=f"Initial version: {content}")
        time.sleep(0.5)  # Small delay
        
    # Add second versions to multi-version test objects
    for bucket, key, _ in special_cases[2:]:
        s3.put_object(Bucket=bucket, Key=key, Body=f"Second version of {key}")
        time.sleep(0.5)
    
    # Add third versions to multi-version test objects
    for bucket, key, _ in special_cases[2:]:
        s3.put_object(Bucket=bucket, Key=key, Body=f"Third version of {key}")
        time.sleep(0.5)
        
    # Now add delete markers to all special test objects
    for bucket, key, _ in special_cases:
        s3.delete_object(Bucket=bucket, Key=key)
        time.sleep(0.5)
        
    print("All special test cases created")
    print("All buckets populated successfully!")

if __name__ == "__main__":
    main()