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
    return key

def populate_bucket(s3_client, bucket, num_objects, max_workers=32):
    """Populate a bucket with objects using parallel uploads"""
    print(f"Populating {bucket} with {num_objects} objects using {max_workers} workers")
    
    # Create arguments list for each upload
    upload_args = [(s3_client, bucket, i) for i in range(num_objects)]
    
    # Use ThreadPoolExecutor for parallel uploads
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        list(tqdm(executor.map(upload_object, upload_args), total=num_objects, desc=f"Bucket: {bucket}"))

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
    
    # Populate all buckets
    buckets = ["origin-bucket1", "origin-bucket2", "origin-bucket3"]
    for bucket in buckets:
        populate_bucket(s3, bucket, num_objects, max_workers)
        print(f"Finished populating {bucket}")
    
    print("All buckets populated successfully!")

if __name__ == "__main__":
    main()