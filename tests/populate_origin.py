#!/usr/bin/env python

import boto3
import random
import string
import os
import time
from tqdm import tqdm

def random_key(prefix, size=10, max_depth=5):
    depth = random.randint(1, max_depth)
    parts = [prefix]
    for _ in range(depth):
        parts.append("".join(random.choices(string.ascii_letters + string.digits, k=size)))
    return "/".join(parts)

def main():
    # Set up S3 client
    s3 = boto3.client(
        "s3",
        endpoint_url="http://minio-origin:9000",
        aws_access_key_id="origin-access",
        aws_secret_access_key="origin-secret"
    )
    
    # Scale factor from environment
    scale_factor = int(os.environ.get("SCALE_FACTOR", "100"))
    num_objects = 30 * scale_factor  # 3000 objects at scale factor 100
    
    # Populate all buckets
    buckets = ["origin-bucket1", "origin-bucket2", "origin-bucket3"]
    for bucket in buckets:
        print(f"Populating {bucket} with {num_objects} objects")
        for i in range(num_objects):
            if i % 100 == 0:
                print(f"  {i}/{num_objects}")
            key = random_key("origin")
            s3.put_object(Bucket=bucket, Key=key, Body=f"Origin content {i}")
        print(f"Finished populating {bucket}")
    
    print("All buckets populated successfully!")

if __name__ == "__main__":
    main()