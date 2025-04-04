#!/bin/sh

set -e

# Function to check if a bucket exists
check_bucket() {
  mc alias set origin http://minio-origin:9000 origin-access origin-secret
  mc alias set overlay http://minio-overlay:9000 overlay-access overlay-secret

  echo "Waiting for bucket: $1"
  until mc ls "$1" >/dev/null 2>&1; do
    sleep 1
  done
  echo "Bucket $1 is ready."
}

# Check all required buckets
check_bucket "origin/origin-bucket1"
check_bucket "origin/origin-bucket2"
check_bucket "origin/origin-bucket3"
check_bucket "overlay/overlay"

echo "All buckets are ready."

touch /tmp/buckets-ready
sleep inf

