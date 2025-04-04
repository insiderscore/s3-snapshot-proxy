#!/bin/sh
set -e

# Set up alias
echo 'Setting up origin alias...'
mc alias set origin http://minio-origin:9000 origin-access origin-secret

# Create and enable versioning on origin buckets
echo 'Creating origin buckets...'
mc mb origin/origin-bucket1 || true
mc mb origin/origin-bucket2 || true
mc mb origin/origin-bucket3 || true
mc version enable origin/origin-bucket1
mc version enable origin/origin-bucket2
mc version enable origin/origin-bucket3

# Verify buckets exist
echo 'Verifying origin buckets...'
mc ls origin/origin-bucket1
mc ls origin/origin-bucket2
mc ls origin/origin-bucket3

echo 'Origin buckets created successfully!'