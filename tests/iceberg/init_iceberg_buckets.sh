#!/bin/sh
set -e

echo 'Setting up iceberg origin alias...'
mc alias set origin http://minio-origin:9000 origin-access origin-secret

echo 'Setting up iceberg overlay alias...'
mc alias set overlay http://minio-overlay:9000 overlay-access overlay-secret

echo 'Creating iceberg origin bucket...'
mc mb origin/iceberg-origin || true
mc version enable origin/iceberg-origin
mc ls origin/iceberg-origin

echo 'Creating iceberg overlay buckets...'
for bucket in iceberg-overlay-a iceberg-overlay-b iceberg-overlay-c; do
  mc mb "overlay/${bucket}" || true
  mc version enable "overlay/${bucket}"
  mc ls "overlay/${bucket}"
done

echo 'Iceberg buckets created successfully!'
