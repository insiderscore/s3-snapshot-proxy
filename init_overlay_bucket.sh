#!/bin/sh
set -e

# Set up alias
echo 'Setting up overlay alias...'
mc alias set overlay http://minio-overlay:9000 overlay-access overlay-secret

# Create and enable versioning on overlay bucket
echo 'Creating overlay bucket...'
mc mb overlay/overlay || true
mc version enable overlay/overlay

# Verify bucket exists
echo 'Verifying overlay bucket...'
mc ls overlay/overlay

echo 'Overlay bucket created successfully!'