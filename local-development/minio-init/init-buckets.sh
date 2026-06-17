#!/bin/bash
set -e

echo "MinIO bucket initialization starting..."

# Wait for MinIO to be ready
until curl -f http://localhost:9000/minio/health/live 2>/dev/null; do
  echo "Waiting for MinIO to be ready..."
  sleep 2
done

# Create the flink-checkpoints bucket if it doesn't exist
echo "Creating flink-checkpoints bucket..."
# MinIO doesn't have a built-in mc command in the container, so we use the API directly
# Create bucket via AWS S3 API
curl -X PUT http://localhost:9000/flink-checkpoints \
  -H "Authorization: AWS4-HMAC-SHA256 Credential=minioadmin/20240101/us-east-1/s3/aws4_request, SignedHeaders=host;x-amz-content-sha256;x-amz-date, Signature=placeholder" \
  || echo "Bucket may already exist or will be created on first write"

# Create the flink-snapshots bucket for savepoint storage
echo "Creating flink-snapshots bucket..."
curl -X PUT http://localhost:9000/flink-snapshots \
  -H "Authorization: AWS4-HMAC-SHA256 Credential=minioadmin/20240101/us-east-1/s3/aws4_request, SignedHeaders=host;x-amz-content-sha256;x-amz-date, Signature=placeholder" \
  || echo "Bucket may already exist or will be created on first write"

# Create subdirectories within flink-checkpoints bucket for each job variant
# Each variant gets distinct checkpoint and savepoint directories to avoid state collision

declare -a VARIANTS=(
  "datastream"
  "table-api"
  "sql-api"
  "outbox"
  "yaml-pipeline"
)

for variant in "${VARIANTS[@]}"; do
  checkpoint_dir="flink-checkpoints/${variant}-checkpoints"
  savepoint_dir="flink-checkpoints/${variant}-savepoints"

  echo "Creating ${checkpoint_dir} directory..."
  echo "" | curl -X PUT -d @- http://localhost:9000/${checkpoint_dir}/.mkdir \
    || echo "Directory may already exist"

  echo "Creating ${savepoint_dir} directory..."
  echo "" | curl -X PUT -d @- http://localhost:9000/${savepoint_dir}/.mkdir \
    || echo "Directory may already exist"
done

echo "MinIO initialization complete"
