#!/bin/bash
set -e

export MYSQL_HOST="${MYSQL_HOST:-localhost}"
export MYSQL_PORT="${MYSQL_PORT:-3306}"
export MYSQL_USER="${MYSQL_USER:-flink}"
export MYSQL_PASSWORD="${MYSQL_PASSWORD:-flink}"
export MYSQL_DATABASE="${MYSQL_DATABASE:-poc_db}"
export KAFKA_BOOTSTRAP="${KAFKA_BOOTSTRAP:-localhost:9092}"
export KAFKA_TOPIC_PREFIX="${KAFKA_TOPIC_PREFIX:-poc.flink}"

# flink-cdc.sh submits to the JobManager REST endpoint read from rest.address in
# $FLINK_HOME/conf/config.yaml (default 0.0.0.0, unreachable from this container).
# On Podman bridge the JM is the flink-jobmanager service; override via JOBMANAGER_HOST.
JOBMANAGER_HOST="${JOBMANAGER_HOST:-localhost}"
# Replace the address line immediately following the `rest:` key (nested 2.x YAML).
sed -i "/^rest:/{n;s/address:.*/address: ${JOBMANAGER_HOST}/}" "${FLINK_HOME}/conf/config.yaml"

# Note: FLINK_PROPERTIES is processed by flink-cdc.sh via $FLINK_HOME/bin/config.sh
# which sources it and appends to config.yaml automatically. Do NOT manually append
# here — doing so causes duplicate YAML keys (fatal to SnakeYAML) when config.sh
# appends the same env var a second time during startup.

# Ensure the S3 checkpoint bucket exists before submitting.
# podman-compose 1.0.6 does not support service_completed_successfully, so we
# cannot declare a dependency on minio-init.  Creating the bucket here removes
# the race between minio-init and job submission on every fresh stack start.
S3_ENDPOINT="${S3_ENDPOINT:-http://minio:9000}"
S3_BUCKET="${S3_BUCKET:-flink-checkpoints}"
echo "Ensuring S3 bucket '${S3_BUCKET}' exists at ${S3_ENDPOINT}..."
mc alias set s3 "${S3_ENDPOINT}" "${S3_ACCESS_KEY:-minioadmin}" "${S3_SECRET_KEY:-minioadmin}" --insecure 2>/dev/null
mc mb --ignore-existing "s3/${S3_BUCKET}" 2>/dev/null && echo "Bucket '${S3_BUCKET}' ready." || echo "Warning: could not create bucket (will retry on checkpoint)."

# Wait for at least one TaskManager with available slots to register with the
# JobManager before submitting — the JM healthcheck passes before TM registration
# completes, so submitting immediately causes slot-allocation timeouts.
# Uses a subshell so set -e does not exit on curl failure during the poll loop.
echo "Waiting for TaskManager to register with JobManager..."
until (curl -sf "http://${JOBMANAGER_HOST}:8081/taskmanagers" 2>/dev/null \
       | grep -q '"slotsNumber":[1-9]'); do
  sleep 2
done
echo "TaskManager registered. Submitting pipeline..."

envsubst < /pipeline.yaml > /tmp/pipeline-resolved.yaml

# Checkpoint/state config is passed to the submitted job via flink-cdc.sh -D flags.
# The Flink CDC Pipeline API does NOT propagate execution.checkpointing.* / state.*
# keys placed in the pipeline: section of pipeline.yaml, and this container's
# FLINK_PROPERTIES only configures the flink-cdc.sh client process — neither
# reaches the job's execution environment. -D is the documented mechanism (see
# `flink-cdc.sh --help`: "-D  Session dynamic flink config key=val  ... options
# can be found at flink-docs-stable/ops/config.html"). The dir matches the 4 Java
# variants (s3://flink-checkpoints/checkpoints) so this job writes
# checkpoints/<job-id>/ alongside them — 5 folders under checkpoints/ total.
exec flink-cdc.sh /tmp/pipeline-resolved.yaml \
  -D execution.checkpointing.interval=30000 \
  -D execution.checkpointing.timeout=60000 \
  -D execution.checkpointing.mode=EXACTLY_ONCE \
  -D execution.checkpointing.max-concurrent-checkpoints=1 \
  -D execution.checkpointing.min-pause=5000 \
  -D execution.checkpointing.externalized-checkpoint-retention=RETAIN_ON_CANCELLATION \
  -D execution.checkpointing.dir=s3://flink-checkpoints/checkpoints \
  -D execution.checkpointing.savepoint-dir=s3://flink-checkpoints/savepoints \
  -D state.backend=rocksdb
