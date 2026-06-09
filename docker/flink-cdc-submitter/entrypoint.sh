#!/bin/bash
set -e

export MYSQL_HOST="${MYSQL_HOST:-localhost}"
export MYSQL_PORT="${MYSQL_PORT:-3306}"
export MYSQL_USER="${MYSQL_USER:-flink}"
export MYSQL_PASSWORD="${MYSQL_PASSWORD:-flink}"
export MYSQL_DATABASE="${MYSQL_DATABASE:-poc_db}"
export KAFKA_BOOTSTRAP="${KAFKA_BOOTSTRAP:-localhost:9092}"
export KAFKA_TOPIC_PREFIX="${KAFKA_TOPIC_PREFIX:-poc.cdc}"

envsubst < /pipeline.yaml > /tmp/pipeline-resolved.yaml
exec flink-cdc.sh /tmp/pipeline-resolved.yaml
