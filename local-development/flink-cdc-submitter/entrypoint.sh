#!/bin/bash
set -e

export MYSQL_HOST="${MYSQL_HOST:-localhost}"
export MYSQL_PORT="${MYSQL_PORT:-3306}"
export MYSQL_USER="${MYSQL_USER:-flink}"
export MYSQL_PASSWORD="${MYSQL_PASSWORD:-flink}"
export MYSQL_DATABASE="${MYSQL_DATABASE:-poc_db}"
export KAFKA_BOOTSTRAP="${KAFKA_BOOTSTRAP:-localhost:9092}"
export KAFKA_TOPIC_PREFIX="${KAFKA_TOPIC_PREFIX:-poc.cdc}"

# flink-cdc.sh submits to the JobManager REST endpoint read from rest.address in
# $FLINK_HOME/conf/config.yaml (default 0.0.0.0, unreachable from this container).
# On Docker host-networking the JM is on localhost; on a Podman bridge it is the
# flink-jobmanager service. Override via JOBMANAGER_HOST (default localhost).
JOBMANAGER_HOST="${JOBMANAGER_HOST:-localhost}"
# Replace the address line immediately following the `rest:` key (nested 2.x YAML).
sed -i "/^rest:/{n;s/address:.*/address: ${JOBMANAGER_HOST}/}" "${FLINK_HOME}/conf/config.yaml"

envsubst < /pipeline.yaml > /tmp/pipeline-resolved.yaml
exec flink-cdc.sh /tmp/pipeline-resolved.yaml
