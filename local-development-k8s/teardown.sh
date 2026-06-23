#!/usr/bin/env bash
# Tear down the flink-cdc-poc k8s slice.
#
# By default removes only the in-cluster resources + operator releases, keeping
# the kind cluster (so re-running deploy.sh is fast). Pass --full to also delete
# the kind cluster (frees the node image's disk and all PVCs).
#
# Usage:
#   ./local-development-k8s/teardown.sh          # remove resources + operators
#   ./local-development-k8s/teardown.sh --full    # also delete the kind cluster
set -euo pipefail
CLUSTER=flink-cdc-poc

echo "▶ uninstalling Flink Kubernetes Operator"
helm uninstall flink-kubernetes-operator -n flink-system 2>/dev/null || true
echo "▶ uninstalling Strimzi"
helm uninstall strimzi-kafka-operator -n strimzi 2>/dev/null || true
echo "▶ deleting poc namespace (MySQL/Kafka/MinIO/FlinkDeployment + PVCs)"
kubectl delete namespace poc --ignore-not-found || true

if [ "${1:-}" = "--full" ]; then
  echo "▶ deleting kind cluster $CLUSTER"
  kind delete cluster --name "$CLUSTER" || true
else
  echo "▶ kind cluster $CLUSTER kept (re-run deploy.sh to restore; teardown.sh --full to delete)"
fi
echo "✅ done"