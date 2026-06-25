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

# All Flink variants: 4 Java (DataStream/Table API/SQL API/Outbox) + 1 YAML
# pipeline session cluster. Kept in sync with deploy.sh's VARIANT_NAMES + the
# yaml-pipeline-cdc deployment it applies in §9.
VARIANT_NAMES=(datastream-cdc table-api-cdc sql-api-cdc outbox-cdc yaml-pipeline-cdc)

# delete_flinkdeployment: delete with --wait=false, then bound-wait for the
# deletion. Mirrors deploy.sh: if the operator's finalizer
# (flinkdeployments.flink.apache.org/finalizer) doesn't clear within 120s —
# which happens when the operator's kubernetes-client watch is wedged —
# force-patch the finalizer off so the object is GC'd. In teardown the
# force-patch path is harmless: we're removing everything, so the operator's
# stale reconcile has nothing left to destroy (unlike deploy.sh, where it
# would wreck a freshly-recreated JM — see the §9 note there).
delete_flinkdeployment() {  # name
  local name="$1"
  kubectl -n poc delete flinkdeployment "${name}" --ignore-not-found=true --wait=false
  if kubectl -n poc wait flinkdeployment "${name}" --for=delete --timeout=120s 2>/dev/null; then
    return 0
  fi
  echo "⚠ ${name}: finalizer did not clear in 120s — force-patching finalizer and retrying"
  kubectl -n poc patch flinkdeployment "${name}" --type=merge -p '{"metadata":{"finalizers":[]}}' 2>/dev/null || true
  kubectl -n poc wait flinkdeployment "${name}" --for=delete --timeout=60s 2>/dev/null || true
}

# Delete FlinkDeployments BEFORE uninstalling the operator. Once the operator
# is gone, nothing clears the FlinkDeployment finalizers, so a subsequent
# `kubectl delete namespace poc` hangs in Terminating on those stuck
# finalizers. Deleting while the operator is still alive lets it reconcile the
# deletes normally; the helper force-patches any finalizer the (possibly wedged)
# operator fails to clear.
echo "▶ deleting FlinkDeployments (finalizers cleared by the still-running operator)"
for name in "${VARIANT_NAMES[@]}"; do
  delete_flinkdeployment "${name}"
done

echo "▶ uninstalling Flink Kubernetes Operator"
helm uninstall flink-kubernetes-operator -n flink-system 2>/dev/null || true
echo "▶ uninstalling Strimzi"
helm uninstall strimzi-kafka-operator -n strimzi 2>/dev/null || true
echo "▶ deleting poc namespace (MySQL/Kafka/MinIO/PVCs — FlinkDeployments already gone)"
kubectl delete namespace poc --ignore-not-found || true

if [ "${1:-}" = "--full" ]; then
  echo "▶ deleting kind cluster $CLUSTER"
  kind delete cluster --name "$CLUSTER" || true
else
  echo "▶ kind cluster $CLUSTER kept (re-run deploy.sh to restore; teardown.sh --full to delete)"
fi
echo "✅ done"