#!/usr/bin/env bash
# Deploy the flink-cdc-poc k8s stack to a local kind cluster.
#
# Brings up: kind cluster → 4 Java variant fat-jars (datastream, table-api,
# sql-api, outbox) → flink-with-mysql base image + 4 per-variant artifact
# images (podman build, all loaded into kind) → poc namespace → Strimzi Kafka
# → MySQL → MinIO + bucket-init → Flink Kubernetes Operator → 4 FlinkDeployment
# CRs (one per Java variant). Variant-5 (YAML pipeline) is zero-code and uses a
# distinct session-cluster + submitter approach (see
# flinkdeployment-yaml.yaml + the submitter Job — expand phase).
#
# Artifact strategy: the variant fat-jar is NOT baked into the Flink image. Each
# FlinkDeployment's init-container copies it from the per-variant artifact image
# (flink-cdc-artifact-<variant>, a busybox image holding only the jar) into an
# emptyDir mounted at /opt/flink/usrlib — the official Flink Operator artifact
# pattern (upstream examples/pod-template.yaml). flink-with-mysql stays generic
# and is the JM/TM runtime image (spec.image), so it AND every artifact image
# must be loaded into kind.
#
# Idempotent: safe to re-run. Coexists with the running Podman Compose stack
# (no host ports are bound — host access is via kubectl port-forward).
#
# Usage: ./local-development-k8s/deploy.sh
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"   # repo root
K8S="$ROOT/local-development-k8s"
CLUSTER=flink-cdc-poc
cd "$ROOT"

# The 4 Java fat-jar variants deployed as FlinkDeployment CRs. Variant-5 (YAML
# pipeline) is zero-code (no fat-jar) and uses a distinct session-cluster +
# submitter approach — handled separately, not in these arrays. The parallel
# arrays map each variant's: gradle module, artifact image name, fat-jar name
# inside the artifact image, image build subdir, FlinkDeployment manifest, and
# FlinkDeployment resource name.
VARIANT_MODULES=(variant-flink-datastream-api-v1-cdc-job variant-flink-table-api-cdc-job variant-flink-sql-api-cdc-job variant-flink-datastream-api-v1-outbox-job)
VARIANT_ARTIFACTS=(flink-cdc-artifact-datastream flink-cdc-artifact-table-api flink-cdc-artifact-sql-api flink-cdc-artifact-outbox)
VARIANT_JARNAMES=(datastream-job.jar table-api-job.jar sql-api-job.jar outbox-job.jar)
VARIANT_IMGDIRS=(datastream table-api sql-api outbox)
VARIANT_DEPLOYMENTS=(flinkdeployment-datastream.yaml flinkdeployment-table-api.yaml flinkdeployment-sql-api.yaml flinkdeployment-outbox.yaml)
VARIANT_NAMES=(datastream-cdc table-api-cdc sql-api-cdc outbox-cdc)

# ── 1. kind cluster ────────────────────────────────────────────────────────
if ! kind get clusters 2>/dev/null | grep -qx "$CLUSTER"; then
  echo "▶ creating kind cluster $CLUSTER"
  kind create cluster --config "$K8S/cluster/kind-config.yaml"
fi
kubectl config use-context "kind-$CLUSTER"

# ── 1b. raise the kind node container's PID cgroup limit ───────────────────
# The single kind node is a rootless-podman container that defaults to
# pids_limit=2048. The full slice (5 Flink JM+TM pairs + Kafka + Kafka Connect
# + MySQL + MinIO + kube-prometheus-stack) sits at ~1950 PIDs at steady state,
# and the simultaneous cold-start burst during deploy pushes past 2048. When
# that happens a FlinkDeployment TaskManager's containerd shim cannot fork an
# OS thread ("failed to create new OS thread (have 6 already; errno=11)" =
# EAGAIN from clone()) → the TM pod goes Error → the job logs
# "NoResourceAvailableException: Slot request bulk is not fulfillable!" and
# stays CREATED → `kubectl wait flinkdeployment/<v> --for=Running` times out
# at 600s and deploy.sh aborts. Raising the node's PID cgroup limit to 8192
# gives ~6k headroom so all pods can start concurrently. `podman update` is
# live (takes effect on the running container's cgroup immediately) and
# idempotent, so re-running deploy.sh is cheap.
NODE_CONTAINER="${CLUSTER}-control-plane"
echo "▶ ensuring kind node pids_limit ≥ 8192 (avoid containerd-shim thread EAGAIN under cold-start burst)"
podman update --pids-limit 8192 "$NODE_CONTAINER" >/dev/null || \
  echo "⚠ could not raise pids_limit on $NODE_CONTAINER (continuing — may hit thread EAGAIN under burst)"

# ── 2. build the 4 Java fat-jars + Kafka Connect SMT ───────────────────────
# Variant-5 (YAML pipeline) is zero-code (no fat-jar) — handled separately.
echo "▶ building 4 Java variant fat-jars (datastream, table-api, sql-api, outbox)"
for mod in "${VARIANT_MODULES[@]}"; do
  ./gradlew ":${mod}:shadowJar"
done
# SMT fat-jar (with-deps) is needed before building the kafka-connect-debezium
# image below; the Dockerfile COPYs it from this path.
echo "▶ building Kafka Connect SMT jar"
./gradlew :kafka-connect-smts:shadowJar

# ── 3. build images (podman) ───────────────────────────────────────────────
# flink-with-mysql is the generic JM/TM runtime image (FlinkDeployment spec.image),
# variant-agnostic. The 4 per-variant artifact images are tiny busybox images
# holding only the fat-jar; each FlinkDeployment's init-container copies its jar
# into an emptyDir at /opt/flink/usrlib (no jar baked into the Flink image).
echo "▶ building flink-with-mysql base image"
podman build -t flink-with-mysql:latest "$ROOT/local-development-podman/flink-with-mysql"

for i in "${!VARIANT_MODULES[@]}"; do
  mod="${VARIANT_MODULES[$i]}"
  artifact="${VARIANT_ARTIFACTS[$i]}"
  imgdir="${VARIANT_IMGDIRS[$i]}"
  JAR="$(ls -t "${mod}/build/libs"/*-all.jar 2>/dev/null | head -1)"
  if [ -z "$JAR" ]; then
    echo "✗ ${mod} fat-jar not found in ${mod}/build/libs/"; exit 1
  fi
  echo "▶ ${artifact}: using fat-jar $JAR"
  cp "$JAR" "$K8S/flink/images/${imgdir}/job.jar"
  podman build -t "${artifact}:latest" "$K8S/flink/images/${imgdir}"
done

# Variant-5 YAML pipeline submitter image — built from the shared Podman Compose
# Dockerfile (local-development/flink-cdc-submitter/). Downloads Flink CDC 3.6.0
# at build time; subsequent builds are cheap (layer cache).
echo "▶ building flink-cdc-submitter image (YAML pipeline submitter)"
podman build -t flink-cdc-submitter:latest "$ROOT/local-development-podman/flink-cdc-submitter"

# Kafka Connect image: Strimzi base (Kafka 4.2.0) + Debezium 3.5.2.Final + SMT.
# Build context is repo root so the COPY of the SMT jar resolves.
# Tag :local (not :latest) → kubelet defaults imagePullPolicy=IfNotPresent →
# uses the kind-loaded image without a registry.
# --no-cache: the Debezium download step (RUN curl) is expensive to re-run but
# must not be cached across version bumps — a cached layer from 3.0.x would
# silently survive a Dockerfile version upgrade and cause NoSuchMethodError at
# runtime (poll(long) removed in Kafka 4.0).
echo "▶ building kafka-connect-debezium:local image (Debezium 3.5.x + SMT)"
podman build --no-cache -t kafka-connect-debezium:local \
  -f "$K8S/kafka-connect/Dockerfile" \
  "$ROOT"

# ── 4. load all images into kind ──────────────────────────────────────────
# flink-with-mysql (spec.image) + the 4 artifact images (init-container images)
# are all referenced with imagePullPolicy: Never, so all must be present in the
# kind node.
echo "▶ loading flink-with-mysql + 4 artifact images + flink-cdc-submitter + kafka-connect into kind"
# Podman tags locally-built images with a `localhost/` prefix; `kind load` must
# use the same qualified name or it reports "not present locally".
kind load docker-image localhost/flink-with-mysql:latest --name "$CLUSTER"
for artifact in "${VARIANT_ARTIFACTS[@]}"; do
  kind load docker-image "localhost/${artifact}:latest" --name "$CLUSTER"
done
kind load docker-image localhost/flink-cdc-submitter:latest --name "$CLUSTER"
kind load docker-image localhost/kafka-connect-debezium:local --name "$CLUSTER"

# kind + podman provider quirk: `kind load` stores images in the node under the
# `localhost/<name>:latest` tag (podman's default registry prefix) and its re-tag
# to the unqualified name fails with "failed to re-tag image on the node ... Will
# load it instead". The FlinkDeployments reference the unqualified names
# (flink-with-mysql:latest / flink-cdc-artifact-<variant>:latest), and with
# imagePullPolicy: Never kubelet resolves by exact ref — so a pod's init-container
# fails with ErrImageNeverPull. Re-tag inside the node's containerd (k8s.io
# namespace) to both the unqualified and the docker.io/library normalized form
# (the latter is what kubelet's CRI image service resolves to).
# `podman exec` into the kind node container to run ctr commands directly.
NODE="${CLUSTER}-control-plane"
echo "▶ re-tagging images in kind node containerd (kind+podman localhost/ prefix quirk)"
for img in flink-with-mysql "${VARIANT_ARTIFACTS[@]}" flink-cdc-submitter; do
  podman exec "$NODE" ctr -n k8s.io images tag "localhost/${img}:latest" "${img}:latest" >/dev/null \
    || echo "⚠ re-tag localhost/${img}:latest → ${img}:latest failed (may already exist)"
  podman exec "$NODE" ctr -n k8s.io images tag "localhost/${img}:latest" "docker.io/library/${img}:latest" >/dev/null \
    || echo "⚠ re-tag localhost/${img}:latest → docker.io/library/${img}:latest failed (may already exist)"
done
# kafka-connect-debezium uses tag :local (not :latest) so it's handled separately.
podman exec "$NODE" ctr -n k8s.io images tag \
  "localhost/kafka-connect-debezium:local" "kafka-connect-debezium:local" >/dev/null \
  || echo "⚠ re-tag localhost/kafka-connect-debezium:local → kafka-connect-debezium:local failed (may already exist)"
podman exec "$NODE" ctr -n k8s.io images tag \
  "localhost/kafka-connect-debezium:local" "docker.io/library/kafka-connect-debezium:local" >/dev/null \
  || echo "⚠ re-tag localhost/kafka-connect-debezium:local → docker.io/library/kafka-connect-debezium:local failed (may already exist)"

# ── 5. namespace ────────────────────────────────────────────────────────────
kubectl apply -f "$K8S/namespace.yaml"

# ── 6. Strimzi operator + Kafka CR ─────────────────────────────────────────
echo "▶ installing Strimzi 1.0.1"
helm repo add strimzi https://strimzi.io/charts/ 2>/dev/null || true
helm repo update >/dev/null
helm upgrade --install strimzi-kafka-operator strimzi/strimzi-kafka-operator \
  --version 1.0.1 -n strimzi --create-namespace --set watchAnyNamespace=true

kubectl apply -f "$K8S/kafka/kafka.yaml"
echo "▶ waiting for Kafka to become Ready (can take a few minutes on first pull)..."
kubectl -n poc wait kafka/poc-kafka --for=condition=Ready --timeout=900s

# ── 7. MySQL + MinIO ────────────────────────────────────────────────────────
kubectl apply -f "$K8S/mysql/mysql.yaml"
kubectl apply -f "$K8S/minio/minio.yaml"
echo "▶ waiting for MySQL + MinIO..."
kubectl -n poc wait --for=condition=ready pod -l app=mysql --timeout=300s
kubectl -n poc wait --for=condition=ready pod -l app=minio --timeout=300s
kubectl -n poc wait --for=condition=complete job/minio-init --timeout=180s || \
  echo "⚠ minio-init not complete yet (bucket may already exist) — continuing"

# ── 8. Flink Kubernetes Operator + FlinkDeployment ─────────────────────────
echo "▶ installing Flink Kubernetes Operator 1.15.0"
helm repo add flink-operator https://archive.apache.org/dist/flink/flink-kubernetes-operator-1.15.0/ 2>/dev/null || true
helm repo update >/dev/null
helm upgrade --install flink-kubernetes-operator flink-operator/flink-kubernetes-operator \
  -n flink-system --create-namespace --set webhook.create=false
kubectl -n flink-system wait deploy/flink-kubernetes-operator \
  --for=condition=available --timeout=300s

kubectl apply -f "$K8S/flink/flink-rbac.yaml"
for dep in "${VARIANT_DEPLOYMENTS[@]}"; do
  kubectl apply -f "$K8S/flink/${dep}"
done
echo "▶ waiting for the 4 FlinkDeployments to reach Running..."
for name in "${VARIANT_NAMES[@]}"; do
  kubectl -n poc wait "flinkdeployment/${name}" --for=condition=Running --timeout=600s
done

# ── 9. YAML Pipeline variant (session cluster + submitter) ──────────────────
# Session cluster: no spec.job; spec.mode: standalone so the operator pre-deploys
# the JM + the taskManager.replicas TM pod (native mode would ignore replicas and
# deploy no TM, deadlocking the submitter which waits for a TM before submitting)
# and the yaml-pipeline-cdc-rest ClusterIP Service (port 8081).
echo "▶ deploying YAML Pipeline session cluster (variant-5)"
kubectl apply -f "$K8S/flink/configmap-yaml-pipeline.yaml"
kubectl apply -f "$K8S/flink/flinkdeployment-yaml.yaml"

# Wait for the JM to become available before launching the submitter. The
# Flink operator creates the JM Deployment a few seconds after the
# FlinkDeployment CR is applied; `rollout status deployment/...` races with
# that creation and returns NotFound if it runs first. Waiting on the CR's
# Running condition instead is race-free — the CR exists immediately after
# `apply`, and the operator sets Running once JobManagerReady (same pattern
# used for the 4 Java variants above).
echo "▶ waiting for yaml-pipeline-cdc JobManager to become available..."
kubectl -n poc wait flinkdeployment/yaml-pipeline-cdc --for=condition=Running --timeout=300s

# Delete any previous submitter Job so the apply is idempotent (Job names are
# unique; re-applying an existing completed Job is a no-op for kubectl but the
# pod may be gone, making `wait --for=complete` hang). Replace instead.
kubectl -n poc delete job yaml-pipeline-submitter --ignore-not-found=true
kubectl apply -f "$K8S/flink/job-yaml-submitter.yaml"

echo "▶ waiting for yaml-pipeline-submitter to complete (submits pipeline to session cluster)..."
kubectl -n poc wait job/yaml-pipeline-submitter --for=condition=complete --timeout=180s || \
  echo "⚠ submitter not yet complete — check: kubectl -n poc logs job/yaml-pipeline-submitter"

# ── 10. Kafka Connect (KafkaConnect CR + 5 KafkaConnector CRs) ──────────────
# The KafkaConnect CR starts a Connect worker pod; the Strimzi operator then
# reconciles the 5 KafkaConnector CRs by POSTing each config to the Connect
# REST API. Connector state reaches RUNNING when Debezium begins snapshotting MySQL.
echo "▶ deploying KafkaConnect cluster + 5 Debezium connectors"
kubectl apply -f "$K8S/kafka-connect/kafka-connect.yaml"

echo "▶ waiting for KafkaConnect poc-connect to become Ready..."
kubectl -n poc wait kafkaconnect/poc-connect --for=condition=Ready --timeout=300s

echo "▶ waiting for all 5 KafkaConnectors to reach Running state..."
for connector in kc-datastream-cdc kc-table-api-cdc kc-sql-api-cdc kc-yaml-pipeline-cdc kc-outbox-cdc; do
  kubectl -n poc wait kafkaconnector/${connector} --for=condition=Ready --timeout=120s || \
    echo "⚠ ${connector} not Ready yet — check: kubectl -n poc describe kafkaconnector/${connector}"
done

# ── 11. Monitoring (kube-prometheus-stack + PodMonitor + PrometheusRule + dashboard) ─
# Installs a minimal Prometheus + Grafana stack (Alertmanager + infra components
# disabled to save kind node resources). The PodMonitor in poc namespace scrapes
# all Flink JM/TM pods on port 9249; the PrometheusRule defines the 3 alert rules
# mirroring the Podman Terraform alerts; the ConfigMap auto-imports the dashboard.
echo "▶ installing kube-prometheus-stack (Prometheus + Grafana, no Alertmanager)"
helm repo add prometheus-community https://prometheus-community.github.io/helm-charts 2>/dev/null || true
helm repo update >/dev/null
helm upgrade --install kube-prometheus-stack prometheus-community/kube-prometheus-stack \
  -n monitoring --create-namespace \
  -f "$K8S/monitoring/kube-prometheus-values.yaml"

kubectl apply -f "$K8S/monitoring/grafana-dashboard-cm.yaml"
kubectl apply -f "$K8S/monitoring/pod-monitor.yaml"
kubectl apply -f "$K8S/monitoring/prometheus-rules.yaml"

echo "▶ waiting for Grafana to become available..."
kubectl -n monitoring wait deploy/kube-prometheus-stack-grafana \
  --for=condition=available --timeout=300s

# ── 12. host access ─────────────────────────────────────────────────────────
cat <<EOF

✅ flink-cdc-poc k8s stack is up (5 Flink + 5 Kafka Connect variants + monitoring). In separate terminals:

  kubectl -n poc port-forward svc/mysql                        13306:3306   # MySQL
  kubectl -n poc port-forward svc/poc-kafka-kafka-external-bootstrap 19092:9094 # Kafka (host-access listener; advertisedHost=localhost)
  kubectl -n poc port-forward svc/datastream-cdc-rest          18081:8081   # Flink UI (DataStream)
  kubectl -n poc port-forward svc/table-api-cdc-rest           18082:8081   # Flink UI (Table API)
  kubectl -n poc port-forward svc/sql-api-cdc-rest             18083:8081   # Flink UI (SQL API)
  kubectl -n poc port-forward svc/outbox-cdc-rest              18084:8081   # Flink UI (Outbox)
  kubectl -n poc port-forward svc/yaml-pipeline-cdc-rest       18085:8081   # Flink UI (YAML Pipeline)
  kubectl -n poc port-forward svc/poc-connect-connect-api                 18086:8083   # Kafka Connect REST
  kubectl -n poc port-forward svc/minio                                   9001:9001    # MinIO console
  kubectl -n monitoring port-forward svc/kube-prometheus-stack-grafana    13001:80     # Grafana (admin/admin)
  kubectl -n monitoring port-forward svc/kube-prometheus-stack-prometheus 19090:9090   # Prometheus

Grafana dashboard: http://localhost:13001/d/flink-cdc-poc-monitoring
Prometheus alerts: http://localhost:19090/alerts

Then verify end-to-end (see K8S.md):
  FLINK_REST_URL=http://localhost:18081 MYSQL_PORT=13306 KAFKA_BOOTSTRAP=localhost:19092 ./gradlew :component-tests:test --tests 'poc.component.DataStreamCdcTest'
  FLINK_REST_URL=http://localhost:18085 MYSQL_PORT=13306 KAFKA_BOOTSTRAP=localhost:19092 ./gradlew :component-tests:test --tests 'poc.component.YamlPipelineCdcTest'
  KAFKA_CONNECT_URL=http://localhost:18086 SCHEMA_HISTORY_KAFKA_BOOTSTRAP=poc-kafka-kafka-bootstrap:9092 MYSQL_PORT=13306 KAFKA_BOOTSTRAP=localhost:19092 ./gradlew :component-tests:test --tests 'poc.component.KafkaConnectDataStreamTest'
EOF