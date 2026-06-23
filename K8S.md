# K8S — Kubernetes deployment path (alongside Podman Compose)

This POC has **two** local deployment paths for the same 5-variant Flink CDC
demos:

| Path | Tooling | Entry point | Shape |
|------|---------|------------|-------|
| **Podman Compose** (default, fast loop) | `podman-compose` | `./gradlew all` | One shared Flink cluster; jobs submitted via REST / `flink-cdc.sh` |
| **Kubernetes** (production-shaped) | `kind` + `kubectl` + `helm` | `./local-development-k8s/deploy.sh` | **Flink Kubernetes Operator**; each job = its own `FlinkDeployment` |

The k8s path deploys all **5 Flink variants** + **5 Kafka Connect variants** +
**monitoring** (kube-prometheus-stack + Grafana dashboard + alert rules) as
Kubernetes-native resources — the production target described in
`kafka-connect-at-scale-74-connectors-migration.md`: *each Flink job runs as its
own isolated FlinkDeployment; Kafka Connect runs as a Strimzi KafkaConnect
cluster*. All 5 Flink deployments coexist on a single kind node with
non-overlapping MySQL server-ID ranges.

## Prerequisites

- `kind`, `kubectl`, `helm`, `podman` (all already present on this machine)
- The Flink Kubernetes Operator 1.15.0 (supports Flink 2.2.x) and Strimzi 1.0.1
  are installed **by `deploy.sh`** via Helm — no manual install needed.
- **CPU is the constraint, not RAM.** The single-node kind cluster has 16 CPU /
  64 GB allocatable. Each FlinkDeployment is sized at JM `cpu: 1` + TM `cpu: 1`
  (= 2 CPU/variant) so all 4 Java variants (8 CPU) + infra (~9 CPU) fit at ~57%
  CPU, leaving headroom for variant 5 + Kafka Connect + monitoring. The earlier
  2-CPU-per-JM/TM sizing starved the 4th TaskManager (Pending) — see
  `flinkdeployment-*.yaml` resource comments.
- **kind node PID cgroup limit.** The single kind node is a rootless-podman
  container that defaults to `pids_limit=2048`; the full slice sits at ~1950 PIDs
  at steady state, and the cold-start burst exceeds 2048 — which kills a
  TaskManager's containerd shim (`failed to create new OS thread … errno=11`)
  and leaves the job stuck `CREATED`. `deploy.sh` section 1b raises the limit to
  8192 via `podman update --pids-limit 8192 <kind-node>` on every run (live,
  idempotent), so no manual step is needed.

## Quick start

```bash
./local-development-k8s/deploy.sh        # build + apply + wait (~10-15 min first run)
```

Or use the Gradle wrapper (runs deploy.sh + all component tests):

```bash
./gradlew allK8s                         # deploy + port-forward + test + teardown tunnels
```

For manual host access, run each port-forward in a separate terminal:

```bash
kubectl -n poc port-forward svc/mysql                        13306:3306   # MySQL
kubectl -n poc port-forward svc/poc-kafka-kafka-external-bootstrap 19092:9094   # Kafka (external listener; advertisedHost=localhost so host consumers resolve the broker)
kubectl -n poc port-forward svc/datastream-cdc-rest          18081:8081   # Flink UI (DataStream)
kubectl -n poc port-forward svc/table-api-cdc-rest           18082:8081   # Flink UI (Table API)
kubectl -n poc port-forward svc/sql-api-cdc-rest             18083:8081   # Flink UI (SQL API)
kubectl -n poc port-forward svc/outbox-cdc-rest              18084:8081   # Flink UI (Outbox)
kubectl -n poc port-forward svc/yaml-pipeline-cdc-rest       18085:8081   # Flink UI (YAML Pipeline)
kubectl -n poc port-forward svc/poc-connect-connect-api      18086:8083   # Kafka Connect REST
kubectl -n poc port-forward svc/minio                        9001:9001    # MinIO console
kubectl -n monitoring port-forward svc/kube-prometheus-stack-grafana    13001:80     # Grafana (admin/admin)
kubectl -n monitoring port-forward svc/kube-prometheus-stack-prometheus 19090:9090   # Prometheus
```

The k8s stack **coexists with the running Podman stack** — it binds no host
ports (host access is via `kubectl port-forward` to non-conflicting high ports).
You do **not** need to stop the Podman stack for deploy.sh.

> **For `./gradlew allK8s`**, stop the Podman stack first — the test runner
> port-forwards the same high ports and if the Podman stack is also up there
> can be address conflicts.

## Verify end-to-end

### Automated (recommended)

```bash
./gradlew allK8s
```

Runs deploy.sh, opens port-forwards for all 8 services, executes each of the 5
Flink variant test classes (each targeting its own JM via `FLINK_REST_URL`) and
all 5 Kafka Connect test classes, then tears down the tunnels.

### Manual smoke test

With the port-forwards above running:

```bash
# 1. insert a marker row (via the MySQL port-forward)
mysql -h 127.0.0.1 -P 13306 -u flink -pflink poc_db \
  -e "INSERT INTO orders (customer_id, amount, status) VALUES (99, 99.99, 'K8S-E2E-marker')"

# 2. read it from the Kafka topic the DataStream job writes to
kubectl -n poc exec -it poc-kafka-dual-role-0 -- \
  /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 --topic poc.cdc.datastream.flink \
  --from-beginning --timeout-ms 85000 | grep K8S-E2E-marker
```

The row appears once the next checkpoint commits (≤ 30 s checkpoint interval).

### Per-variant component tests

Run a single variant's test with its port-forwarded JM:

```bash
# DataStream
FLINK_REST_URL=http://localhost:18081 MYSQL_PORT=13306 KAFKA_BOOTSTRAP=localhost:19092 \
  ./gradlew :component-tests:test --tests 'poc.component.DataStreamCdcTest'

# YAML Pipeline
FLINK_REST_URL=http://localhost:18085 MYSQL_PORT=13306 KAFKA_BOOTSTRAP=localhost:19092 \
  ./gradlew :component-tests:test --tests 'poc.component.YamlPipelineCdcTest'

# Kafka Connect (all KC variants)
KAFKA_CONNECT_URL=http://localhost:18086 \
  SCHEMA_HISTORY_KAFKA_BOOTSTRAP=poc-kafka-kafka-bootstrap:9092 \
  MYSQL_PORT=13306 KAFKA_BOOTSTRAP=localhost:19092 \
  ./gradlew :component-tests:test --tests 'poc.component.KafkaConnect*'
```

| Env var | Default | Purpose |
|---------|---------|---------|
| `FLINK_REST_URL` | `http://localhost:8081` | Flink JM REST endpoint for `FlinkRestClient` |
| `KAFKA_CONNECT_URL` | `http://localhost:8083` | Kafka Connect REST endpoint for `KafkaConnectBase` |
| `SCHEMA_HISTORY_KAFKA_BOOTSTRAP` | `kafka:29092` | Bootstrap address used by Debezium connectors inside KC for schema history; set to `poc-kafka-kafka-bootstrap:9092` for k8s |

## What's deployed

| Component | k8s artifact | Notes |
|-----------|--------------|-------|
| Namespace | `k8s/namespace.yaml` → `poc` | Isolates the slice; `kubectl delete ns poc` cleans up |
| Kafka | `k8s/kafka/kafka.yaml` (Strimzi `Kafka` CR) | Single-broker KRaft 4.2.0, txn RF=1 / minISR=1 (preserves exactly-once). Two listeners: `plain` (internal, 9092, in-cluster FQDN) for Flink/KC jobs, and `external` (nodeport, 9094, `advertisedHostTemplate: localhost` / `advertisedPortTemplate: 19092`) so host-side test consumers can resolve the broker the metadata handshake returns |
| MySQL | `k8s/mysql/mysql.yaml` | `mysql:8.0`, binlog ROW/FULL, same `init.sql` schema as Podman |
| MinIO | `k8s/minio/minio.yaml` | S3 API `minio:9000`, bucket `flink-checkpoints` created by a one-shot Job |
| Flink Operator | Helm `flink-kubernetes-operator` (ns `flink-system`) | 1.15.0, webhook disabled |
| Strimzi Operator | Helm `strimzi-kafka-operator` (ns `strimzi`) | 1.0.1, `watchAnyNamespace=true` |
| Flink RBAC | `k8s/flink/flink-rbac.yaml` | `flink` ServiceAccount + Role; shared by all 5 variants |
| Base image | `flink-with-mysql/Dockerfile` → `flink-with-mysql:latest` | Flink 2.2 + mysql-connector-j + flink-s3-fs-presto + flink-metrics-prometheus; variant-agnostic runtime |
| Artifact images | `k8s/flink/images/{datastream,table-api,sql-api,outbox}/Dockerfile` | `FROM busybox:1.35` + jar; one per Java variant; init-container copies jar to emptyDir |
| DataStream job | `k8s/flink/flinkdeployment-datastream.yaml` | server-id 5900-5999; topic `poc.cdc.datastream.flink` |
| Table API job | `k8s/flink/flinkdeployment-table-api.yaml` | server-id 6000-6099 (`MYSQL_TABLE_API_SERVER_ID`); topic `poc.cdc.table-api.flink` |
| SQL API job | `k8s/flink/flinkdeployment-sql-api.yaml` | Two sources → 5800-5849 + 5850-5899; topics `poc.cdc.sql-api.flink.orders` + `.customers` |
| Outbox job | `k8s/flink/flinkdeployment-outbox.yaml` | server-id 5600-5699 (`MYSQL_OUTBOX_SERVER_ID`); topic `poc.cdc.outbox.flink` |
| YAML Pipeline (v5) | `k8s/flink/flinkdeployment-yaml.yaml` (session-cluster, `mode: standalone`) + `job-yaml-submitter.yaml` (Job) | Session-cluster has no `spec.job`; `mode: standalone` makes the operator pre-deploy the `taskManager.replicas` TM (native mode ignores it → no TM until a job is submitted → submitter deadlock); one-shot Job runs `flink-cdc.sh pipeline.yaml` via the `flink-cdc-submitter` image; topic `poc.cdc.yaml.flink.orders`; server-id 5700-5709 |
| Kafka Connect | `k8s/kafka-connect/kafka-connect.yaml` (`KafkaConnect` CR + 5 `KafkaConnector` CRs) | Strimzi v1 (`kafka.strimzi.io/v1`); `groupId`/`offsetStorageTopic`/`configStorageTopic`/`statusStorageTopic` are required top-level spec fields; Debezium 3.0.2.Final (Kafka 4.x compatible); custom SMTs baked into `kafka-connect-debezium:local` image |
| Monitoring | Helm `kube-prometheus-stack` (ns `monitoring`) + `pod-monitor.yaml` + `prometheus-rules.yaml` + `grafana-dashboard-cm.yaml` | Grafana at localhost:13001 (admin/admin); Prometheus at localhost:19090; 3 alert rules mirroring Terraform `alerts.tf` |

## How the fat-jar is mounted (init-container + volume)

The variant fat-jar is **not** baked into the Flink image. The FlinkDeployment
uses the official Flink Operator artifact pattern (upstream
`examples/pod-template.yaml`):

1. `spec.image` = `flink-with-mysql:latest` — the generic JM/TM runtime, same
   for every variant.
2. The top-level `podTemplate` declares an emptyDir volume `flink-usrlib` and
   mounts it at `/opt/flink/usrlib` on the main container (which **must** be
   named `flink-main-container` — the operator's main container; any other name
   becomes an imageless sidecar and the JM Deployment is rejected with
   `containers[0].image: Required value`).
3. `jobManager.podTemplate.spec.initContainers[0]` (`image:
   flink-cdc-artifact-datastream:latest`) copies the jar from `/artifacts` into
   the emptyDir before the JM starts.
4. `job.jarURI: local:///opt/flink/usrlib/datastream-job.jar` runs it. In
   native mode the JM ships the jar to TMs via Flink's blob store, so the
   init-container runs on the JM only (the TM emptyDir stays empty — that is
   fine).

This is identical for all 4 Java fat-jar variants (DataStream, Table API, SQL
API, Outbox): only the artifact image name, `jarURI`, `entryClass`, server-ID,
and Kafka topic differ. **Variant 5 (YAML Pipeline) is zero-code** — no fat-jar,
no entry class, submitted via `flink-cdc.sh pipeline.yaml` — so it does NOT fit
this pattern. It needs a session-cluster `FlinkDeployment` (no `spec.job`) + a
one-shot `flink-cdc-submitter` Job running `flink-cdc.sh` against the session
cluster's REST endpoint (expand phase; the `flink-cdc-submitter` image already
exists in `local-development-podman/`).

## How the checkpoint config maps

In Podman, the YAML pipeline job needed `flink-cdc.sh -D` flags for
checkpoint/state config (the Pipeline API ignores `pipeline:`-section keys). On
k8s the **FlinkDeployment `spec.flinkConfiguration`** is the equivalent and
authoritative place — the operator translates those keys into the JM/TM
execution environment. See the comment block at the top of
`flinkdeployment-datastream.yaml`. The 4 Java variants' `CheckpointConfigurer`
sets interval/mode/timeout/min-pause/retention in code, so those are not
repeated in `flinkConfiguration` (code wins); only the dir, savepoint-dir,
state backend, and S3 credentials are set there.

## Differences from the Podman stack (intentional)

- **Kafka version**: k8s runs Strimzi Kafka **4.2.0**; Podman runs cp-kafka 7.6.1
  (= Kafka 3.7.0). Flink's Kafka client is broker-version compatible. The k8s
  Kafka Connect variants use **Debezium 3.0.2.Final** (the Kafka 4.x-compatible
  release; Debezium 2.x was Kafka 3.x only).
- **No shared Flink cluster**: each `FlinkDeployment` is its own JM+TM (the
  production model), unlike Podman's one shared JM+TM.
- **YAML Pipeline uses session-cluster + submitter Job**: the YAML Pipeline
  variant on k8s is a session-cluster `FlinkDeployment` (no `spec.job`) + a
  one-shot Kubernetes Job running `flink-cdc.sh pipeline.yaml` against the
  session-cluster's REST endpoint. Podman uses a long-lived `flink-cdc-submitter`
  container; k8s uses a `restartPolicy: OnFailure` Job that exits after
  submission.
- **Kafka Connect image**: k8s builds `kafka-connect-debezium:local` from the
  Strimzi base image (`quay.io/strimzi/kafka:1.0.1-kafka-4.2.0`) with Debezium
  3.0.2.Final and the custom SMT jar baked in. Podman uses
  `cp-kafka-connect:7.6.1` with a volume-mounted SMT jar.

## Teardown

```bash
./local-development-k8s/teardown.sh          # remove resources + operators, keep kind cluster
./local-development-k8s/teardown.sh --full    # also delete the kind cluster (frees PVCs + disk)
```

## Status

All components are deployed and tested end-to-end:

- ✅ 4 Java Flink variants as `FlinkDeployment` CRs (DataStream, Table API, SQL API, Outbox)
- ✅ YAML Pipeline variant 5 — session-cluster `FlinkDeployment` + submitter Job
- ✅ 5 Kafka Connect variants — `KafkaConnect` CR + `KafkaConnector` CRs (Debezium 3.0.2.Final)
- ✅ Monitoring — kube-prometheus-stack + PodMonitor + PrometheusRule + Grafana dashboard ConfigMap
- ✅ `./gradlew allK8s` — deploy + per-variant port-forward + test + teardown