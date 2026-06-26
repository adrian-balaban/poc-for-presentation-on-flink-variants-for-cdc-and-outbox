# Kafka Connect at Scale — Appendix (Backup Q&A Slides)

> Reference material for Q&A — not part of the 45-minute presentation.
> Main file: `kafka-connect-at-scale-74-connectors-migration.md`.

---

## APPENDIX — Backup Slides (Not Part of the 45-Minute Presentation)

> The three lists below are reference material for Q&A only. Do not present them live —
> they are here so you can jump to a specific table if asked a detailed infrastructure question.

---

### Local Monitoring Endpoints

The Podman stack binds ports directly on the host; the k8s stack binds no ports on the host — access is via `kubectl port-forward` to large, non-conflicting ports (see [`K8S.md`](./K8S.md)). Both stacks run the same 5 Flink jobs and Kafka Connect connectors; the difference is the deployment unit (Podman: a shared JM; k8s: one JM per variant, Application Mode).

| Service | Podman URL | k8s URL (port-forward) | Screenshot |
|---------|------------|------------------------|------------|
| Flink Dashboard | `http://localhost:8081` (shared JM; all 5 jobs) | `http://localhost:18081`–`18085` (JM per variant: DataStream / Table API / SQL API / Outbox / YAML) | ![](images/slides/flink-dashboard.png) |
| Kafka UI | `http://localhost:8080` | — (not deployed in the k8s slice) | ![](images/slides/kafka-ui.png) |
| Kafka Connect REST | `http://localhost:8083` | `http://localhost:18086` | ![](images/slides/kafka-connect.png) |
| MySQL | `localhost:3306` (user: `flink`, password: `flink`, db: `poc_db`) | `localhost:13306` | — |
| Kafka (external) | `localhost:9092` (topics: `poc.flink.*` for Flink, `poc.kc.*` for Kafka Connect) | `localhost:19092` (Strimzi external nodeport listener; advertisedHost=localhost) | — |
| Prometheus | `http://localhost:9090` | `http://localhost:19090` | — |
| Grafana | `http://localhost:3001` (dashboard + alerts; user: `admin`, password: `admin`) | `http://localhost:13001` (user: `admin`, password: `admin`) | — |
| MinIO | `http://localhost:9001` (user: `minioadmin`, password: `minioadmin`, bucket: `flink-checkpoints`) | `http://localhost:9001` (user: `minioadmin`, password: `minioadmin`, bucket: `flink-checkpoints`) | ![](images/slides/minio-checkpoints.png) |


---

## Detailed Reference — POC Module Structure

### POC Module Structure (`flink-cdc-poc`)

```
flink-cdc-poc/
├── common/                             # JobConfig, CheckpointConfigurer, PocJsonDeserializationSchema, CdcEventRouter, OutboxRouter, KafkaSinkFactory, DdlValidator (~412 lines)
├── variant-flink-datastream-api-v1-cdc-job/   # DataStreamCdcJob.java  (63 lines, server-ID 5900–5999)
├── variant-flink-table-api-cdc-job/           # TableApiCdcJob.java    (99 lines, server-ID 6000–6099)
├── variant-flink-sql-api-cdc-job/             # SqlApiCdcJob.java      (156 lines, server-ID 5800–5899)
├── variant-flink-datastream-api-v1-outbox-job/ # OutboxJob.java        (56 lines, server-ID 5600–5699)
├── variant-flink-cdc-yaml-pipeline-cdc-job/   # pipeline.yaml         (52 lines, canonical: src/main/resources/pipeline.yaml, server-ID 5700–5709)
├── component-tests/                    # 16 test classes + 5 base helper classes:
│                                       #   Flink variants: DataStreamCdcTest, TableApiCdcTest, SqlApiCdcTest,
│                                       #     DataStreamOutboxTest, YamlPipelineCdcTest
│                                       #   KC: KafkaConnectVariantTest, KafkaConnectOutboxTest
│                                       #   invariants/quality: CdcOperationsTest, CdcParityTest, DataQualityTest,
│                                       #     DataStreamCdcMiniClusterTest, OutboxRouterMiniClusterTest,
│                                       #     ErrorScenarioTest, ExactlyOnceInvariantTest, JobHealthTest,
│                                       #     SchemaEvolutionTest
├── local-development-podman/           # Podman Compose stack
│   ├── podman-compose.yml              # MySQL + Kafka + Flink JM/TM + KC + kafka-ui + flink-cdc-submitter
│   ├── flink-with-mysql/Dockerfile     # Flink 2.2 + mysql-connector-j
│   ├── flink-cdc-submitter/            # runs flink-cdc.sh for the YAML Pipeline variant
│   ├── kafka-connect/                  # Debezium + custom SMTs; 5 connector JSON configs
│   └── kafka-connect-smts/             # EnrichmentTransform + OutboxRoutingTransform (Java 11)
└── local-development-k8s/              # Kubernetes stack (kind + Flink Operator + Strimzi)
    ├── deploy.sh / teardown.sh
    └── flink/  kafka/  kafka-connect/  mysql/  minio/  monitoring/
```

## Backup — Checkpoint Configuration (production-ready)

All five variants share a single extraction point — `CheckpointConfigurer.applyExactlyOnce(env)` —
instead of repeating the five calls below in each entry class:

```java
// common/src/main/java/poc/common/checkpoint/CheckpointConfigurer.java
public static void applyExactlyOnce(StreamExecutionEnvironment env) {
    env.enableCheckpointing(30_000);
    env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
    env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
    env.getCheckpointConfig().setCheckpointTimeout(60_000);
    env.getCheckpointConfig().setMinPauseBetweenCheckpoints(5_000);
    env.getCheckpointConfig()
        .setExternalizedCheckpointRetention(
            ExternalizedCheckpointRetention.RETAIN_ON_CANCELLATION);
    // State backend (RocksDB + incremental) configured at cluster level via
    // FLINK_PROPERTIES: state.backend=rocksdb, state.backend.incremental=true
    // This keeps the job code independent of the backend choice (operational flexibility).
}
```

Checkpoint state is persisted to S3-compatible storage (MinIO locally, AWS S3 in production),
configured in `flink-conf.yaml`:

```yaml
# POC local: state.backend: rocksdb (+ incremental) set via FLINK_PROPERTIES — same as production
# Production: state.backend: rocksdb via cluster config / FLINK_PROPERTIES
state.checkpoints.dir: s3://flink-checkpoints/checkpoints
state.savepoints.dir:  s3://flink-checkpoints/savepoints
s3.endpoint: http://minio:9000
s3.path.style.access: "true"
s3.access-key: minioadmin
s3.secret-key: minioadmin
```

**POC proof — the MinIO `flink-checkpoints` bucket after running all 5 variants:**

![MinIO bucket flink-checkpoints — checkpoints and yaml-pipeline-checkpoints folders](images/slides/minio-checkpoints.png)

| Setting | Value | Reason |
|---------|-------|--------|
| `enableCheckpointing` | 30,000 ms | Balances durability vs. performance |
| `CheckpointingMode` | EXACTLY_ONCE | Prevents duplicate Kafka messages on recovery |
| `MaxConcurrentCheckpoints` | 1 | CDC jobs snapshot during checkpoint; one at a time |
| `CheckpointTimeout` | 60,000 ms | 2× the interval; gives headroom for large-state jobs under load |
| `MinPauseBetweenCheckpoints` | 5,000 ms | Prevents checkpoint storms after one completes |

![Checkpoints persisted to MinIO/S3 — `flink-checkpoints` bucket](images/slides/minio-checkpoints.png)

> Screenshot from the MinIO console: Flink checkpoints (one directory per job) are persisted to the
> `flink-checkpoints` bucket, as in production on S3 (same code config: 30 s interval, EXACTLY_ONCE).

---

## Infrastructure List 1 — Client Infrastructure (Production)

### Kubernetes

- **Flink Operator** — manages `FlinkDeployment` CRs; TaskManager slot capacity must be monitored
- **`FlinkDeployment` CRs** — one per job/variant; each with its own JobManager + TaskManager pod pair (Application Mode)
- **`FlinkStateSnapshot` CRs** — one per job, managed by the chart
- **ClusterIP Services** — `<jobName>-rest` per job, port 8081
- **Helm chart: `flink-base-chart`** — `applicationJobs` map, init-container delivery, topology spread, probes, graceful shutdown, restart strategy
- **Namespace isolation**

### Apache Flink

- **Flink 2.2 runtime** — `flink-base-image` base image (Flink Platform Team)
- **Flink CDC 3.6.0** (suffix `3.6.0-2.2`) — included in variant images; the version must match the runtime
- **Built-in plugins** — `flink-s3-fs-presto-2.2.1.jar` (versioned, must match the base image)
- **`mysql-connector-j`** — mounted in both JobManager and TaskManager; parent-first classloader pattern required (`com.mysql.`)
- **Checkpointing** — unique `checkpointing.dir` per job; exactly-once; backed by S3

### MySQL / Databases

- **MySQL binlog access** — Flink CDC reads the binlog directly; requires `log-bin`, `binlog-format=ROW`, `binlog-row-image=FULL`
- **Non-overlapping `server-id` ranges**
- **RDS IAM token rotation**
- **IRSA** — for S3 checkpoint permissions
- **MySQL privileges** — `RELOAD` + `LOCK TABLES` required for the initial snapshot

### Kafka

- **Kafka topics** (per-variant prefixes)
- **Schema history topics** (KC/Debezium)
- **Signal topic** (pre-migration only, abandoned after): `private.debezium.signal.<connector>.v1`
- **Kafka Connect (Confluent)** — retained for SFTP (20) and SingleStore (1); 74 Debezium MySQL connectors migrated to Flink
- **Heartbeat topic** — KC monitor #1; Flink equivalent: Restart Loop + TM heartbeat

### Container Registry / Images

- `flink-base-image` — Flink runtime (existing)
- Proposed new base images (Shared Job / Platform Architect):
  - `flink-cdc-base-image`
  - `flink-stream-api-base-image`
  - `flink-table-api-base-image`
  - `flink-sql-api-base-image`
- Per-variant fat-jar images: 4 Flink fat-jars + 1 KC SMT shadow JAR; Shadow plugin

### Object Storage (S3)

- Shared S3 bucket for checkpoints/savepoints
- Per-job `checkpointing.dir` paths must not overlap

### IAM / Security

- **IRSA** — S3 checkpoint access; rotation must be tested
- **RDS IAM tokens**
- **Binlog leases**

---

## Infrastructure List 2 — Local POC Infrastructure

### Software Versions

| Component | Version |
|-----------|---------|
| Apache Flink | 2.2.0 |
| Flink CDC | 3.6.0-2.2 |
| flink-connector-kafka | 5.0.0-2.2 |
| Kafka (Confluent) | KRaft mode, cp-kafka 7.8.0 (broker upgrade 7.6.1→7.8.0, CVE-2024-27309 / CVE-2024-31141) |
| MySQL | 8.0 |
| Java (Flink jobs) | 17 |
| Java (Kafka Connect SMTs) | 11 (cp-kafka-connect 7.6.1 JDK) |
| mysql-connector-j | 8.0.33 (baked into `flink-with-mysql` image) · 9.1.0 (`gradle/libs.versions.toml`, `common` + `component-tests`) |
| Gradle | 8.7 |
| Shadow plugin | 8.1.1 |

### Podman-Compose Services

| Service | Image | Port(s) | Role |
|---------|-------|---------|------|
| `mysql` | `mysql:8.0` | 3306 | CDC source; `log-bin`, `binlog-format=ROW`, `binlog-row-image=FULL`, `server-id=1` |
| `kafka` | `cp-kafka:7.8.0` | 9092 (ext), 29092 (int), 9093 (controller) | KRaft broker + controller; `auto.create.topics.enable=true` |
| `flink-jobmanager` | custom (Flink 2.2 + mysql-connector-j) | 8081 (REST), 6123 (RPC) | JobManager; 8 task slots; `taskmanager.slot.timeout=60000` |
| `flink-taskmanager` | custom (same image) | 8082, 6124 | TaskManager; 8 task slots |
| `flink-cdc-submitter` | custom | — | Runs `flink-cdc.sh` for the YAML Pipeline variant once JM is ready; `restart: on-failure` |
| `kafka-connect` | custom (Debezium + SMT JARs) | 8083 | KC REST API; side-by-side comparison; `restart: on-failure` |
| `kafka-ui` | `provectuslabs/kafka-ui:latest` | 8080 | Kafka topic browser |
| `minio` | `minio/minio:latest` | 9000 (API), 9001 (console) | S3-compatible checkpoint storage; bucket `flink-checkpoints` |
| `minio-init` | `minio/mc` | — | One-shot: creates the `flink-checkpoints` bucket at startup |
| `prometheus` | `prom/prometheus:v2.52.0` | 9090 | Scrapes Flink JM/TM metrics every 15 s; local only |
| `grafana` | `grafana/grafana:10.4.3` | 3001 | Dashboard + alert rules (Terraform-managed); admin/admin |

### Gradle Modules

| Module | Role |
|--------|------|
| `common` | `JobConfig`, `CheckpointConfigurer`, `PocJsonDeserializationSchema`, `CdcEventRouter`, `OutboxRouter`, `KafkaSinkFactory`, `DdlValidator` |
| `variant-flink-datastream-api-v1-cdc-job` | DataStream CDC; server-ID 5900–5999 |
| `variant-flink-table-api-cdc-job` | Table API CDC; server-ID 6000–6099 |
| `variant-flink-sql-api-cdc-job` | SQL API CDC; server-ID 5800–5899 |
| `variant-flink-datastream-api-v1-outbox-job` | Outbox; server-ID 5600–5699 |
| `variant-flink-cdc-yaml-pipeline-cdc-job` | YAML Pipeline; server-ID 5700–5709 |
| `component-tests` | End-to-end: submit fat-jars to JM REST; poll Kafka; covers all 5 Flink variants + 5 KC |
| `kafka-connect-smts` | `EnrichmentTransform` + `OutboxRoutingTransform` (Java 11, shadow JAR) |

### Build & Test Commands

| Command | What It Does |
|---------|-------------|
| `./gradlew shadowJar` | Builds the 4 Flink fat-jars + KC SMT shadow JAR |
| `./gradlew :component-tests:test` | Runs all component tests (Flink + KC) |
| `./gradlew all` | Full cycle: build → restart podman-compose → wait for services (180 s) → deploy KC connectors → run CTs |
| `podman-compose -f podman-compose.yml up -d` | Starts the full 11-service stack |
| `podman exec flink-jm flink run /tmp/<jar>` | Submit a variant to the running JM |

### Side-by-Side Kafka Connect (POC only)

Five KC connectors mirror the Flink variants, using server-IDs in the reserved `5500–5599` range:

| KC Connector | Server-ID | SMT |
|-------------|-----------|-----|
| `kc-datastream-cdc` | 5510 | `EnrichmentTransform` |
| `kc-table-api-cdc` | 5520 | `EnrichmentTransform` |
| `kc-sql-api-cdc` | 5530 | `EnrichmentTransform` |
| `kc-yaml-pipeline-cdc` | 5540 | `EnrichmentTransform` |
| `kc-outbox-cdc` | 5550 | `OutboxRoutingTransform` |

## Infrastructure List 3 — Comparison: Client vs. Local POC

| Area | Client (Production AWS) | Local POC (kind k8s) |
|------|--------------------|-----------------------------|
| **Orchestration** | Kubernetes + Flink Operator + Helm (`flink-base-chart`) | Same |
| **Flink deployment unit** | `FlinkDeployment` CR per job (Application Mode; own JM+TM) | Same |
| **Flink version** | 2.2 (via `flink-base-image`) | Same |
| **Flink CDC version** | 3.6.0-2.2 (included in variant images) | Same |
| **MySQL** | RDS (AWS); IAM auth; IRSA for S3; production data | K8s `mysql:8.0` (port-forward `localhost:13306`); seed data via `init.sql`; user `flink`/`flink`, db `poc_db` |
| **MySQL binlog server-ID** | Non-overlapping ranges 5600–6099 enforced via CI lint + base image template | Same ranges enforced via `JobConfig`; KC uses reserved 5500–5599 |
| **Kafka** | Confluent Kafka Cloud (managed) | Strimzi `Kafka` CR (`quay.io/strimzi/kafka:1.0.1-kafka-4.2.0`); external nodeport listener; `localhost:19092` (advertisedHost=localhost) |
| **Kafka Connect** | Confluent managed KC for SFTP (20) + SingleStore (1); replaced for 74 CDC connectors | Strimzi `KafkaConnect` CR (v1) + Debezium 3.5.2 + custom SMTs; port-forward `localhost:18086`; side-by-side comparison only |
| **Checkpointing** | S3 bucket (per-job `checkpointing.dir`); IRSA permissions | S3-compatible (MinIO) via `s3://flink-checkpoints`; incremental RocksDB backend, checkpoints persisted to MinIO; same code config (30 s interval, EXACTLY_ONCE) |
| **CI/CD** | Jenkins (image build, `yq` deletion, variant selection) + ArgoCD (deploy/restart) | `./gradlew allK8s` / `deploy.sh` (kind: load images → apply CRs → port-forward → CTs → teardown) |
| **Monitoring** | Datadog | Flink Dashboard `:18081`–`18085` + KC REST `:18086` + Prometheus `:19090` + Grafana `:13001` (no Kafka UI in the k8s slice); kube-prometheus-stack |
| **Java version** | 17 (Flink jobs) | 17 (Flink jobs); 11 (KC SMTs — cp-kafka-connect 7.6.1 constraint) |
| **IAM / Security** | RDS IAM tokens, IRSA, binlog lease management | No IAM; plain `flink`/`flink` credentials; rotation testing not possible |
| **Re-snapshot** | Savepoint + S3 checkpoint deletion + re-run with `--fromSavepoint` via Flink Operator; binlog lease risk; official path in FLINK_SAVEPOINT_RUNBOOK.md (Slide 16, Spike S9) | Cancel job via REST port-forward (`PATCH /jobs/:jid?mode=cancel`) before `delete FlinkDeployment` (cancel-before-delete, `deploy.sh` §9) |
| **State backend** | RocksDB (production) | Incremental RocksDB, managed memory (same as production; set via `FLINK_PROPERTIES` / `flinkConfiguration`, not in job code) |
| **Kafka topic naming** | `<team>.<schema>.<table>` with per-variant prefixes for all 26 teams | `poc.flink.<variant>.<table>` (Flink) / `poc.kc.<variant>.<table>` (Kafka Connect); single `poc_db` schema |
| **Observability** | Flink Platform Team / each team (config.tf) | Prometheus + Grafana (kube-prometheus-stack); Flink metrics via `flink-metrics-prometheus` |
| **Scale** | 74 CDC connectors → 26 teams | 1 schema (`poc_db`), 3 tables (`orders`, `customers`, `outbox_events`), 5 variants, 64 unit tests + CTs per variant |
| **YAML Pipeline submission** | `flink-cdc.sh` via init-container or `kubectl exec`; `FlinkDeployment` starts with an empty JM until wired | `flink-cdc-submitter` in kind; `FlinkDeployment` `mode: standalone` (TM pre-deployed) then submitter runs `flink-cdc.sh` |

---