# Flink CDC & Outbox Connectors POC — Kafka Connect vs Flink for MySQL

A working proof-of-concept demonstrating:
- **5 CDC patterns, each implemented twice** — once with Apache Flink, once with Kafka Connect — running side-by-side on the same MySQL (5 Flink + 5 KC = 10 implementations)

Each variant is an independent Gradle subproject that builds its own fat-jar and connects to the same MySQL + Kafka infra.

This repo contains the **[Flink Presentation and POC](./kafka-connect-at-scale-74-connectors-migration.md)** — a client proposal to migrate tens of connectors from Confluent Kafka Cloud to Flink (74 connectors); Cognizant Java Community presentation (26 June 2026). A [Romanian translation](./kafka-connect-at-scale-74-connectors-migration.ro.md) of the deck is also available.

---

## Variants

| # | Module | API | LOC | Server-ID range | Best for |
|---|--------|-----|------|-----------------|----------|
| 1 | `variant-flink-datastream-api-v1-cdc-job` | DataStream | 63 | 5900–5999 | CDC + custom enrichment/routing |
| 2 | `variant-flink-table-api-cdc-job` | Table API | 99 | 6000–6099 | CDC with future SQL joins/aggregations |
| 3 | `variant-flink-sql-api-cdc-job` | SQL API (StatementSet) | 156 | 5800–5899 | Multi-table CDC → single JobGraph |
| 4 | `variant-flink-datastream-api-v1-outbox-job` | DataStream | 56 | 5600–5699 | Transactional outbox, per-row topic routing |
| 5 | `variant-flink-cdc-yaml-pipeline-cdc-job` | YAML Pipeline | 0 | 5700–5709 | Simple CDC, zero Java |

---

## Stack

| Component | Version |
|-----------|---------|
| Apache Flink | 2.2.0 |
| Flink CDC | 3.6.0-2.2 |
| flink-connector-kafka | 5.0.0-2.2 |
| MySQL | 8.0 |
| Kafka | KRaft (Confluent 7.8.0) |
| Java | 17 (Flink jobs) / 11 (Kafka Connect SMTs) |
| Gradle | 8.7 |

---

## Prerequisites

**Podman Compose path (default, fast iteration):**
- Podman + podman-compose (`pip install podman-compose`)
- Java 17+

**Kubernetes path (production-shaped):**
- `kind`, `kubectl`, `helm`, `podman` on PATH
- Java 17+
- See [K8S.md](./K8S.md) for full k8s setup and port-forward details

---

## Quick Start

### 1. Start infrastructure

```bash
cd local-development-podman
podman-compose -f podman-compose.yml up -d
```

Services started:
- MySQL at `localhost:3306` (user: `flink`, password: `flink`, db: `poc_db`)
- Kafka at `localhost:9092`
- Flink Dashboard at http://localhost:8081
- Kafka UI at http://localhost:8080
- Kafka Connect at http://localhost:8083
- MinIO at http://localhost:9001 (user: `minioadmin`, password: `minioadmin`, bucket: `flink-checkpoints`)

### 2. Build all variants

```bash
./gradlew shadowJar
```

Each Java variant produces a fat-jar at `<module>/build/libs/<module>-all.jar` (the Shadow plugin's `-all` classifier; the plain `<module>.jar` next to it is the thin jar and will not run). Variant 5 (YAML pipeline) produces no jar.

### 3. Submit a variant

The Podman stack uses bridge networking; the JobManager container refers to other services by name.
Use `podman exec -e KEY=VALUE` to override `JobConfig.fromEnv()` defaults.

**DataStream CDC:**
```bash
podman cp variant-flink-datastream-api-v1-cdc-job/build/libs/variant-flink-datastream-api-v1-cdc-job-all.jar flink-jm:/tmp/
podman exec flink-jm flink run /tmp/variant-flink-datastream-api-v1-cdc-job-all.jar
```

**Table API:**
```bash
podman cp variant-flink-table-api-cdc-job/build/libs/variant-flink-table-api-cdc-job-all.jar flink-jm:/tmp/
podman exec flink-jm flink run /tmp/variant-flink-table-api-cdc-job-all.jar
```

**SQL API (multi-table StatementSet):**
```bash
podman cp variant-flink-sql-api-cdc-job/build/libs/variant-flink-sql-api-cdc-job-all.jar flink-jm:/tmp/
podman exec flink-jm flink run /tmp/variant-flink-sql-api-cdc-job-all.jar
```

**Outbox:**
```bash
podman cp variant-flink-datastream-api-v1-outbox-job/build/libs/variant-flink-datastream-api-v1-outbox-job-all.jar flink-jm:/tmp/
podman exec flink-jm flink run /tmp/variant-flink-datastream-api-v1-outbox-job-all.jar
```

**YAML Pipeline (no Java, submit via flink-cdc.sh):**
```bash
MYSQL_HOST=localhost KAFKA_BOOTSTRAP=localhost:9092 \
  flink-cdc.sh variant-flink-cdc-yaml-pipeline-cdc-job/src/main/resources/pipeline.yaml
```

### 4. Trigger CDC events

```sql
-- connect to MySQL
podman exec -it mysql mysql -uflink -pflink poc_db

-- insert / update to generate binlog events
INSERT INTO orders (customer_id, amount, status) VALUES (1, 500.00, 'PENDING');
UPDATE orders SET status = 'SHIPPED' WHERE id = 1;
DELETE FROM orders WHERE id = 1;
```

### 5. Watch the output

- **Flink Dashboard** → http://localhost:8081 — running jobs, task slots
- **Kafka UI** → http://localhost:8080 — topics `poc.flink.*` (Flink jobs) and `poc.kc.*` (Kafka Connect); see [TOPICS.md](./TOPICS.md)
- **Grafana** → http://localhost:3001 — Flink metrics dashboards (admin/admin)
- **Prometheus** → http://localhost:9090 — raw metrics scrapes
- **MinIO** → http://localhost:9001 — checkpoint state browser (`flink-checkpoints` bucket)

---

## Monitoring — Prometheus + Grafana

The POC includes a full Prometheus + Grafana monitoring stack that mirrors the production `rtdp-datadog-tf` approach using Terraform.

### Services

- **Prometheus** (port 9090) — scrapes Flink JobManager + TaskManager metrics every 15 seconds
- **Grafana** (port 3001) — dashboards and alert rules (admin/admin)

Both are included in `podman-compose.yml` and start automatically with `podman-compose up -d --build`.

**Dashboard:** http://localhost:3001/d/flink-cdc-poc-monitoring

### Dashboard panels

Dashboard is Terraform-managed (`local-development-podman/terraform/dashboard.tf` reads `flink-cdc-monitoring.json`):

- **3 shipped monitors** (stat panels) — mirrors `rtdp-datadog-tf`:
  1. Restart Loop — `flink.job.numRestarts` change > 3 per job
  2. Checkpoint Duration — `flink.jobmanager.job.lastCheckpointDuration` > 180 s
  3. Checkpoint Failures — `flink.jobmanager.job.numberOfFailedCheckpoints` change > 3

- **4 open items** (text placeholders) — pending Spike S1 (Flink metric parity investigation):
  - KC Monitor #4: Millis Behind Source (source latency)
  - KC Monitor #5: Database Disconnects (connection failures)
  - KC Monitor #6: Malformed Transactions (deserialization errors)
  - KC Monitor #7: DB Connected (connection status)

- **JVM monitoring** — heap usage (bytes), job restarts cumulative (timeseries)

### Alert rules

Three Grafana alert rules provisioned by Terraform (`local-development-podman/terraform/alerts.tf`), mirroring the monitors already shipped in `rtdp-datadog-tf`:

| Alert | PromQL | Threshold | Severity | For |
|-------|--------|-----------|----------|-----|
| Flink Restart Loop | `increase(flink_jobmanager_job_numRestarts[5m])` | > 3 | critical | 1 m |
| Flink Checkpoint Duration High | `flink_jobmanager_job_lastCheckpointDuration` | > 180 000 ms | warning | 2 m |
| Flink Checkpoint Failures | `increase(flink_jobmanager_job_numberOfFailedCheckpoints[5m])` | > 3 | critical | 1 m |

Alerts fire to the `flink-cdc-poc-email` contact point. View and silence at http://localhost:3001/alerting/list.

Each alert rule is linked to its dashboard panel via `__dashboardUid__` / `__panelId__` annotations. When an alert fires or resolves, Grafana writes an annotation marker directly onto the corresponding stat panel at http://localhost:3001/d/flink-cdc-poc-monitoring. The dashboard also has an "Alert state changes" annotation layer (red vertical lines) that appears on all timeseries panels.

| Alert | Linked panel |
|-------|-------------|
| Flink Restart Loop | Panel 1 — Restart Loop stat |
| Flink Checkpoint Duration High | Panel 2 — Checkpoint Duration stat |
| Flink Checkpoint Failures | Panel 3 — Checkpoint Failures stat |

> **Note:** Do not change the Grafana admin password through the UI — Terraform authenticates as `admin:admin` and will fail with 401 on the next apply. If the password is accidentally changed, reset it with `podman exec grafana grafana cli admin reset-admin-password admin` then `podman restart grafana`.

### Terraform

Dashboard and alert rules are fully managed by Terraform — `./gradlew all` runs `terraform apply` automatically after Grafana is healthy.

To apply manually:

```bash
cd local-development-podman/terraform
terraform init
terraform apply
```

`terraform apply` is idempotent — safe to re-run at any time.

Resources managed: Prometheus datasource · dashboard · folder · 3 alert rules · contact point · notification policy.

---

## Full Integration Test

### Podman Compose (local fast loop)

```bash
./gradlew all
```

Orchestrates: build all fat-jars → restart Podman Compose → wait for services → deploy Kafka Connect connectors → run component tests.

### Kubernetes (production-shaped)

```bash
./gradlew allK8s
```

Orchestrates: `./local-development-k8s/deploy.sh` (kind cluster + 5 Flink + 5 KC + monitoring) → kubectl port-forwards → run each Flink variant test with `FLINK_REST_URL` targeting its own JM → run KC tests → tear down tunnels.

Each Flink variant test class runs against its own JM REST endpoint (one port-forward per variant, ports 18081–18085). KC tests target the Strimzi KC cluster at port 18086. See [K8S.md](./K8S.md) for details.

---

## Environment Variables

All variants read configuration from environment variables with sensible defaults for local Podman use.

**Flink job config** (read by `JobConfig.fromEnv()` in every variant):

| Variable | Default | Description |
|----------|---------|-------------|
| `MYSQL_HOST` | `localhost` | MySQL hostname |
| `MYSQL_PORT` | `3306` | MySQL port |
| `MYSQL_USER` | `flink` | MySQL username |
| `MYSQL_PASSWORD` | `flink` | MySQL password |
| `MYSQL_DATABASE` | `poc_db` | Database name |
| `MYSQL_TABLES` | `poc_db.orders,poc_db.customers,poc_db.outbox_events` | Fully-qualified table list |
| `MYSQL_SERVER_ID` | `5900-5999` | Binlog replica server-ID range |
| `KAFKA_BOOTSTRAP` | `localhost:9092` | Kafka bootstrap servers |
| `KAFKA_TOPIC_PREFIX` | `poc.flink` | Topic prefix; `.<variant>.<table>` appended (KC connectors use their own `poc.kc` prefix) |

**Component test targeting** (override defaults for k8s port-forwards):

| Variable | Default | Description |
|----------|---------|-------------|
| `FLINK_REST_URL` | `http://localhost:8081` | Flink JM REST endpoint (`FlinkRestClient`) |
| `KAFKA_CONNECT_URL` | `http://localhost:8083` | Kafka Connect REST endpoint (`KafkaConnectBase`) |
| `SCHEMA_HISTORY_KAFKA_BOOTSTRAP` | `kafka:29092` | Bootstrap servers for Debezium schema history topic inside KC; set to `poc-kafka-kafka-bootstrap:9092` for k8s |

---

## Project Layout

```
flink-cdc-poc/
├── build.gradle                        # root — versions + shared config
├── settings.gradle
├── gradle/wrapper/
├── common/                             # shared: JobConfig, deserializer, routers, KafkaSinkFactory, CheckpointConfigurer, DdlValidator
│   └── src/main/java/poc/common/
│       ├── config/JobConfig.java
│       ├── deserializer/PocJsonDeserializationSchema.java
│       ├── router/CdcEventRouter.java
│       ├── router/OutboxRouter.java
│       └── sink/KafkaSinkFactory.java
├── variant-flink-datastream-api-v1-cdc-job/
│   └── src/main/java/poc/datastream/DataStreamCdcJob.java
├── variant-flink-table-api-cdc-job/
│   └── src/main/java/poc/tableapi/TableApiCdcJob.java
├── variant-flink-sql-api-cdc-job/
│   └── src/main/java/poc/sqlapi/SqlApiCdcJob.java
├── variant-flink-datastream-api-v1-outbox-job/
│   └── src/main/java/poc/outbox/OutboxJob.java
├── variant-flink-cdc-yaml-pipeline-cdc-job/
│   └── src/main/resources/pipeline.yaml
├── component-tests/                    # end-to-end tests (Flink + Kafka Connect)
│   └── src/test/java/poc/component/
│       ├── ContainerBase.java          # shared: MySQL/Kafka connectivity, poll helpers
│       ├── FlinkTestBase.java          # submit fat-jar to JM, wait RUNNING, reuse job
│       ├── FlinkRestClient.java        # Flink JM REST client
│       ├── KafkaConnectBase.java       # deploy connector, poll, shared config
│       ├── DataStreamCdcTest.java      # one per Flink variant: DataStream / Table API
│       ├── TableApiCdcTest.java        #   / SQL API / Outbox / YAML Pipeline
│       ├── SqlApiCdcTest.java
│       ├── DataStreamOutboxTest.java
│       ├── YamlPipelineCdcTest.java
│       ├── KafkaConnectVariantTest.java  # 4 enrichment KC variants (parameterized)
│       ├── KafkaConnectOutboxTest.java   # outbox routing KC variant
│       └── …                           # plus parity / data-quality / mini-cluster suites
├── local-development-podman/           # Podman Compose stack
│   ├── podman-compose.yml
│   ├── mysql-init/init.sql             # schema + seed data
│   ├── flink-with-mysql/               # Flink JM + TM image (MySQL JDBC driver added)
│   │   └── Dockerfile
│   ├── flink-cdc-submitter/            # runs flink-cdc.sh for variant 5 (YAML pipeline)
│   │   ├── Dockerfile
│   │   └── entrypoint.sh
│   ├── kafka-connect/                  # Kafka Connect image + connector configs
│   │   ├── Dockerfile
│   │   ├── deploy-connectors.sh
│   │   └── connectors/                 # 5 × connector JSON configs
│   └── kafka-connect-smts/             # custom SMT Gradle subproject (Java 11)
│       └── src/main/java/poc/kafka/connect/
│           ├── EnrichmentTransform.java
│           └── OutboxRoutingTransform.java
└── local-development-k8s/              # Kubernetes stack (kind + Flink Operator + Strimzi)
    ├── deploy.sh                        # full build + apply + wait orchestrator
    ├── teardown.sh
    ├── port-forward.sh                  # open/close host port-forwards (18081-18086, 13306, 19092, …)
    ├── flink/                           # FlinkDeployment CRs + artifact Dockerfiles
    ├── kafka-connect/                   # KafkaConnect CR + KafkaConnector CRs
    ├── kafka/                           # Strimzi Kafka CR
    ├── mysql/ minio/                    # MySQL + MinIO manifests
    └── monitoring/                      # kube-prometheus-stack values + PodMonitor + alerts
```

---

## Production Deployment

All variants are configured with **exactly-once semantics** and **30-second checkpoint intervals**. For production deployments:

- **Safe upgrades with savepoints** → See [FLINK_SAVEPOINT_RUNBOOK.md](./FLINK_SAVEPOINT_RUNBOOK.md)
  - 5-phase workflow: create → verify → cancel → upgrade → resume
  - Handles MySQL binlog lease expiration and server-ID collision prevention

- **Checkpoint configuration & monitoring** → See [FLINK_CHECKPOINT_CONFIG.md](./FLINK_CHECKPOINT_CONFIG.md)
  - Flink 2.2 checkpoint semantics and production checklist
  - REST API examples and Flink Dashboard monitoring

---

## Key Design Decisions

- **Server-ID ranges per variant** — Flink CDC 3.x incremental snapshot allocates multiple server IDs for parallel readers and restarts. A single integer collides on restart because the previous MySQL binlog lease hasn't expired. Each variant owns a non-overlapping range.

- **One fat-jar per variant** — built with the Shadow plugin. Enables independent upgrades; no fleet-wide coordinated releases.

- **StatementSet in variant 3** — all `INSERT` statements compile into a single JobGraph: one checkpoint, one recovery unit, exactly-once semantics across multiple tables.

- **All config via env vars** — makes each variant drop-in compatible with the Helm-override shared-job model described in the architecture proposal.

---

## References

### Project Documentation
- [CLAUDE.md](./CLAUDE.md) — module structure, server-ID ranges, component tests, build commands
- [K8S.md](./K8S.md) — Kubernetes deployment path (kind + Flink Operator + Strimzi + monitoring)
- [FLINK_CHECKPOINT_CONFIG.md](./FLINK_CHECKPOINT_CONFIG.md) — checkpoint semantics, monitoring, troubleshooting
- [FLINK_SAVEPOINT_RUNBOOK.md](./FLINK_SAVEPOINT_RUNBOOK.md) — safe upgrade workflows, state recovery
- [KAFKA_CONNECT.md](./KAFKA_CONNECT.md) — Kafka Connect CDC variants, SMTs, comparison
- [kafka-connect-at-scale-74-connectors-migration.md](./kafka-connect-at-scale-74-connectors-migration.md) — presentation deck (EN): real-world 74-connector KC→Flink migration case study
- [kafka-connect-at-scale-74-connectors-migration.ro.md](./kafka-connect-at-scale-74-connectors-migration.ro.md) — Romanian translation of the presentation deck

### External
- [Apache Flink CDC documentation](https://nightlies.apache.org/flink/flink-cdc-docs-stable/)
- [MySQL CDC Connector](https://nightlies.apache.org/flink/flink-cdc-docs-stable/docs/connectors/pipeline-connectors/mysql/)
- [Flink Kafka Connector](https://nightlies.apache.org/flink/flink-docs-stable/docs/connectors/datastream/kafka/)
- [Flink Checkpointing](https://nightlies.apache.org/flink/flink-docs-stable/docs/dev/datastream/fault_tolerance/checkpointing/)
