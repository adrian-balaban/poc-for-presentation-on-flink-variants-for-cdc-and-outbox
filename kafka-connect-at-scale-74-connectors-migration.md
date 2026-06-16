# Kafka Connect @ Scale: 74 Connectors Migration Case

**Author:** Adrian Balaban  
**Date:** 2026-06

---

## Slide 1 — Abstract

**Kafka Connect @ Scale: 74 Connectors Migration Case**

In this session, we will explore Kafka Connect through real client experience,
focusing on proposed migration of **74 MySQL connectors** (out of 95 total) **from Confluent Kafka Cloud to Flink** and a proof of concept.

We will walk through the current challenges, what improvements the new approach
aims to address, and the trade-offs involved. The talk also highlights
**alternative options considered** and the **reasoning behind the proposed solution**.

> Real client. Real scale. 95 connectors, 26 teams, one shared cluster — and the
> question whether Flink is the right way out.

---

## Slide 2 — The Client Context (Where We Are Today)

**Real client experience: Confluent Kafka Cloud at scale**

- **95 connectors** on **one shared Kafka Connect cluster** across **26 teams**
- Two connector families today:
  - **Debezium (Kafka Connect)** — reads MySQL binlog via Confluent-managed KC, one event per change per topic
  - SFTP + SingleStore sink/source connectors
- Everything shares one cluster: one config, one rebalance group, one blast radius

> The shared cluster was convenient at 5 connectors. At 95, it is the single
> biggest source of cross-team incidents.

---

## Slide 3 — The Challenges (Why We Started This Proposal and POC)

| Pain | Who Suffers | How Often |
|------|------------|-----------|
| Rebalancing storms — one bad connector destabilises all | All 26 teams | Multiple times/quarter |
| Shared blast radius — 95 connectors, one cluster | All 26 teams | Every incident |
| Recurring lag — no per-tribe lever | Affected team + consumers | Ongoing |
| Production-only failures — surface only after deploy | Teams deploying new connectors | Some time for new connector |
| Confluent Kafka Cloud licensing cost | Organisation | Monthly |
| Centralised security patching | Team who do maintenance | Every release cycle |

> One bad connector restart triggers a **cascade rebalance across unrelated tribes**.

---

## Slide 4 — Scope of the Migration

**What we're migrating:** 74 CDC (MySQL binlog) connectors → Apache Flink MySQL CDC Connector

**What stays on Kafka Connect:** 21 SFTP + SingleStore connectors (Flink has no equivalent)

![Migration Pattern: Before and After](images/migration-before-after.svg)

---

## Slide 5 — The POC: Five Flink Variants

We built **5 variants** in a single repo (`model-flink-job` PR #61) and ran them
**simultaneously** in one Flink cluster.

| # | Variant | Entry-Class Size | Output Format | Java Required |
|---|---------|-----------|---------------|---------------|
| 1 | DataStream CDC | 49 lines | Flattened + enrichment | Yes |
| 2 | Table API | 93 lines | Native Debezium envelope | Yes |
| 3 | SQL API | 136 lines | Native Debezium envelope | Minimal |
| 4 | Outbox | 53 lines | Raw payload per destination | Yes |
| 5 | YAML Pipeline | 55 lines YAML | Native Debezium envelope | **No** |

> All four Java variants additionally share ~295 lines of `common/` infrastructure
> (`JobConfig`, `CheckpointConfigurer`, deserializer, routers, `KafkaSinkFactory`) —
> entry classes contain only variant-specific wiring.

### POC Module Structure (`flink-cdc-poc`)

```
flink-cdc-poc/
├── common/                             # JobConfig, CheckpointConfigurer, deserializer, CdcEventRouter, OutboxRouter, KafkaSinkFactory
├── variant-flink-datastream-api-v1-cdc-job/   # DataStreamCdcJob.java  (49 lines, server-ID 5900–5999)
├── variant-flink-table-api-cdc-job/           # TableApiCdcJob.java    (93 lines, server-ID 6000–6099)
├── variant-flink-sql-api-cdc-job/             # SqlApiCdcJob.java      (136 lines, server-ID 5800–5899)
├── variant-flink-datastream-api-v1-outbox-job/ # OutboxJob.java        (53 lines, server-ID 5600–5699)
├── variant-flink-cdc-yaml-pipeline-cdc-job/   # pipeline.yaml         (55 lines,  server-ID 5700–5709)
├── component-tests/                    # end-to-end: DataStreamCdcTest, TableApiCdcTest, SqlApiCdcTest,
│                                       #   DataStreamOutboxTest, YamlPipelineCdcTest,
│                                       #   KafkaConnectVariantTest, KafkaConnectOutboxTest
└── local-development/
    ├── podman-compose.yml              # MySQL + Kafka + Flink JM/TM + KC + kafka-ui + flink-cdc-submitter
    ├── flink-with-mysql/Dockerfile     # Flink 2.2 + mysql-connector-j
    ├── flink-cdc-submitter/            # runs flink-cdc.sh for YAML Pipeline variant
    ├── kafka-connect/                  # Debezium + custom SMTs; 5 connector JSON configs
    └── kafka-connect-smts/             # EnrichmentTransform + OutboxRoutingTransform (Java 11)
```

---

## Slide 6 — Decision Matrix: Which Variant for Which Connector?

![Connector Decision Tree: Which Variant for Which Connector?](images/connector-decision-tree.svg)

---

## Slide 7 — The Java Dev's View: Code Comparison

### DataStream CDC (49-line entry class, most control)

```java
MySqlSource<String> source = MySqlSource.<String>builder()
    .hostname(config.mysqlHost).port(config.mysqlPort)
    .databaseList(config.mysqlDatabase).tableList(config.mysqlTables)
    .username(config.mysqlUser).password(config.mysqlPassword)
    .serverTimeZone("UTC")
    .serverId(config.serverId)
    .deserializer(new PocJsonDeserializationSchema())
    .build();

env.fromSource(source, WatermarkStrategy.noWatermarks(), "MySQL CDC Source")
   .process(new CdcEventRouter(config))
   .sinkTo(KafkaSinkFactory.create(config, "datastream"));
```

> All connection details come from `JobConfig.fromEnv()` — nothing is hardcoded;
> this is the same parametrisation the shared-job model (Slide 8) relies on.

### YAML Pipeline (55 lines, zero Java)

```yaml
source:
  type: mysql
  hostname: ${MYSQL_HOST}
  database-name: ${MYSQL_DB}
  table-name: ${MYSQL_TABLE}
sink:
  type: kafka
  topic: ${KAFKA_TOPIC}
pipeline:
  name: ${JOB_NAME}
```

---

## Slide 8 — Recommended Architecture: Shared Job Model

**One base image. 74 tribes. Zero Java per tribe.**

Flink Platform Team owns and maintains a single parametrisable DataStream CDC image.
Each tribe gets their connector by overriding Helm values only — no fork, no Java, no release pipeline.

![K8s Deployment Topology: Shared Job Model](images/k8s-deployment-topology.svg)

```yaml
# All a tribe needs
applicationJobs:
  my-tribe-cdc:
    image: flink-stream-api-base-image:1.0.0
    extraEnvs:
      MYSQL_HOST: my-db.internal
      MYSQL_DB: my_schema
      KAFKA_TOPIC_PREFIX: my-tribe.cdc
```

| Variable | Description |
|----------|-------------|
| `MYSQL_HOST/PORT/USER/PASSWORD` | CDC source |
| `MYSQL_DATABASE` / `MYSQL_TABLES` | Scope of capture |
| `MYSQL_SERVER_ID` | Binlog replica range (non-overlapping) |
| `KAFKA_BOOTSTRAP` / `KAFKA_TOPIC_PREFIX` | Sink config |

---

## Slide 9 — K8s Deployment Model

**One repo, five variants, one Jenkins pipeline definition — one execution per variant.**

The `flink-base-chart` `applicationJobs` map emits per key:
- `FlinkDeployment` CR (its own JobManager + TaskManager)
- `<jobName>-rest` ClusterIP Service
- `FlinkStateSnapshot` CR

### Collision Avoidance — Each Variant Gets Its Own Lane

| Axis | Allocation |
|------|------------|
| MySQL server-ID | outbox=5600–5699, pipeline=5700–5709, sql-api=5800–5899, cdc=5900–5999, table-api=6000–6099 |
| MySQL schema | `cdc_db`, `sql_api_db`, `table_api_db`, `pipeline_db`, `outbox_db` |
| Kafka topic prefix | `shared-cdc.cdc-db.*`, `sql-api.sql-api-db.*`, `table-api.table-api-db.*`, `pipeline.pipeline-db.*`, `outbox.destination.*` |
| S3 checkpoint paths | Auto-namespaced by `jobId` — shared bucket, safe |

**Kafka Connect (POC side-by-side)** uses the reserved `5500–5599` range:

| KC Connector | Server-ID |
|-------------|-----------|
| kc-datastream-cdc | 5510 |
| kc-table-api-cdc | 5520 |
| kc-sql-api-cdc | 5530 |
| kc-yaml-pipeline-cdc | 5540 |
| kc-outbox-cdc | 5550 |

> **Why ranges, not single IDs?** Flink CDC 3.x incremental snapshot allocates IDs for
> parallel readers + restart attempts. A single int collides on restart because the
> previous MySQL binlog lease hasn't timed out.

---

## Slide 10 — CDC Snapshotting: Before vs After

![CDC Snapshotting Flow: Today vs Post-Migration](images/cdc-snapshotting-flow.svg)

**What disappears:** `OneShotUnboundedSource`, `SnapshotSignalProcessFunction`, signal Kafka topic
— **3 Java classes and 1 Kafka topic eliminated**.

> **Caution:** `stateless` is a one-shot re-snapshot lever — always revert to `last-state`.
> Left on permanently, **every** restart re-snapshots the full table.

### Checkpoint Configuration (production-ready)

All five variants share one extraction point — `CheckpointConfigurer.applyExactlyOnce(env)` —
rather than repeating the five calls below in every entry class:

```java
env.enableCheckpointing(30_000);              // 30-second interval
env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
env.getCheckpointConfig().setCheckpointTimeout(60_000);
env.getCheckpointConfig().setMinPauseBetweenCheckpoints(5_000);
```

| Setting | Value | Reason |
|---------|-------|--------|
| `enableCheckpointing` | 30,000 ms | Balances durability vs performance |
| `CheckpointingMode` | EXACTLY_ONCE | Prevents duplicate Kafka messages on recovery |
| `MaxConcurrentCheckpoints` | 1 | CDC jobs snapshot during checkpoint; one at a time |
| `CheckpointTimeout` | 60,000 ms | 2× the interval; gives headroom for large-state jobs under load |
| `MinPauseBetweenCheckpoints` | 5,000 ms | Prevents checkpoint storms after one finishes |

---

## Slide 11 — POC Evidence

| Verification | Result |
|-------------|--------|
| Unit tests | 60/60 passing |
| All 8 modules compile | Clean |
| Format (Spotify fmt) | Compliant |
| Flink CDC 3.6.0 on Flink 2.2 | Verified |
| Per-variant component tests | Configured (gated by `FLINK_JOB_TYPE` in Jenkins) |
| StatementSet → 1 JobGraph | Verified (SQL API + Table API) |
| All 5 variants running simultaneously | Verified (localhost:8081 Flink Dashboard) |

---

## Slide 11b — POC Evidence: Live Screenshots

**All 5 Flink variants running simultaneously on localhost — captured during the live POC.**

### Flink Dashboard — 5/5 Jobs RUNNING

![Flink Dashboard — 5 variants running simultaneously](images/slides/flink-dashboard.png)

> All five CDC variants (DataStream, Table API, SQL API, Outbox, YAML Pipeline) live in one
> Flink cluster. Each has its own MySQL server-ID range; zero collisions.

### Kafka UI — poc Cluster (32 topics, 109 partitions)

![Kafka UI — poc cluster overview](images/slides/kafka-ui.png)

> Topics auto-created by CDC connectors. 32 topics = per-table topics for all 5 variants
> plus schema-history and signal topics.

### Kafka Connect REST API — 5 KC Connectors (side-by-side comparison)

![Kafka Connect — 5 connectors list](images/slides/kafka-connect.png)

> KC connectors run in parallel for output comparison only. Server-IDs in the reserved
> `5500–5599` range to avoid collision with the Flink variants.

---

## Slide 12 — Improvements the New Approach Addresses

- **Reduced blast radius** — each tribe's Flink job is isolated; failure can't cascade
- **Clear ownership** — tribe owns their connector repo and deploy cadence
- **No licensing costs** — Apache Flink 2.2 + Flink CDC (open source, Apache 2.0)
- **Native Kubernetes** — Flink Operator handles lifecycle, scaling, recovery
- **Native checkpointing** — per-job exactly-once semantics; no shared offset topic
- **Independent upgrades** — per-job versioning; no fleet-wide coordinated upgrades

> Every row in the "Challenges" table maps to a concrete improvement here.

---

## Slide 13 — The Trade-offs

- KC doesn't disappear entirely (21 SFTP/SingleStore connectors remain — two systems to run)
- Field-level encryption complexity transfers — connectors using custom CDC SMTs must replicate encryption logic in Flink `MapFunction`
- Learning curve for teams unfamiliar with Flink (mitigated by shared-job: no Java required)
- Per-connector cutover work (mitigated by automation tooling — Spikes S5/S6)
- New operational surface — Flink Operator, checkpoints, savepoints to learn and monitor

---

## Slide 14 — Alternatives Considered & Reasoning

![Alternatives Analysis: Why Flink CDC Shared-Job Model?](images/alternatives-analysis.svg)

---

## Slide 15 — Open Spikes

| ID | Topic | Why It Matters | Timebox |
|----|-------|---------------|---------|
| S1/S10 | Flink metric parity — Debezium JMX metrics via Flink? | Determines monitoring module design; blocks #4–#7 KC monitor mapping | 3 days |
| S2 | Initial snapshot memory pressure on largest table (~15M rows) | Prevents surprise in Phase 1/2 | 2 days |
| S3 | Outbox multi-topic routing at scale (POC tests at 2; production outbox uses ~15 destinations) | Phase 1 go-live blocker | 2 days |
| S4 | `snapshot.aborted`/`snapshot.running` Flink equivalent | outbox-transactron-connector migration (Phase 3) | 2 days |
| S5 | Production failure modes (RDS IAM, binlog leases, IRSA rotation) | POC can't surface these; staging soak needed | ≥7-day soak |
| S6 | Cutover automation tooling (KC → Flink) | Manual switches won't scale across tribes | Pre-Phase-3 |
| S7 | Self-service Claude migration tooling for tribes | Tribes can't wait for Flink Platform Team hand-holding | 3 days |

**Phase 0 total (S1–S4): ~9 engineering days — parallelisable within 1 sprint.**

---

## Slide 16 — Centralised Monitoring: KC and Flink

**POC coverage** (local): Flink Dashboard (:8081) + Kafka UI (:8080) cover the same signals as Datadog — restarts, checkpoint duration, checkpoint failures.

**Production gap (Spike S1/S10):** Debezium JMX metrics (connector lag, snapshot status, binlog position) have no direct Flink CDC equivalent yet. Until S1/S10 resolves this, Datadog monitors #4–#7 for KC connectors cannot be directly mapped to Flink jobs.

**Target state:** One shared Terraform module per platform (KC module owned by the Module Owner; Flink module by Flink Platform Team), consumed by each tribe's `config.tf` — ~600 monitors across 26 teams at end-state.

---

## APPENDIX — Backup Slides (Not Part of the 45-Minute Talk)

> The three lists below are reference material for Q&A only. Do not present them live —
> they are here so you can jump to a specific table if asked a detailed infra question.

---

## Infrastructure List 1 — Client (Production) Infrastructure

### Kubernetes

- **Flink Operator** — manages `FlinkDeployment` CRs; under pressure with 4 deployments in preview; TaskManager slot capacity must be watched
- **`FlinkDeployment` CRs** — one per job/variant; each with its own JobManager + TaskManager pod pair (Application Mode)
- **`FlinkStateSnapshot` CRs** — one per job, managed by chart
- **ClusterIP Services** — `<jobName>-rest` per job, port 8081
- **Helm chart: `flink-base-chart`** — `applicationJobs` map, init-container delivery, topology spread, probes, graceful shutdown, restart strategy
- **Namespace isolation** — per-variant schema + server-ID ranges enforced by values, not by chart

### Apache Flink

- **Flink runtime 2.2** — base image `flink-base-image` (Flink Platform Team)
- **Flink CDC 3.6.0** (suffix `3.6.0-2.2`) — bundled in variant images; version must match runtime
- **Built-in plugins** — `flink-s3-fs-presto-2.2.0.jar` (version-stamped, must match base image)
- **`mysql-connector-j`** — mounted into both JobManager and TaskManager; classloader parent-first pattern required (`com.mysql.`)
- **Checkpointing** — per-job unique `checkpointing.dir`; exactly-once; S3-backed

### MySQL / Databases

- **MySQL binlog access** — Flink CDC reads binlog directly; requires `log-bin`, `binlog-format=ROW`, `binlog-row-image=FULL`
- **Per-variant schemas**: `cdc_db`, `sql_api_db`, `table_api_db`, `pipeline_db`, `outbox_db`
- **Non-overlapping `server-id` ranges**: outbox 5600–5699, pipeline 5700–5709, sql-api 5800–5899, shared-cdc 5900–5999, table-api 6000–6099; KC reserved 5500–5599
- **RDS IAM token rotation** — production-only failure mode (S5); IAM pattern captured in base image template
- **IRSA** — for checkpoint-store S3 permissions; rotation tested in ≥7-day Phase 1 soak
- **MySQL privileges** — `RELOAD` + `LOCK TABLES` required for initial snapshot

### Kafka

- **Kafka topics** (per-variant prefixes): `shared-cdc.cdc-db.*`, `sql-api.sql-api-db.*`, `table-api.table-api-db.*`, `pipeline.pipeline-db.*`, `outbox.destination.*`
- **Schema history topics** (KC/Debezium): `dbhistory.<variant>` — one per connector
- **Signal topic** (pre-migration only, dropped after): `private.debezium.signal.<connector>.v1`
- **Kafka Connect (Confluent)** — retained for SFTP (25) and SingleStore (1); 74 Debezium MySQL connectors migrated to Flink
- **Heartbeat topic** — KC monitor #1; Flink equivalent: Restart Loop + TM heartbeat

### Container Registry / Images

- `flink-base-image` — Flink runtime (existing)
- Proposed new base images (Shared Job / Platform Architect):
  - `flink-cdc-base-image`
  - `flink-stream-api-base-image`
  - `flink-table-api-base-image`
  - `flink-sql-api-base-image`
- Per-variant fat-jar images: 5 Docker images; one-per-PR via Jenkins; Shadow plugin

### Object Storage (S3)

- Shared S3 bucket for checkpoints/savepoints — auto-namespaced by `jobId`
- Per-job `checkpointing.dir` paths must not overlap

### CI/CD

- **Jenkins** — `FLINK_JOB_TYPE` selects variant per PR; `sed` replaces image placeholders; `yq` deletes unused `applicationJobs` entries; master cron rotates all 5
- **ArgoCD** — `FlinkDeployment` CR lifecycle; `restartNonce` + `upgradeMode` for re-snapshots
- **Component tests** — per-variant, hit `<name>-rest:8081`, verify via Kafka polling

### Observability (Datadog via Terraform)

- **`<datadog-tf-repo>`** — central Terraform repo for all 26 teams (target state)
- **Shipped monitors** (16): Restart Loop, Checkpoint Duration, Checkpoint Failures
- **Shipped dashboards** (2): `[Platform] Flink Jobs Monitoring`, `[Platform] Flink CDC Streamer`
- **~600 monitors** at end-state across 26 teams — quota forecast needed (open item)
- **Notification routing**: 3 global channels (1/env) + per-team Slack/Zendesk/PagerDuty
- **Terraform modules**: `<kafka-datadog-tf-module>/kafka-connector-outbox` → `<datadog-tf-repo>/monitors/shared-definitions/kafka-connect` (Module Owner); new Flink module (Flink Platform Team)
- **Validation**: `terraform plan` + `*.tftest.hcl`; no `terraform apply` from branches

### IAM / Security

- **IRSA** — S3 checkpoint access; rotation must be tested
- **RDS IAM tokens** — short-lived; expiry surfaces only in production
- **Binlog leases** — MySQL binlog replica; server-ID lease timeout must be managed on restart

### Not Needed Post-Migration

- Kafka signal topic (`private.debezium.signal.*.v1`)
- `OneShotUnboundedSource`, `SnapshotSignalProcessFunction`, `SignalMessage` Java classes
- Confluent Kafka Connect for the 74 Debezium MySQL connectors
- `dbhistory.*` schema history topics for those 74 connectors

---

## Infrastructure List 2 — Local POC Infrastructure
*Source: `flink-cdc-poc/` folder (`podman-compose.yml`, `build.gradle`, `README.md`, `KAFKA_CONNECT.md`, `CHECKPOINT_CONFIG.md`)*

### Software Versions

| Component | Version |
|-----------|---------|
| Apache Flink | 2.2.0 |
| Flink CDC | 3.6.0-2.2 |
| flink-connector-kafka | 5.0.0-2.2 |
| Kafka (Confluent) | KRaft mode, cp-kafka 7.6.1 |
| MySQL | 8.0 |
| Java (Flink jobs) | 17 |
| Java (Kafka Connect SMTs) | 11 (cp-kafka-connect 7.6.1 JDK) |
| mysql-connector-j | 8.0.33 |
| Gradle | 8.7 |
| Shadow plugin | 8.1.1 |

### Podman-Compose Services

| Service | Image | Port(s) | Role |
|---------|-------|---------|------|
| `mysql` | `mysql:8.0` | 3306 | CDC source; `log-bin`, `binlog-format=ROW`, `binlog-row-image=FULL`, `server-id=1` |
| `kafka` | `cp-kafka:7.6.1` | 9092 (ext), 29092 (int), 9093 (controller) | KRaft broker + controller; `auto.create.topics.enable=true` |
| `flink-jobmanager` | custom (Flink 2.2 + mysql-connector-j) | 8081 (REST), 6123 (RPC) | JobManager; 8 task slots; `taskmanager.slot.timeout=60000` |
| `flink-taskmanager` | custom (same image) | 8082, 6124 | TaskManager; 8 task slots |
| `flink-cdc-submitter` | custom | — | Runs `flink-cdc.sh` for YAML Pipeline variant on JM ready; `restart: on-failure` |
| `kafka-connect` | custom (Debezium + SMT JARs) | 8083 | KC REST API; side-by-side comparison; `restart: on-failure` |
| `kafka-ui` | `provectuslabs/kafka-ui:latest` | 8080 | Kafka topic browser |

### Gradle Modules

| Module | Role |
|--------|------|
| `common` | `JobConfig`, `CheckpointConfigurer`, `PocJsonDeserializationSchema`, `CdcEventRouter`, `OutboxRouter`, `KafkaSinkFactory` |
| `variant-flink-datastream-api-v1-cdc-job` | DataStream CDC; server-ID 5900–5999 |
| `variant-flink-table-api-cdc-job` | Table API CDC; server-ID 6000–6099 |
| `variant-flink-sql-api-cdc-job` | SQL API CDC; server-ID 5800–5899 |
| `variant-flink-datastream-api-v1-outbox-job` | Outbox; server-ID 5600–5699 |
| `variant-flink-cdc-yaml-pipeline-cdc-job` | YAML Pipeline; server-ID 5700–5709 |
| `component-tests` | End-to-end: submits fat-jars to JM REST; polls Kafka; covers all 5 Flink + 5 KC variants |
| `kafka-connect-smts` | `EnrichmentTransform` + `OutboxRoutingTransform` (Java 11, shadow JAR) |

### Build & Test Commands

| Command | What it does |
|---------|-------------|
| `./gradlew shadowJar` | Builds all 5 fat-jars |
| `./gradlew :component-tests:test` | Runs all component tests (Flink + KC) |
| `./gradlew all` | Full cycle: build → podman-compose restart → wait for services (180 s) → deploy KC connectors → run CTs |
| `podman-compose -f podman-compose.yml up -d` | Starts the full 7-service stack |
| `podman exec flink-jm flink run /tmp/<jar>` | Submits a variant to the running JM |

### Kafka Connect Side-by-Side (POC only)

Five KC connectors mirror the Flink variants, using server-IDs in the reserved `5500–5599` range:

| KC Connector | Server-ID | SMT |
|-------------|-----------|-----|
| `kc-datastream-cdc` | 5510 | `EnrichmentTransform` |
| `kc-table-api-cdc` | 5520 | `EnrichmentTransform` |
| `kc-sql-api-cdc` | 5530 | `EnrichmentTransform` |
| `kc-yaml-pipeline-cdc` | 5540 | `EnrichmentTransform` |
| `kc-outbox-cdc` | 5550 | `OutboxRoutingTransform` |

### Local Monitoring Endpoints

| URL | What | Screenshot |
|-----|------|------------|
| `http://localhost:8081` | Flink Dashboard (running jobs, task slots, checkpoints) | ![](images/slides/flink-dashboard.png) |
| `http://localhost:8080` | Kafka UI (topics, messages) | ![](images/slides/kafka-ui.png) |
| `http://localhost:8083` | Kafka Connect REST API | ![](images/slides/kafka-connect.png) |
| `http://localhost:3306` | MySQL (user: `flink`, password: `flink`, db: `poc_db`) | — |
| `localhost:9092` | Kafka (external; topics: `poc.cdc.*`) | — |

---

## Infrastructure List 3 — Comparison: Client vs Local POC

| Area | Client (Production) | Local POC (`flink-cdc-poc`) |
|------|--------------------|-----------------------------|
| **Orchestration** | Kubernetes + Flink Operator + Helm (`flink-base-chart`) | Podman-compose (7 containers, bridge network) |
| **Flink deployment unit** | `FlinkDeployment` CR per job (Application Mode; own JM+TM) | Single shared JM + TM containers; all 5 variants submitted as jobs |
| **Flink version** | 2.2 (via `flink-base-image`) | 2.2.0 (custom Dockerfile: `flink-with-mysql`) |
| **Flink CDC version** | 3.6.0-2.2 (bundled in variant images) | 3.6.0-2.2 (Gradle dep in `build.gradle`) |
| **MySQL** | RDS (AWS); IAM auth; IRSA for S3; production data | `mysql:8.0` container; user `flink`/`flink`; `poc_db`; seed data via `init.sql` |
| **MySQL binlog server-ID** | Non-overlapping ranges 5600–6099 enforced by CI lint + base image template | Same ranges enforced by `JobConfig`; KC uses reserved 5500–5599 |
| **Kafka** | Confluent Kafka Cloud (managed) | `cp-kafka:7.6.1` KRaft container; single broker; `localhost:9092` |
| **Kafka Connect** | Confluent managed KC for SFTP (25) + SingleStore (1); being replaced for 74 CDC connectors | Local KC container + Debezium + custom SMTs; side-by-side comparison only |
| **Checkpointing** | S3 bucket (per-job `checkpointing.dir`); IRSA permissions | In-memory / local (no S3 in compose); same code config (30 s interval, EXACTLY_ONCE) |
| **CI/CD** | Jenkins (image build, `yq` delete, variant select) + ArgoCD (deploy/restart) | `./gradlew all` (build → compose restart → deploy connectors → CTs) |
| **Monitoring** | Datadog via `<datadog-tf-repo>` (16 monitors, 2 dashboards; target: ~600) | Flink Dashboard `:8081` + Kafka UI `:8080` + KC REST `:8083` |
| **Images** | Per-variant fat-jar images built by Jenkins; separate base images per API type | Shadow-JAR fat-jars built locally; bundled into JM/TM container at runtime |
| **Java version** | 17 (Flink jobs); SMT not applicable (no KC in production Flink path) | 17 (Flink jobs); 11 (KC SMTs — cp-kafka-connect 7.6.1 constraint) |
| **IAM / Security** | RDS IAM tokens, IRSA, binlog lease management | No IAM; plain `flink`/`flink` credentials; no rotation testing possible |
| **Re-snapshot** | `upgradeMode: stateless` + `restartNonce` in ArgoCD (post-migration) | Cancel job, delete state, re-submit (`flink cancel <JOB_ID>` + `flink run`) |
| **State backend** | RocksDB (production recommendation; configured via cluster config) | In-memory / HashMapStateBackend (default for local demo) |
| **Kafka topic naming** | `<tribe>.<schema>.<table>` with per-variant prefixes across all 26 teams | `poc.cdc.<variant>.<table>` (single `poc_db` schema) |
| **Observability ownership** | Three-way: Module Owner (KC module) / Flink Platform Team (Flink module) / each tribe (config.tf) | Single developer; no ownership model needed |
| **Scale** | 74 CDC connectors → 26 teams → ~600 monitors at end-state | 1 schema (`poc_db`), 1 table (`orders`), 5 variants, 60 unit tests + CT per variant |
| **YAML Pipeline submission** | `flink-cdc.sh` via init-container or `kubectl exec`; `FlinkDeployment` comes up with empty JM until wired | `flink-cdc-submitter` container runs `flink-cdc.sh` automatically on JM ready |

---

## References

- Apache Flink 2.2.0 documentation
- Apache Flink CDC 3.6 documentation
- Apache Flink CDC project home
- Debezium MySQL Connector (via Flink CDC 3.6 / `flink-cdc-connectors`)
- `flink-cdc-poc/CHECKPOINT_CONFIG.md` — checkpoint semantics, monitoring, troubleshooting
- `flink-cdc-poc/SAVEPOINT_RUNBOOK.md` — safe upgrade workflows, state recovery
- `flink-cdc-poc/KAFKA_CONNECT.md` — KC CDC variants, SMTs, Flink vs KC comparison
