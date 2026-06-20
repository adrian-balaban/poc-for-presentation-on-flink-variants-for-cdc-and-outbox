# CLAUDE.md — flink-cdc-poc

## What this project is

A 5-variant Gradle multi-project POC demonstrating Apache Flink MySQL CDC as a replacement for Kafka Connect CDC connectors. Each subproject is one variant; all share the `common` module and the same Podman infra.

## Build

```bash
./gradlew shadowJar          # build all fat-jars
./gradlew :variant-flink-datastream-api-v1-cdc-job:shadowJar   # single variant
./gradlew test               # run all tests (unit + component)
./gradlew all                # build all, restart Podman Compose, run component tests
```

Requires Java 17. Gradle wrapper is included — no local Gradle install needed.

### Code formatting (Spotless — Google Java Format)

All Java code must be formatted with Google Java Format via [Spotless](https://github.com/diffplug/spotless).

```bash
./gradlew fmt       # Format all Java files (auto-fix)
./gradlew fmtCheck  # Check formatting without applying changes
```

Formatting is enforced in the `check` task — `./gradlew build` will fail if code is unformatted. IDE integration: most IDEs (VS Code, IntelliJ) have formatter plugins; the easiest flow is to run `./gradlew fmt` before committing.

**Note:** Component tests require Podman to be running (`cd local-development && podman-compose -f podman-compose.yml up -d`). If the stack is unavailable, component tests are skipped gracefully (shown as yellow ⭕ in test explorer).

### Full Integration Test (all)

The `all` task orchestrates a complete build-and-test cycle:

1. **Builds all modules** — `./gradlew clean build -x test shadowJar` (includes the variant fat-jars the component tests submit)
2. **Restarts Podman Compose** — `cd local-development && podman-compose -f podman-compose.yml down -v && ... up -d` (`down` exit is ignored — "container not found" on first run is normal)
3. **Waits for services** — polls MySQL + Kafka + Kafka Connect + Flink (up to 180 s); if Flink container doesn't exist (image build failure) the task throws with a diagnostic message rather than silently skipping
4. **Builds Kafka Connect SMTs** — `./gradlew :kafka-connect-smts:shadowJar`
5. **Deploys Kafka Connect connectors** — REST API (with `DB_HOST=mysql` for bridge networking)
6. **Runs component tests** — `./gradlew :component-tests:test`

Usage:
```bash
./gradlew all
```

The task runs all steps sequentially, stopping on any failure. Useful for CI/CD pipelines or full validation before deployment.

## Versions (change in root build.gradle only)

| Variable | Value |
|----------|-------|
| `flinkVersion` | `2.2.0` |
| `flinkCdcVersion` | `3.6.0-2.2` |
| `flinkKafkaVersion` | `5.0.0-2.2` |
| `kafkaVersion` | `3.7.0` |

All subproject `build.gradle` files reference these via `rootProject.ext.*` — never hardcode versions in subprojects.

**Java versions:** Flink job modules target Java 17. `kafka-connect-smts` targets Java 11 — `cp-kafka-connect:7.6.1` ships JDK 11 and refuses class files compiled for 17.

## Module map

| Module | Entry class | Notes |
|--------|-------------|-------|
| `common` | — | Shared config, deserializer, sink factory. No fat-jar. |
| `variant-flink-datastream-api-v1-cdc-job` | `poc.datastream.DataStreamCdcJob` | Most flexible; supports per-row routing |
| `variant-flink-table-api-cdc-job` | `poc.tableapi.TableApiCdcJob` | DDL-driven; good for future SQL joins |
| `variant-flink-sql-api-cdc-job` | `poc.sqlapi.SqlApiCdcJob` | StatementSet → single JobGraph |
| `variant-flink-datastream-api-v1-outbox-job` | `poc.outbox.OutboxJob` | Reads `outbox_events` table; routes by `destination` field |
| `variant-flink-cdc-yaml-pipeline-cdc-job` | — | No Java; submitted via `flink-cdc.sh pipeline.yaml` |

## Local infra

```bash
cd local-development && podman-compose -f podman-compose.yml up -d --build    # MySQL + Kafka + Flink JM+TM + Kafka Connect + Kafka UI + Prometheus + Grafana
cd local-development && podman-compose -f podman-compose.yml down -v         # tear down (data lost)
```

Services:
- Flink Dashboard: http://localhost:8081
- Kafka UI:        http://localhost:8080
- Kafka Connect:   http://localhost:8083
- Grafana:         http://localhost:3001 (admin/admin)
- Prometheus:      http://localhost:9090
- MinIO:           http://localhost:9001 (minioadmin/minioadmin, bucket: flink-checkpoints)
- MySQL:           localhost:3306  user=flink  password=flink  db=poc_db

MySQL binlog is enabled via `--log-bin=mysql-bin --binlog-format=ROW --binlog-row-image=FULL`.

## Monitoring — Prometheus metrics & Grafana dashboards

All Flink services (JobManager + TaskManager) export Prometheus metrics natively via the `flink-metrics-prometheus` plugin. The local stack includes Prometheus scraper and Grafana for dashboard/alerting.

### Metrics export

- **Flink JobManager** — http://localhost:9249/metrics (port 9249 on bridge network)
- **Flink TaskManager** — http://localhost:9250/metrics (on host; internally 9249 on bridge)

Metrics are scraped by Prometheus every 15 seconds and stored locally.

### Grafana dashboard

**Dashboard URL:** http://localhost:3001/d/flink-cdc-poc-monitoring (admin/admin)

Managed by Terraform (`terraform/dashboard.tf` reads `grafana/provisioning/dashboards/flink-cdc-monitoring.json`). Panels:
- 3 stat panels — Restart Loop, Checkpoint Duration, Checkpoint Failures (mirrors `rtdp-datadog-tf`)
- 4 text placeholders — KC monitors #4–#7, pending Spike S1
- 2 timeseries — JVM heap, job restarts

### Alert rules

**Alerts URL:** http://localhost:3001/alerting/list

Three rules managed by Terraform (`terraform/alerts.tf`):

| Alert | Threshold | Severity |
|-------|-----------|----------|
| Flink Restart Loop | `increase(numRestarts[5m])` > 3 | critical |
| Flink Checkpoint Duration High | `lastCheckpointDuration` > 180 000 ms | warning |
| Flink Checkpoint Failures | `increase(numberOfFailedCheckpoints[5m])` > 3 | critical |

Contact point: `flink-cdc-poc-email`. Notification policy groups by `alertname` + `job_name`.

Each rule has `__dashboardUid__ = "flink-cdc-poc-monitoring"` and `__panelId__` set (1/2/3), so alert state changes appear as annotation markers on the corresponding stat panels. The dashboard JSON includes an "Alert state changes" annotation layer for timeseries panels.

**Do not change the Grafana admin password through the UI.** Terraform uses `admin:admin`. If changed accidentally: `podman exec grafana grafana cli admin reset-admin-password admin && podman restart grafana`.

### Terraform

`./gradlew all` runs `terraform apply -auto-approve` automatically after Grafana is healthy. To apply manually:

```bash
cd local-development/terraform
terraform init   # first time only; downloads grafana/grafana provider ~3.4
terraform apply
```

`terraform apply` is idempotent — safe to re-run. State in `terraform/terraform.tfstate` (local backend).

Resources managed: dashboard · folder "Flink CDC POC" · 3 alert rules · contact point · notification policy.

**Datasource ownership:** The Prometheus datasource (uid `prometheus`) is auto-provisioned by Grafana from `local-development/grafana/provisioning/datasources/prometheus.yml`. Terraform reads it as a `data` source (`data.grafana_data_source.prometheus`) — it does **not** create it. Adding it as a Terraform `resource` would cause a 409 conflict on every `all` run.

## Adding a new variant

1. Create `variant-<name>/build.gradle` — apply shadow plugin, depend on `:common`, add CDC deps
2. Add entry class under `src/main/java/poc/<name>/`
3. Register in `settings.gradle`: `include 'variant-<name>'`
4. Assign a non-overlapping MySQL server-ID range (see table below) and document it in `podman-compose.yml`

## Server-ID ranges (do not overlap)

| Variant | Range |
|---------|-------|
| variant-flink-datastream-api-v1-outbox-job | 5600–5699 |
| variant-flink-cdc-yaml-pipeline-cdc-job | 5700–5709 |
| variant-flink-sql-api-cdc-job | 5800–5899 |
| variant-flink-datastream-api-v1-cdc-job | 5900–5999 |
| variant-flink-table-api-cdc-job | 6000–6099 |
| Kafka Connect connectors (kc-*) | 5500–5599 |

Ranges must be non-overlapping because Flink CDC 3.x incremental snapshot allocates IDs for parallel readers and restart attempts. A single ID collides on restart because the previous MySQL binlog lease hasn't expired.

## Configuration

All variants read from env vars via `poc.common.config.JobConfig.fromEnv()`. Defaults work against the Podman Compose setup without any extra config.

Key vars: `MYSQL_HOST`, `MYSQL_PORT`, `MYSQL_USER`, `MYSQL_PASSWORD`, `MYSQL_DATABASE`, `MYSQL_TABLES` (default: `poc_db.orders,poc_db.customers,poc_db.outbox_events`), `KAFKA_BOOTSTRAP`, `KAFKA_TOPIC_PREFIX`.

Per-variant server-ID ranges are also env-overridable (defaults match the [server-ID table](#server-id-ranges-do-not-overlap)): `MYSQL_SERVER_ID` (DataStream, `5900-5999`), `MYSQL_OUTBOX_SERVER_ID` (Outbox, `5600-5699`), `MYSQL_TABLE_API_SERVER_ID` (Table API, `6000-6099`), `MYSQL_SQL_API_ORDERS_SERVER_ID` (SQL API orders, `5800-5849`), `MYSQL_SQL_API_CUSTOMERS_SERVER_ID` (SQL API customers, `5850-5899`). All are validated as non-blank in `JobConfig.Builder.build()`.

## What to avoid

- Do not hardcode versions in subproject `build.gradle` files — always use `rootProject.ext.*`
- Do not reuse server-ID ranges across variants — MySQL will reject duplicate replica IDs
- Do not leave `upgradeMode: stateless` permanently in Flink deployments — every restart would re-snapshot the full table
- Variant 5 (YAML Pipeline) has no Maven shade module — do not add Java sources to it; it is intentionally zero-code
- Do not compile `kafka-connect-smts` for Java 17 — `cp-kafka-connect:7.6.1` runs Java 11 and refuses class files with major version 61 (`UnsupportedClassVersionError`); keep `sourceCompatibility = VERSION_11` in that subproject
- When deploying Kafka Connect connectors on Podman bridge, pass `DB_HOST=mysql` to `deploy-connectors.sh` — the Connect container cannot reach MySQL via `localhost`; `./gradlew all` does this automatically
- Do not remove `KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR: 1` and `KAFKA_TRANSACTION_STATE_LOG_MIN_ISR: 1` from the Kafka service in `podman-compose.yml` — without them, a single-broker KRaft cluster cannot serve `InitProducerId` requests, causing Flink's exactly-once Kafka sinks to restart-loop indefinitely with `TimeoutException: Timeout expired after 60000ms while awaiting InitProducerId`
- Do not add `DEFAULT 'PENDING'` (or any non-null default) to the `orders.status` column — Debezium 1.9.x applies column DEFAULT values when serialising null, so `DEFAULT 'PENDING'` causes CDC events to carry `"status":"PENDING"` instead of `"status":null` for explicitly-null inserts. The column is intentionally defined without a default (`status VARCHAR(1024)`) so that null values are faithfully preserved in CDC output
- When using Podman from a snap-installed VS Code: the snap overrides `XDG_DATA_HOME`, which splits podman storage between VS Code terminals (`~/snap/code/<rev>/.local/share/containers`) and the rest of the system (`~/.local/share/containers`). Symptoms: healthchecks stuck in `(starting)` forever, compose app invisible outside VS Code, stale aardvark-dns entries causing "No route to host" between containers. Fix: pin `graphroot` in `~/.config/containers/storage.conf` (already done on this machine)

## Component tests

Each Flink variant and Kafka Connect variant has corresponding component tests in the `component-tests` subproject. Flink tests submit fat-jars to the real Flink JobManager container via the REST API (jobs visible at http://localhost:8081 during the test run). Kafka Connect tests use the Kafka Connect REST API.

**Prerequisites:**
```bash
cd local-development && podman-compose -f podman-compose.yml up -d
```

**Run tests:**
```bash
# All tests (unit + component) — component tests auto-skip if stack unavailable
./gradlew test

# Component tests only
./gradlew :component-tests:test

# Single component test
./gradlew :component-tests:test --tests "poc.component.DataStreamCdcTest"

# Or run everything (all Flink, Kafka Connect, components, restarts Podman stack):
./gradlew all
```

### Flink Variants

Tests submit the variant fat-jar to `localhost:8081`, wait for RUNNING, assert Kafka output, then cancel. Server-IDs come from `JobConfig` defaults (production ranges), overridable per the env vars in [Configuration](#configuration).

| Test class | Variant | Server-ID range | Kafka topic | Status |
|---|---|---|---|---|
| `DataStreamCdcTest` | variant-flink-datastream-api-v1-cdc-job | 5900–5999 (`MYSQL_SERVER_ID` default) | `poc.cdc.datastream` | ✅ PASS |
| `TableApiCdcTest` | variant-flink-table-api-cdc-job | 6000–6099 (`MYSQL_TABLE_API_SERVER_ID` default) | `poc.cdc.table-api` | ✅ PASS |
| `SqlApiCdcTest` | variant-flink-sql-api-cdc-job | 5800–5899 (`MYSQL_SQL_API_*_SERVER_ID` defaults) | `poc.cdc.sql-api.orders` | ✅ PASS |
| `DataStreamOutboxTest` | variant-flink-datastream-api-v1-outbox-job | 5600–5699 (`MYSQL_OUTBOX_SERVER_ID` default) | `poc.cdc.outbox` | ✅ PASS |
| `YamlPipelineCdcTest` | variant-flink-cdc-yaml-pipeline-cdc-job | 5700–5709 (submitter container) | `poc.cdc.yaml.orders` | ✅ PASS |

### Kafka Connect Variants

| Test class | Variant | Server-ID | Status |
|---|---|---|---|
| `KafkaConnectDataStreamTest` | kc-datastream-cdc | 5510 | ✅ PASS |
| `KafkaConnectTableApiTest` | kc-table-api-cdc | 5520 | ✅ PASS |
| `KafkaConnectSqlApiTest` | kc-sql-api-cdc | 5530 | ✅ PASS |
| `KafkaConnectOutboxTest` | kc-outbox-cdc | 5550 | ✅ PASS |
| `KafkaConnectYamlPipelineTest` | kc-yaml-pipeline-cdc | 5540 | ✅ PASS |

**Stack availability:**
- If Podman stack is running (MySQL + Kafka + Flink JM): tests pass (✅ green in VS Code)
- If Podman stack is stopped: tests skip gracefully (⭕ yellow in VS Code)
- If Flink JM is not available: Flink tests skip gracefully
- If Kafka Connect is not available: Kafka Connect tests skip gracefully

**Note:** Flink jobs submitted by component tests are **not cancelled** after the test — they remain visible at http://localhost:8081/#/job/running for the lifetime of the stack. Tests submit via `FlinkTestBase.ensureJobRunning()`, which reuses an already-RUNNING job of the same name instead of resubmitting — a second instance of the same variant would collide on its MySQL server-id. Because all 5 Flink jobs and all 5 Kafka Connect connectors run **simultaneously**, every consumer needs its own server-ID range: the Kafka Connect connectors use the dedicated `5500–5599` range, and `OutboxJob` defaults to its own `5600–5699` range (via `MYSQL_OUTBOX_SERVER_ID`) instead of sharing the `MYSQL_SERVER_ID` default (`5900–5999`) with `DataStreamCdcJob`.

## Production Deployment

All variants are configured with **exactly-once semantics** checkpoints (30-second interval, 60-second timeout). For safe job upgrades and state recovery, see the runbooks below.

## Kafka Connect Variants (Alternative CDC Approach)

In addition to Flink variants, this POC includes **Kafka Connect** versions of all 5 CDC patterns using Debezium's MySQL connector with custom Single Message Transformers (SMTs):

**Quick start:**
```bash
./gradlew all
```

**What's included:**
- 5 connectors mirroring Flink variants (DataStream, Table API, SQL API, Outbox, YAML Pipeline)
- Custom SMT code for enrichment and dynamic topic routing
- Gradle project for building SMT JARs (Java 11 target)
- Deployment script with health checks and bridge-network hostname support
- Unit tests for transformations

**Key differences from Flink:**
- Kafka Connect: Stateless workers, horizontal scaling, REST API management
- Flink: Full state API, windowing, timers, fine-grained control
- Same Debezium source and Kafka sink
- Useful for benchmarking and understanding CDC trade-offs

See [KAFKA_CONNECT.md](./KAFKA_CONNECT.md) and [local-development/KAFKA_CONNECT_QUICKSTART.md](./local-development/KAFKA_CONNECT_QUICKSTART.md).

## Code Quality Analysis

**Checkstyle** — Code style and formatting rules (imports, naming, indentation)
```bash
./gradlew codeQuality          # Run on all subprojects
./gradlew :MODULE:checkstyleMain  # Single module
```

Reports are generated in `build/reports/checkstyle/` for each subproject.

**Configuration:** `config/checkstyle/checkstyle.xml` — customize rules here (default includes unused import detection).

To add SpotBugs or OWASP Dependency Check in the future, see the archived configuration files in `config/`.

## See Also

- [**FLINK_CHECKPOINT_CONFIG.md**](./FLINK_CHECKPOINT_CONFIG.md) — Flink 2.2 checkpoint semantics, monitoring, troubleshooting
- [**FLINK_SAVEPOINT_RUNBOOK.md**](./FLINK_SAVEPOINT_RUNBOOK.md) — 5-phase safe upgrade workflow, server-ID management, disaster recovery
- [**KAFKA_CONNECT.md**](./KAFKA_CONNECT.md) — Kafka Connect CDC variants, custom SMTs, detailed comparison
- [**kafka-connect-at-scale-74-connectors-migration.md**](./kafka-connect-at-scale-74-connectors-migration.md) — presentation: real-world 74-connector migration case study (EN)
- [**kafka-connect-at-scale-74-connectors-migration.ro.md**](./kafka-connect-at-scale-74-connectors-migration.ro.md) — traducere română a prezentării

## Context

This POC supports the architecture described in `../markdown/proposalA/`. The five variants map directly to the decision matrix in section 5 of that proposal. The recommended production path is the **DataStream CDC shared-job model** : one parametrisable image per tribe, Helm-chart env overrides, no per-tribe Java fork.
