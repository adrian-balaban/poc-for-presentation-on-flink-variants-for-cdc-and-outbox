# CLAUDE.md — flink-cdc-poc

## What this project is

A 5-variant Gradle multi-project POC demonstrating Apache Flink MySQL CDC as a replacement for Kafka Connect CDC connectors. Each subproject is one variant; all share the `common` module and the same Docker infra.

## Build

```bash
./gradlew shadowJar          # build all fat-jars
./gradlew :variant-flink-datastream-api-v1-cdc-job:shadowJar   # single variant
./gradlew test               # run all tests (unit + component)
./gradlew all                # build all, restart Docker Compose, run component tests
```

Requires Java 17. Gradle wrapper is included — no local Gradle install needed.

**Note:** Component tests require Docker to be running (`cd docker && docker compose up -d`). If Docker is unavailable, component tests are skipped gracefully (shown as yellow ⭕ in test explorer).

### Full Integration Test (all)

The `all` task orchestrates a complete build-and-test cycle:

1. **Builds all modules** — `./gradlew clean build -x test`
2. **Restarts Docker Compose** — `cd docker && docker compose down && docker compose up -d`
3. **Waits for services** — 5-second pause to allow Docker containers to start
4. **Runs component tests** — `./gradlew :component-tests:test`

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

**Docker (default):**
```bash
cd docker && docker compose up -d    # MySQL + Kafka + Flink JM+TM + Kafka UI
cd docker && docker compose down     # tear down (data lost)
```

**Podman (alternative):**
```bash
cd docker && podman-compose -f podman-compose.yml up -d
cd docker && podman-compose -f podman-compose.yml down -v
```

Services:
- Flink Dashboard: http://localhost:8081
- Kafka UI:        http://localhost:8080
- MySQL:           localhost:3306  user=flink  password=flink  db=poc_db

MySQL binlog is enabled via `--log-bin=mysql-bin --binlog-format=ROW --binlog-row-image=FULL`.

## Adding a new variant

1. Create `variant-<name>/build.gradle` — apply shadow plugin, depend on `:common`, add CDC deps
2. Add entry class under `src/main/java/poc/<name>/`
3. Register in `settings.gradle`: `include 'variant-<name>'`
4. Assign a non-overlapping MySQL server-ID range (see table below) and document it in `docker-compose.yml`

## Server-ID ranges (do not overlap)

| Variant | Range |
|---------|-------|
| variant-flink-datastream-api-v1-outbox-job | 5600–5699 |
| variant-flink-cdc-yaml-pipeline-cdc-job | 5700–5709 |
| variant-flink-sql-api-cdc-job | 5800–5899 |
| variant-flink-datastream-api-v1-cdc-job | 5900–5999 |
| variant-flink-table-api-cdc-job | 6000–6099 |
| _reserved_ | 5500–5599 |

Ranges must be non-overlapping because Flink CDC 3.x incremental snapshot allocates IDs for parallel readers and restart attempts. A single ID collides on restart because the previous MySQL binlog lease hasn't expired.

## Configuration

All variants read from env vars via `poc.common.config.JobConfig.fromEnv()`. Defaults work against the Docker Compose setup without any extra config.

Key vars: `MYSQL_HOST`, `MYSQL_PORT`, `MYSQL_USER`, `MYSQL_PASSWORD`, `MYSQL_DATABASE`, `MYSQL_TABLES`, `MYSQL_SERVER_ID`, `KAFKA_BOOTSTRAP`, `KAFKA_TOPIC_PREFIX`.

## What to avoid

- Do not hardcode versions in subproject `build.gradle` files — always use `rootProject.ext.*`
- Do not reuse server-ID ranges across variants — MySQL will reject duplicate replica IDs
- Do not leave `upgradeMode: stateless` permanently in Flink deployments — every restart would re-snapshot the full table
- Variant 5 (YAML Pipeline) has no Maven shade module — do not add Java sources to it; it is intentionally zero-code
- When using Podman: use `podman-compose.yml` (not `docker-compose.yml`) — it has bridge networking + rootless-compatible volume flags
- When using Podman from a snap-installed VS Code: the snap overrides `XDG_DATA_HOME`, which splits podman storage between VS Code terminals (`~/snap/code/<rev>/.local/share/containers`) and the rest of the system (`~/.local/share/containers`). Symptoms: healthchecks stuck in `(starting)` forever, compose app invisible outside VS Code, stale aardvark-dns entries causing "No route to host" between containers. Fix: pin `graphroot` in `~/.config/containers/storage.conf` (already done on this machine)

## Component tests

Each Flink variant and Kafka Connect variant has corresponding component tests in the `component-tests` subproject. Tests connect to existing Docker infrastructure (MySQL + Kafka) and run Flink in an embedded local cluster or connect to Kafka Connect REST API.

**Prerequisites:**
```bash
cd docker && docker compose up -d    # Start MySQL, Kafka, etc.
# For Kafka Connect tests only:
cd docker/kafka-connect && ./build-and-deploy.sh
```

**Run tests:**
```bash
# All tests (unit + component) — component tests auto-skip if Docker unavailable
./gradlew test

# Component tests only
./gradlew :component-tests:test

# Single component test
./gradlew :component-tests:test --tests "poc.component.DataStreamCdcTest"

# Or run everything (all Flink, Kafka Connect, components, restarts Docker):
./gradlew all
```

### Flink Variants

| Test class | Variant | Server-ID range | Status |
|---|---|---|---|
| `DataStreamCdcTest` | variant-flink-datastream-api-v1-cdc-job | 7000–7009, 7050–7059 | ✅ PASS |
| `TableApiCdcTest` | variant-flink-table-api-cdc-job | 7010–7019 | ✅ PASS |
| `SqlApiCdcTest` | variant-flink-sql-api-cdc-job | 7020–7039 | ✅ PASS |
| `DataStreamOutboxTest` | variant-flink-datastream-api-v1-outbox-job | 7040–7049 | ✅ PASS |
| `YamlPipelineCdcTest` | variant-flink-cdc-yaml-pipeline-cdc-job | n/a — manual only | ✅ PASS |

### Kafka Connect Variants

| Test class | Variant | Server-ID | Status |
|---|---|---|---|
| `KafkaConnectDataStreamTest` | kc-datastream-cdc | 5900 | ✅ PASS |
| `KafkaConnectTableApiTest` | kc-table-api-cdc | 6000 | ✅ PASS |
| `KafkaConnectSqlApiTest` | kc-sql-api-cdc | 5800 | ✅ PASS |
| `KafkaConnectOutboxTest` | kc-outbox-cdc | 5600 | ✅ PASS |
| `KafkaConnectYamlPipelineTest` | kc-yaml-pipeline-cdc | 5700 | ✅ PASS |

The 7000–7099 block is reserved exclusively for Flink component tests (not in production ranges above).

**Docker availability:**
- If Docker is running: tests pass (✅ green in VS Code)
- If Docker is stopped: tests skip gracefully (⭕ yellow in VS Code)
- If Kafka Connect is not available: Kafka Connect tests skip gracefully

## Production Deployment

All variants are configured with **exactly-once semantics** checkpoints (30-second interval, 60-second timeout). For safe job upgrades and state recovery, see the runbooks below.

## Kafka Connect Variants (Alternative CDC Approach)

In addition to Flink variants, this POC includes **Kafka Connect** versions of all 5 CDC patterns using Debezium's MySQL connector with custom Single Message Transformers (SMTs):

**Quick start:**
```bash
cd docker/kafka-connect
./build-and-deploy.sh
```

**What's included:**
- 5 connectors mirroring Flink variants (DataStream, Table API, SQL API, Outbox, YAML Pipeline)
- Custom SMT code for enrichment and dynamic topic routing
- Maven project for building SMT JARs
- Deployment script with health checks
- Unit tests for transformations

**Key differences from Flink:**
- Kafka Connect: Stateless workers, horizontal scaling, REST API management
- Flink: Full state API, windowing, timers, fine-grained control
- Same Debezium source and Kafka sink
- Useful for benchmarking and understanding CDC trade-offs

See [KAFKA_CONNECT.md](./KAFKA_CONNECT.md) and [docker/KAFKA_CONNECT_QUICKSTART.md](./docker/KAFKA_CONNECT_QUICKSTART.md).

## See Also

- [**CHECKPOINT_CONFIG.md**](./CHECKPOINT_CONFIG.md) — Flink 2.2 checkpoint semantics, monitoring, troubleshooting
- [**SAVEPOINT_RUNBOOK.md**](./SAVEPOINT_RUNBOOK.md) — 5-phase safe upgrade workflow, server-ID management, disaster recovery
- [**KAFKA_CONNECT.md**](./KAFKA_CONNECT.md) — Kafka Connect CDC variants, custom SMTs, detailed comparison

## Context

This POC supports the architecture described in `../markdown/proposalA/`. The five variants map directly to the decision matrix in section 5 of that proposal. The recommended production path is the **DataStream CDC shared-job model** (Jereczek, May 2026): one parametrisable image per tribe, Helm-chart env overrides, no per-tribe Java fork.
