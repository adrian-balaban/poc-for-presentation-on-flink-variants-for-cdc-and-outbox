# Flink CDC & Outbox Connectors POC — Kafka Connect vs Flink for MySQL

A working proof-of-concept demonstrating:
- **5 ways to implement MySQL CDC** with Apache Flink (vs Kafka Connect)
- **5 bonus Kafka Connect variants** for side-by-side comparison

Each variant is an independent Gradle subproject that builds its own fat-jar and connects to the same MySQL + Kafka infra.

---

## Variants

| # | Module | API | Java | Server-ID range | Best for |
|---|--------|-----|------|-----------------|----------|
| 1 | `variant-flink-datastream-api-v1-cdc-job` | DataStream | ~100 lines | 5900–5999 | CDC + custom enrichment/routing |
| 2 | `variant-flink-table-api-cdc-job` | Table API | ~220 lines | 6000–6099 | CDC with future SQL joins/aggregations |
| 3 | `variant-flink-sql-api-cdc-job` | SQL API (StatementSet) | ~210 lines | 5800–5899 | Multi-table CDC → single JobGraph |
| 4 | `variant-flink-datastream-api-v1-outbox-job` | DataStream | ~150 lines | 5600–5699 | Transactional outbox, per-row topic routing |
| 5 | `variant-flink-cdc-yaml-pipeline-cdc-job` | YAML Pipeline | 0 lines | 5700–5709 | Simple CDC, zero Java |

---

## Stack

| Component | Version |
|-----------|---------|
| Apache Flink | 2.2.0 |
| Flink CDC | 3.6.0-2.2 |
| flink-connector-kafka | 5.0.0-2.2 |
| MySQL | 8.0 |
| Kafka | KRaft (Confluent 7.6.1) |
| Java | 17 (Flink jobs) / 11 (Kafka Connect SMTs) |
| Gradle | 8.7 |

---

## Prerequisites

- **Podman + podman-compose** (the only supported container engine)
- Java 17+
- (Optional) `flink-cdc.sh` on PATH for variant 5

Install podman-compose if needed:
```bash
pip install podman-compose
```

---

## Quick Start

### 1. Start infrastructure

```bash
cd docker
podman-compose -f podman-compose.yml up -d
```

Services started:
- MySQL at `localhost:3306` (user: `flink`, password: `flink`, db: `poc_db`)
- Kafka at `localhost:9092`
- Flink Dashboard at http://localhost:8081
- Kafka UI at http://localhost:8080
- Kafka Connect at http://localhost:8083

### 2. Build all variants

```bash
./gradlew shadowJar
```

Each variant produces a fat-jar at `<module>/build/libs/<module>.jar`.

### 3. Submit a variant

The Podman stack uses bridge networking; the JobManager container refers to other services by name.
Use `podman exec -e KEY=VALUE` to override `JobConfig.fromEnv()` defaults.

**DataStream CDC:**
```bash
podman cp variant-flink-datastream-api-v1-cdc-job/build/libs/variant-flink-datastream-api-v1-cdc-job.jar flink-jm:/tmp/
podman exec flink-jm flink run /tmp/variant-flink-datastream-api-v1-cdc-job.jar
```

**Table API:**
```bash
podman cp variant-flink-table-api-cdc-job/build/libs/variant-flink-table-api-cdc-job.jar flink-jm:/tmp/
podman exec flink-jm flink run /tmp/variant-flink-table-api-cdc-job.jar
```

**SQL API (multi-table StatementSet):**
```bash
podman cp variant-flink-sql-api-cdc-job/build/libs/variant-flink-sql-api-cdc-job.jar flink-jm:/tmp/
podman exec flink-jm flink run /tmp/variant-flink-sql-api-cdc-job.jar
```

**Outbox:**
```bash
podman cp variant-flink-datastream-api-v1-outbox-job/build/libs/variant-flink-datastream-api-v1-outbox-job.jar flink-jm:/tmp/
podman exec flink-jm flink run /tmp/variant-flink-datastream-api-v1-outbox-job.jar
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
- **Kafka UI** → http://localhost:8080 — topics `poc.cdc.*`

---

## Full Integration Test

For end-to-end validation, use the `all` goal:

```bash
./gradlew all
```

This orchestrates a complete build-and-test cycle:

1. Builds all modules — `./gradlew clean build -x test`
2. Restarts Podman Compose — `podman-compose -f podman-compose.yml down -v && ... up -d`
3. Builds Kafka Connect SMTs — `./gradlew :kafka-connect-smts:shadowJar`
4. Deploys Kafka Connect connectors — REST API deployment
5. Runs component tests — Flink + Kafka Connect tests

The task runs all steps sequentially, stopping on any failure.

---

## Environment Variables

All variants read configuration from environment variables with sensible defaults for local Podman use.

| Variable | Default | Description |
|----------|---------|-------------|
| `MYSQL_HOST` | `localhost` | MySQL hostname |
| `MYSQL_PORT` | `3306` | MySQL port |
| `MYSQL_USER` | `flink` | MySQL username |
| `MYSQL_PASSWORD` | `flink` | MySQL password |
| `MYSQL_DATABASE` | `poc_db` | Database name |
| `MYSQL_TABLES` | `poc_db.orders` | Fully-qualified table list |
| `MYSQL_SERVER_ID` | `5900-5999` | Binlog replica server-ID range |
| `KAFKA_BOOTSTRAP` | `localhost:9092` | Kafka bootstrap servers |
| `KAFKA_TOPIC_PREFIX` | `poc.cdc` | Topic prefix; variant name appended |

---

## Project Layout

```
flink-cdc-poc/
├── build.gradle                        # root — versions + shared config
├── settings.gradle
├── gradle/wrapper/
├── common/                             # shared: JobConfig, deserializer, routers, KafkaSinkFactory
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
└── docker/
    ├── podman-compose.yml
    └── mysql-init/init.sql             # schema + seed data
```

---

## Production Deployment

All variants are configured with **exactly-once semantics** and **30-second checkpoint intervals**. For production deployments:

- **Safe upgrades with savepoints** → See [SAVEPOINT_RUNBOOK.md](./SAVEPOINT_RUNBOOK.md)
  - 5-phase workflow: create → verify → cancel → upgrade → resume
  - Handles MySQL binlog lease expiration and server-ID collision prevention

- **Checkpoint configuration & monitoring** → See [CHECKPOINT_CONFIG.md](./CHECKPOINT_CONFIG.md)
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
- [CLAUDE.md](./CLAUDE.md) — module structure, server-ID ranges, component tests
- [CHECKPOINT_CONFIG.md](./CHECKPOINT_CONFIG.md) — checkpoint semantics, monitoring, troubleshooting
- [SAVEPOINT_RUNBOOK.md](./SAVEPOINT_RUNBOOK.md) — safe upgrade workflows, state recovery
- [KAFKA_CONNECT.md](./KAFKA_CONNECT.md) — Kafka Connect CDC variants, SMTs, comparison
- Architecture proposal: `../markdown/proposalA/`

### External
- [Apache Flink CDC documentation](https://nightlies.apache.org/flink/flink-cdc-docs-stable/)
- [MySQL CDC Connector](https://nightlies.apache.org/flink/flink-cdc-docs-stable/docs/connectors/pipeline-connectors/mysql/)
- [Flink Kafka Connector](https://nightlies.apache.org/flink/flink-docs-stable/docs/connectors/datastream/kafka/)
- [Flink Checkpointing](https://nightlies.apache.org/flink/flink-docs-stable/docs/dev/datastream/fault_tolerance/checkpointing/)
