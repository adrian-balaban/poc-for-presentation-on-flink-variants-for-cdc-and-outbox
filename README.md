# Flink CDC&Outbox Connectors POC — Kafka Connect vs Flink for MySQL 

A working proof-of-concept demonstrating :
- **four ways to migrate a MySQL CDC connector from Kafka Connect to Apache Flink**. 
- **one way to migrate a MySQL Outbox connector from Kafka Connect to Apache Flink**. 


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
| Java | 17 |
| Gradle | 8.7 |

---

## Prerequisites

- Docker + Docker Compose
- Java 17+
- `flink-cdc.sh` on PATH for variant 5

---

## Quick Start

### 1. Start infrastructure

```bash
cd docker
docker compose up -d
```

Services started:
- MySQL at `localhost:3306` (user: `flink`, password: `flink`, db: `poc_db`)
- Kafka at `localhost:9092`
- Flink Dashboard at [http://localhost:8081](http://localhost:8081)
- Kafka UI at [http://localhost:8080](http://localhost:8080)

### 2. Build all variants

```bash
./gradlew shadowJar
```

Each variant produces a fat-jar at `<module>/build/libs/<module>.jar`.

### 3. Submit a variant

The Docker Compose stack uses `network_mode: host`, so the job's default config
(`MYSQL_HOST=localhost`, `KAFKA_BOOTSTRAP=localhost:9092`) already works inside the
JobManager container — no extra config needed. To override a value, pass it to
`docker exec` with `-e` (it sets the container env that `JobConfig.fromEnv()` reads),
e.g. `docker exec -e MYSQL_HOST=other-host flink-jm flink run ...`.

**Variant 1 — DataStream CDC:**
```bash
docker cp variant-flink-datastream-api-v1-cdc-job/build/libs/variant-flink-datastream-api-v1-cdc-job.jar flink-jm:/tmp/
docker exec flink-jm flink run /tmp/variant-flink-datastream-api-v1-cdc-job.jar
```

**Variant 2 — Table API:**
```bash
docker cp variant-flink-table-api-cdc-job/build/libs/variant-flink-table-api-cdc-job.jar flink-jm:/tmp/
docker exec flink-jm flink run /tmp/variant-flink-table-api-cdc-job.jar
```

**Variant 3 — SQL API (multi-table StatementSet):**
```bash
docker cp variant-flink-sql-api-cdc-job/build/libs/variant-flink-sql-api-cdc-job.jar flink-jm:/tmp/
docker exec flink-jm flink run /tmp/variant-flink-sql-api-cdc-job.jar
```

**Variant 4 — Outbox:**
```bash
docker cp variant-flink-datastream-api-v1-outbox-job/build/libs/variant-flink-datastream-api-v1-outbox-job.jar flink-jm:/tmp/
docker exec flink-jm flink run /tmp/variant-flink-datastream-api-v1-outbox-job.jar
```

**Variant 5 — YAML Pipeline (no Java, submit via flink-cdc.sh):**
```bash
MYSQL_HOST=localhost KAFKA_BOOTSTRAP=localhost:9092 \
  flink-cdc.sh variant-flink-cdc-yaml-pipeline-cdc-job/src/main/resources/pipeline.yaml
```

### 4. Trigger CDC events

```sql
-- connect to MySQL
docker exec -it mysql mysql -uflink -pflink poc_db

-- insert / update to generate binlog events
INSERT INTO orders (customer_id, amount, status) VALUES (1, 500.00, 'PENDING');
UPDATE orders SET status = 'SHIPPED' WHERE id = 1;
DELETE FROM orders WHERE id = 1;
```

### 5. Watch the output

- **Flink Dashboard** → [http://localhost:8081](http://localhost:8081) — running jobs, task slots
- **Kafka UI** → [http://localhost:8080](http://localhost:8080) — topics `poc.cdc.*`

---

## Full Integration Test

For end-to-end validation, use the `all` goal:

```bash
./gradlew all
```

This orchestrates a complete build-and-test cycle:

1. **Builds all modules** — `./gradlew clean build -x test`
2. **Restarts Docker Compose** — `cd docker && docker compose down && docker compose up -d`
3. **Waits for services** — 5-second pause to allow Docker containers to start
4. **Runs component tests** — `./gradlew :component-tests:test`

The task runs all steps sequentially, stopping on any failure. Useful for CI/CD pipelines or full validation before deployment.

---

## Environment Variables

All variants read configuration from environment variables with sensible defaults for local Docker use.

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
    ├── docker-compose.yml
    └── mysql-init/init.sql             # schema + seed data
```

---

## Production Deployment

All variants are configured with **exactly-once semantics** and **30-second checkpoint intervals**. For production deployments:

- **Safe upgrades with savepoints** → See [SAVEPOINT_RUNBOOK.md](./SAVEPOINT_RUNBOOK.md)
  - 5-phase workflow: create → verify → cancel → upgrade → resume
  - Handles MySQL binlog lease expiration and server-ID collision prevention
  - Examples: memory leak fixes, corruption recovery, parallelism scaling

- **Checkpoint configuration & monitoring** → See [CHECKPOINT_CONFIG.md](./CHECKPOINT_CONFIG.md)
  - Flink 2.2 checkpoint semantics and production checklist
  - REST API examples and Flink Dashboard monitoring
  - Troubleshooting: slow/hung checkpoints, duplicate messages

---

## Key Design Decisions

**Server-ID ranges per variant** — Flink CDC 3.x incremental snapshot allocates multiple server IDs for parallel readers and restarts. A single integer collides on restart because the previous MySQL binlog lease hasn't expired. Each variant owns a non-overlapping range.

**One fat-jar per variant** — built with the Shadow plugin. Enables independent upgrades; no fleet-wide coordinated releases.

**StatementSet in variant 3** — all `INSERT` statements compile into a single JobGraph: one checkpoint, one recovery unit, exactly-once semantics across multiple tables.

**All config via env vars** — makes each variant drop-in compatible with the Helm-override shared-job model described in the architecture proposal.

---

## References

### Project Documentation
- [CLAUDE.md](./CLAUDE.md) — module structure, server-ID ranges, component tests
- [CHECKPOINT_CONFIG.md](./CHECKPOINT_CONFIG.md) — checkpoint semantics, monitoring, troubleshooting
- [SAVEPOINT_RUNBOOK.md](./SAVEPOINT_RUNBOOK.md) — safe upgrade workflows, state recovery
- Architecture proposal: `../markdown/proposalA/`

### External
- [Apache Flink CDC documentation](https://nightlies.apache.org/flink/flink-cdc-docs-stable/)
- [MySQL CDC Connector](https://nightlies.apache.org/flink/flink-cdc-docs-stable/docs/connectors/pipeline-connectors/mysql/)
- [Flink Kafka Connector](https://nightlies.apache.org/flink/flink-docs-stable/docs/connectors/datastream/kafka/)
- [Flink Checkpointing](https://nightlies.apache.org/flink/flink-docs-stable/docs/dev/datastream/fault_tolerance/checkpointing/)
