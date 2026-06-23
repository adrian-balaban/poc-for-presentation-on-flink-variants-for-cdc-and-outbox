# Kafka Connect CDC Variants

This section documents the Kafka Connect alternatives to the Flink CDC variants. Each variant uses Debezium's MySQL CDC connector with custom Single Message Transformers (SMTs) for enrichment and routing.

## Overview

Five Kafka Connect connectors mirror the Flink variants:

| Connector | Variant | Purpose | SMT |
|-----------|---------|---------|-----|
| `kc-datastream-cdc` | DataStream API | Standard CDC with per-table topics | `EnrichmentTransform` |
| `kc-table-api-cdc` | Table API | DDL-style CDC | `EnrichmentTransform` |
| `kc-sql-api-cdc` | SQL API | StatementSet equivalent | `EnrichmentTransform` |
| `kc-outbox-cdc` | Outbox Pattern | Reads outbox_events, routes by destination | `OutboxRoutingTransform` |
| `kc-yaml-pipeline-cdc` | YAML Pipeline | Configuration-driven CDC | `EnrichmentTransform` |

## Architecture

```
MySQL binlog
    ↓
┌─────────────────────────────────────────┐
│  Debezium MySQL Connector               │
│  - Captures CDC events                  │
│  - Outputs as JSON (no schema)          │
└─────────────────────────────────────────┘
    ↓
┌─────────────────────────────────────────┐
│  Custom SMT (Single Message Transformer)│
│  - Enriches with variant metadata       │
│  - Routes to destination topics         │
│  - Adds routing information             │
└─────────────────────────────────────────┘
    ↓
Kafka Topics (poc.kc.*)
```

## Component Tests

Five component tests verify each Kafka Connect variant — testing the full pipeline from MySQL CDC to Kafka with SMT transformations applied.

**Run Kafka Connect component tests:**
```bash
# Prerequisite: start Kafka Connect (included in podman-compose.yml)

# Run only Kafka Connect tests
./gradlew :component-tests:test --tests "*KafkaConnect*"

# Run all component tests (Flink + Kafka Connect)
./gradlew :component-tests:test

# Run a single test
./gradlew :component-tests:test --tests "poc.component.KafkaConnectOutboxTest"
```

### Test Matrix

| Test class | Tests | Verifies |
|---|---|---|
| `KafkaConnectDataStreamTest` | Snapshot capture, enrichment | Variant name, topic prefix, timestamp |
| `KafkaConnectTableApiTest` | Snapshot capture, enrichment | Variant name, topic prefix, timestamp |
| `KafkaConnectSqlApiTest` | Snapshot capture, enrichment | Variant name, topic prefix, timestamp |
| **`KafkaConnectOutboxTest`** | **Routing by destination** | **Route destination, route topic, routing timestamp** |
| `KafkaConnectYamlPipelineTest` | Snapshot capture, enrichment | Variant name, topic prefix, timestamp |

**Example: KafkaConnectOutboxTest**
- Inserts events to `outbox_events` with different `destination` fields
- Verifies events are routed to `poc.kc.outbox.{destination}` topics
- Checks enrichment metadata: `_route_destination`, `_route_topic`, `_routed_at`

### Availability

- If Kafka Connect REST API is available: tests run (green)
- If Kafka Connect is not available: tests skip gracefully (yellow)

## Building and Deploying

### Automated Setup (Recommended)

```bash
# One command builds, deploys, and tests everything:
./gradlew all
```

This orchestrates:
1. Builds all Flink modules
2. Builds Kafka Connect SMTs (compiled for Java 11 — cp-kafka-connect:7.6.1 runtime)
3. Restarts Podman services (MySQL, Kafka, Kafka Connect)
4. Deploys all 5 connectors via REST API
5. Runs all component tests (Flink + Kafka Connect)

### Manual Setup

#### 1. Build Custom SMTs

```bash
./gradlew :kafka-connect-smts:shadowJar
```

This creates `local-development-podman/kafka-connect-smts/build/libs/kafka-connect-smts-1.0.0-with-deps.jar` — a shadow JAR with `org.json` bundled, compiled targeting Java 11.

#### 2. Start Infrastructure

```bash
cd local-development-podman
podman-compose -f podman-compose.yml up -d
```

The `kafka-connect` service:
1. Uses the pre-built `local-development-podman_kafka-connect` image (Debezium + SMT JARs)
2. Starts Kafka Connect REST API on `http://localhost:8083`
3. Waits for Kafka to be healthy

To rebuild the image after changing the SMT code:
```bash
cd /path/to/flink-cdc-poc
./gradlew :kafka-connect-smts:shadowJar
podman build -t local-development-podman_kafka-connect:latest -f local-development-podman/kafka-connect/Dockerfile .
```

#### 3. Deploy Connectors

```bash
cd local-development-podman/kafka-connect
DB_HOST=mysql ./deploy-connectors.sh
```

The `DB_HOST=mysql` override is required for Podman bridge networking (containers reach MySQL by service name, not `localhost`). The script:
- Waits for Kafka Connect REST API to be ready
- Substitutes `database.hostname` in each JSON config with `DB_HOST`
- POSTs each connector config from `connectors/*.json`
- Reports success/failure for each

#### 4. Verify

```bash
curl http://localhost:8083/connectors
curl http://localhost:8083/connectors/kc-datastream-cdc/status | python3 -m json.tool
```

View topics created by connectors:
```bash
podman exec kafka kafka-topics --bootstrap-server localhost:9092 --list | grep poc
```

## Custom SMTs

### EnrichmentTransform

Adds variant metadata to each CDC event.

**Config parameters:**
```json
{
  "transforms": "enrichment",
  "transforms.enrichment.type": "poc.kafka.connect.EnrichmentTransform",
  "transforms.enrichment.variant.name": "datastream-cdc",
  "transforms.enrichment.topic.prefix": "poc.kc.datastream"
}
```

**What it does:**
- Receives a Kafka Connect `Struct` from Debezium (JSON serialization happens after transforms)
- Renames the Kafka topic from `{prefix}.{db}.{table}` (Debezium default) to `{prefix}.{table}` by reading `source.table` from the CDC envelope
- Adds `variant`, `topic`, and `transformed_at` fields to the event envelope

**Example output (after `JsonConverter` serializes the `Struct`):**
```json
{
  "before": null,
  "after": {"id": 1, "customer_id": 42, "amount": "99.99", "status": "PENDING"},
  "source": {"db": "poc_db", "table": "orders", ...},
  "variant": "datastream-cdc",
  "topic": "poc.kc.datastream.orders",
  "transformed_at": 1718003400000
}
```

### OutboxRoutingTransform

Routes outbox events based on the `destination` field in the payload.

**Config parameters:**
```json
{
  "transforms": "routing",
  "transforms.routing.type": "poc.kafka.connect.OutboxRoutingTransform",
  "transforms.routing.topic.prefix": "poc.kc.outbox",
  "transforms.routing.destination.field": "destination"
}
```

**What it does:**
- Extracts the `destination` field from the event payload
- Routes each event to `{topic.prefix}.{destination}`
- Adds routing metadata (`_route_destination`, `_route_topic`, `_routed_at`)

**Example:**
```json
{
  "after": { "id": 123, "destination": "orders-svc" },
  "_route_destination": "orders-svc",
  "_route_topic": "poc.kc.outbox.orders-svc",
  "_routed_at": 1718003400000
}
```

## Java Version Note

The custom SMTs are compiled with `targetCompatibility = VERSION_11` because `cp-kafka-connect:7.6.1` ships JDK 11. Compiling for Java 17 produces class files that fail to load (`UnsupportedClassVersionError: 61.0 > 55.0`). Flink job modules use Java 17 — only the SMT subproject uses Java 11.

## Server-ID Ranges

Kafka Connect uses fixed server-IDs from the dedicated `5500–5599` range so the always-running connectors never collide with the Flink variants (which own `5600–6099`):

| Connector | Server-ID |
|-----------|-----------|
| kc-datastream-cdc | 5510 |
| kc-table-api-cdc | 5520 |
| kc-sql-api-cdc | 5530 |
| kc-yaml-pipeline-cdc | 5540 |
| kc-outbox-cdc | 5550 |

## Comparison with Flink Variants

| Aspect | Kafka Connect | Flink |
|--------|---------------|-------|
| **Setup** | Simpler, REST API | More code, compilation required |
| **Transformations** | SMTs (pluggable, limited) | Custom ProcessFunction (unlimited) |
| **State Management** | None (stateless transforms) | Full state API, windowing, timers |
| **Exactly-once** | Offset management | Checkpoint state |
| **Scaling** | Workers/tasks | Parallelism, task slots |
| **Operational** | REST API, status dashboard | Flink Dashboard, savepoints |
| **Per-row routing** | Possible (with SMT) | Native (side outputs) |
| **Joins** | Not in SMT layer | Full Table API support |

## Configuration Reference

### Debezium MySQL Connector Options

All connectors share these settings (with `database.hostname` substituted at deploy time):

```json
{
  "database.hostname": "mysql",
  "database.port": 3306,
  "database.user": "flink",
  "database.password": "flink",
  "snapshot.mode": "initial",
  "decimal.handling.mode": "string",
  "include.schema.changes": false,
  "schema.history.internal.kafka.bootstrap.servers": "kafka:29092",
  "schema.history.internal.kafka.topic": "dbhistory.<variant>"
}
```

The connector JSON files store `"database.hostname": "localhost"` as the canonical default. `deploy-connectors.sh` substitutes it with `$DB_HOST` before POSTing, so the same JSON works for any network topology.

The `schema.history.internal.*` fields are required by Debezium 2.x to persist the MySQL DDL schema history in a dedicated Kafka topic (one per connector). The `database.user` needs `RELOAD` and `LOCK TABLES` privileges for the initial snapshot (`FLUSH TABLES WITH READ LOCK`).

### JSON Converter

All connectors use JSON with schema disabled:

```json
{
  "key.converter": "org.apache.kafka.connect.json.JsonConverter",
  "key.converter.schemas.enable": false,
  "value.converter": "org.apache.kafka.connect.json.JsonConverter",
  "value.converter.schemas.enable": false
}
```

## Troubleshooting

### Connector stuck in RUNNING but not producing events

```bash
# Check connector logs
podman logs kafka-connect | tail -50

# Check connector status
curl http://localhost:8083/connectors/kc-datastream-cdc/status | python3 -m json.tool

# Verify database connectivity from inside the container
podman exec kafka-connect timeout 3 bash -c "</dev/tcp/mysql/3306" && echo TCP-OK
```

### Topics not created

Kafka has `auto.create.topics.enable: true` in `podman-compose.yml`. If topics aren't created:
1. Check connector is running: `curl http://localhost:8083/connectors/kc-datastream-cdc/status`
2. Verify database has data: `podman exec mysql mysql -uflink -pflink -e "SELECT * FROM poc_db.orders LIMIT 1"`
3. Check Kafka broker logs: `podman logs kafka | grep -i error`

### Custom SMT not loading

```bash
# Verify JAR is in container (only with-deps JAR should be present)
podman exec kafka-connect ls -la /usr/share/java/custom-smts/

# Check Connect worker logs for class loading errors
podman logs kafka-connect | grep -iE 'EnrichmentTransform|NoClass|ClassNot|UnsupportedClass'

# Rebuild image with correct Java 11 SMT JAR:
./gradlew :kafka-connect-smts:shadowJar
podman build -t local-development-podman_kafka-connect:latest -f local-development-podman/kafka-connect/Dockerfile .
```

### Server ID collision

If running both Flink and Kafka Connect simultaneously, ensure they use different server-ID ranges. Check active replica connections:
```bash
podman exec mysql mysql -uflink -pflink -e "SHOW SLAVE HOSTS"
```

## Monitoring

### Kafka Connect API

```bash
# List connectors and status
curl http://localhost:8083/connectors
curl http://localhost:8083/connectors/kc-datastream-cdc/status
```

### Event Flow

```bash
# Watch events in real-time
podman exec kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic poc.kc.datastream.orders \
  --from-beginning \
  --property print.key=true \
  --property print.timestamp=true
```

## See Also

- [CLAUDE.md](./CLAUDE.md) — Flink variants and server-ID ranges
- [FLINK_CHECKPOINT_CONFIG.md](./FLINK_CHECKPOINT_CONFIG.md) — Checkpoint semantics (also applies to Kafka Connect offsets)
- [Debezium MySQL Connector docs](https://debezium.io/documentation/reference/stable/connectors/mysql.html)
- [Kafka Connect SMT docs](https://kafka.apache.org/documentation/#connect_transforms)
