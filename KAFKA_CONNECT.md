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
Kafka Topics (poc.cdc.*)
```

## Component Tests

Five component tests verify each Kafka Connect variant — testing the full pipeline from MySQL CDC to Kafka with SMT transformations applied.

**Run Kafka Connect component tests:**
```bash
# Prerequisite: start Kafka Connect with ./build-and-deploy.sh (see below)

# Run only Kafka Connect tests
./gradlew :component-tests:test -k "KafkaConnect"

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
- Verifies events are routed to `poc.cdc.outbox.{destination}` topics
- Checks enrichment metadata: `_route_destination`, `_route_topic`, `_routed_at`

### Docker Availability

- If Kafka Connect REST API is available: tests run (✅ green)
- If Kafka Connect is not available: tests skip gracefully (⭕ yellow)

## Building and Deploying

### Automated Setup (Recommended)

```bash
# One command builds, deploys, and tests everything:
./gradlew all
```

This orchestrates:
1. ✅ Builds all Flink modules
2. ✅ Builds Kafka Connect SMTs with Maven
3. ✅ Restarts Docker services (MySQL, Kafka, Kafka Connect)
4. ✅ Deploys all 5 connectors
5. ✅ Runs all component tests (Flink + Kafka Connect)

### Manual Setup

#### 1. Build Custom SMTs

```bash
cd docker/kafka-connect-smts
mvn clean package
```

This creates:
- `target/kafka-connect-smts-1.0.0.jar` — compiled SMTs
- `target/kafka-connect-smts-1.0.0-with-deps.jar` — shaded JAR with dependencies

#### 2. Start Docker Infrastructure

```bash
cd docker
docker compose up -d
```

The `kafka-connect` service will:
1. Build the custom Dockerfile (installs Debezium, copies SMT JARs)
2. Start Kafka Connect REST API on `http://localhost:8083`
3. Wait for Kafka to be healthy

#### 3. Deploy Connectors

Once Kafka Connect is healthy, deploy the 5 connectors:

```bash
cd docker/kafka-connect
./deploy-connectors.sh
```

This script:
- Waits for Kafka Connect REST API to be ready
- POSTs each connector config from `connectors/*.json`
- Reports success/failure for each

#### 4. Verify

Check connector status:
```bash
curl http://localhost:8083/connectors
curl http://localhost:8083/connectors/kc-datastream-cdc/status
```

View topics created by connectors:
```bash
docker exec kafka kafka-topics --bootstrap-server localhost:9092 --list | grep poc
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
  "transforms.enrichment.topic.prefix": "poc.cdc.datastream"
}
```

**What it does:**
- Parses CDC JSON payload
- Adds `variant` field with variant name
- Adds `topic` field with full topic path
- Adds `transformed_at` timestamp
- Handles both JSON strings and Map objects

**Example output:**
```json
{
  "before": {...},
  "after": {...},
  "source": {...},
  "variant": "datastream-cdc",
  "topic": "poc.cdc.datastream.orders",
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
  "transforms.routing.topic.prefix": "poc.cdc.outbox",
  "transforms.routing.destination.field": "destination"
}
```

**What it does:**
- Extracts the `destination` field from the event payload
- Routes each event to `{topic.prefix}.{destination}`
- Adds routing metadata (`_route_destination`, `_route_topic`, `_routed_at`)
- Handles nested CDC payloads (checks `after` field)

**Example:**
```json
{
  "before": {...},
  "after": {
    "id": 123,
    "destination": "orders-svc",
    ...
  },
  "_route_destination": "orders-svc",
  "_route_topic": "poc.cdc.outbox.orders-svc",
  "_routed_at": 1718003400000
}
```

Routes to Kafka topic: `poc.cdc.outbox.orders-svc`

## Server-ID Ranges

Kafka Connect uses the same server-ID ranges as Flink (avoid collisions with running Flink jobs):

| Connector | Server-ID | Range |
|-----------|-----------|-------|
| kc-outbox-cdc | 5600 | 5600–5699 |
| kc-yaml-pipeline-cdc | 5700 | 5700–5709 |
| kc-sql-api-cdc | 5800 | 5800–5899 |
| kc-datastream-cdc | 5900 | 5900–5999 |
| kc-table-api-cdc | 6000 | 6000–6099 |

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

All connectors share these settings:

```json
{
  "database.hostname": "localhost",
  "database.port": 3306,
  "database.user": "flink",
  "database.password": "flink",
  "snapshot.mode": "initial",
  "decimal.handling.mode": "string",
  "include.schema.changes": false
}
```

**Key options:**
- `snapshot.mode`: `initial` (full snapshot + binlog), `schema_only`, `no_data`
- `decimal.handling.mode`: `string` or `precise` (affects number precision)
- `include.schema.changes`: include DDL changes in output
- `table.include.list`: comma-separated list of tables (use `poc_db.orders,poc_db.customers` format)

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

This matches the Flink variants' JSON deserialization.

## Troubleshooting

### Connector stuck in RUNNING but not producing events

```bash
# Check connector logs
docker logs kafka-connect | tail -50

# Check connector status
curl http://localhost:8083/connectors/kc-datastream-cdc/status | jq

# Verify database connectivity
docker exec kafka-connect curl -v telnet://localhost:3306
```

### Topics not created

Kafka has `auto.create.topics.enable: true` in docker-compose.yml. If topics aren't created:
1. Check if connector is actually running: `curl http://localhost:8083/connectors/kc-datastream-cdc/status`
2. Verify database has data: `docker exec mysql mysql -uflink -pflink -e "SELECT * FROM poc_db.orders LIMIT 1"`
3. Check Kafka broker logs: `docker logs kafka | grep -i error`

### Custom SMT not loading

```bash
# Verify JAR is in container
docker exec kafka-connect ls -la /usr/share/java/custom-smts/

# Check Connect worker logs
docker logs kafka-connect | grep -i "smt\|transform\|class"

# Verify SMT class name in connector config matches pom.xml package
# Should be: poc.kafka.connect.EnrichmentTransform
```

### Server ID collision

If running both Flink and Kafka Connect:
- Flink variant reads as server-id 5900
- Kafka Connect must use different ID (e.g., 6000+)
- If both use 5900, MySQL will reject the second connection
- Check: `docker exec mysql mysql -uflink -pflink -e "SHOW SLAVE HOSTS"`

## Monitoring

### Kafka Connect Dashboard

```bash
curl http://localhost:8083
```

Returns list of deployed connectors and their tasks.

### Topic Lag Monitoring

Monitor consumer lag with Kafka UI:
```bash
# Already running on http://localhost:8080
# Check: Clusters > poc > Topics > poc.cdc.datastream.*
```

### Event Flow

```bash
# Watch events in real-time
docker exec kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic poc.cdc.datastream.orders \
  --from-beginning \
  --property print.key=true \
  --property print.timestamp=true
```

## Next Steps

1. **Benchmarking**: Compare Kafka Connect vs Flink throughput, latency, CPU
2. **Advanced SMTs**: Create SMTs for schema evolution, filtering, aggregation
3. **Production deployment**: Use Confluent Platform, Strimzi operator, or custom Kubernetes deployment
4. **Monitoring**: Integrate with Prometheus, Grafana for metrics collection
5. **Error handling**: Implement DLQ (Dead Letter Queue) for failed transformations

## See Also

- [CLAUDE.md](./CLAUDE.md) — Flink variants and server-ID ranges
- [CHECKPOINT_CONFIG.md](./CHECKPOINT_CONFIG.md) — Checkpoint semantics (also applies to Kafka Connect offsets)
- [Debezium MySQL Connector docs](https://debezium.io/documentation/reference/stable/connectors/mysql.html)
- [Kafka Connect SMT docs](https://kafka.apache.org/documentation/#connect_transforms)
