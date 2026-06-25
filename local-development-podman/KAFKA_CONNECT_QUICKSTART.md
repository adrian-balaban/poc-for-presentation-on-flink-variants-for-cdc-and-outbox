# Kafka Connect Quick Start

Get Kafka Connect variants running alongside the Flink CDC variants.

## Prerequisites

- Podman + podman-compose
- curl

## Quick Start

### Option 1: Automated Setup (Recommended)

```bash
./gradlew all
```

This builds everything, restarts the Podman stack, deploys all 5 connectors, and runs component tests.

### Option 2: Manual Setup

**Step 1: Build SMTs (targets Java 11)**
```bash
./gradlew :kafka-connect-smts:shadowJar
```

**Step 2: Start services**
```bash
cd local-development-podman
podman-compose -f podman-compose.yml up -d mysql kafka kafka-connect
```

**Step 3: Deploy connectors**
```bash
cd local-development-podman/kafka-connect
DB_HOST=mysql ./deploy-connectors.sh
```

`DB_HOST=mysql` is required for Podman bridge networking — the Kafka Connect container reaches MySQL by service name.

## Verify Deployment

### Check connectors
```bash
curl http://localhost:8083/connectors
```

### View connector status
```bash
curl http://localhost:8083/connectors/kc-datastream-cdc/status | python3 -m json.tool
```

### List topics created
```bash
podman exec kafka kafka-topics --bootstrap-server localhost:9092 --list | grep poc
```

### Watch events in real-time
```bash
podman exec kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic poc.kc.datastream.orders \
  --from-beginning \
  --max-messages 5
```

## Connectors Overview

| Name | Variant | Topic Prefix | SMT |
|------|---------|--------------|-----|
| `kc-datastream-cdc` | DataStream API | `poc.kc.datastream` | `EnrichmentTransform` |
| `kc-table-api-cdc` | Table API | `poc.kc.table-api` | `EnrichmentTransform` |
| `kc-sql-api-cdc` | SQL API | `poc.kc.sql-api` | `EnrichmentTransform` |
| `kc-outbox-cdc` | Outbox Pattern | `poc.kc.outbox.*` | `OutboxRoutingTransform` |
| `kc-yaml-pipeline-cdc` | YAML Pipeline | `poc.kc.yaml-pipeline` | `EnrichmentTransform` |

## Rebuilding the Kafka Connect Image

After changing SMT code:

```bash
# 1. Rebuild the shadow JAR (Java 11 target)
./gradlew :kafka-connect-smts:shadowJar

# 2. Rebuild the container image (build context = project root)
podman build -t local-development-podman_kafka-connect:latest -f local-development-podman/kafka-connect/Dockerfile .

# 3. Restart kafka-connect
podman stop kafka-connect && podman rm kafka-connect
cd local-development-podman
podman-compose -f podman-compose.yml up -d kafka-connect

# 4. Redeploy connectors
cd kafka-connect && DB_HOST=mysql ./deploy-connectors.sh
```

## Troubleshooting

### Kafka Connect won't start
```bash
podman logs kafka-connect
# Check for Java version mismatch (UnsupportedClassVersionError: 61.0 > 55.0)
# → rebuild SMTs targeting Java 11: ./gradlew :kafka-connect-smts:shadowJar
```

### Connectors stuck in "RUNNING" but no data
```bash
# 1. Check connector logs
podman logs kafka-connect | grep -i "error\|exception"

# 2. Verify MySQL is working
podman exec mysql mysql -uflink -pflink -e "SELECT COUNT(*) FROM poc_db.orders"

# 3. Check if Kafka topic exists
podman exec kafka kafka-topics --bootstrap-server localhost:9092 --describe --topic poc.kc.datastream.orders
```

### Custom SMT not found
```bash
# Verify only the with-deps JAR is in the container (plain JAR won't load in Java 11)
podman exec kafka-connect ls -la /usr/share/java/custom-smts/

# Rebuild image
cd /path/to/flink-cdc-poc
./gradlew :kafka-connect-smts:shadowJar
podman build -t local-development-podman_kafka-connect:latest -f local-development-podman/kafka-connect/Dockerfile .
```

## Next Steps

- Read [KAFKA_CONNECT.md](../KAFKA_CONNECT.md) for detailed docs
- Compare throughput/latency with Flink variants
- Customize SMTs for additional transformations
