# Kafka Connect Quick Start

Get Kafka Connect variants running alongside the Flink CDC variants.

## Prerequisites

- Docker and Docker Compose
- Maven 3.6+ (for building custom SMTs)
- curl (for testing)

## Quick Start

### Option 1: Automated Setup (Recommended)

```bash
cd docker/kafka-connect
./build-and-deploy.sh
```

This script:
1. ✅ Builds custom SMTs with Maven
2. ✅ Builds Kafka Connect Docker image with plugins
3. ✅ Starts MySQL, Kafka, and Kafka Connect
4. ✅ Deploys all 5 connectors
5. ✅ Displays connection info

### Option 2: Manual Setup

**Step 1: Build SMTs**
```bash
cd docker/kafka-connect-smts
mvn clean package
cd ../..
```

**Step 2: Start services**
```bash
cd docker
docker compose up -d mysql kafka kafka-connect
```

**Step 3: Wait for Kafka Connect**
```bash
# Check when REST API is ready
curl http://localhost:8083
```

**Step 4: Deploy connectors**
```bash
cd kafka-connect
./deploy-connectors.sh
```

## Verify Deployment

### Check connectors
```bash
curl http://localhost:8083/connectors | jq
```

### View connector status
```bash
curl http://localhost:8083/connectors/kc-datastream-cdc/status | jq
```

### List topics created
```bash
docker exec kafka kafka-topics --bootstrap-server localhost:9092 --list | grep poc
```

### Watch events in real-time
```bash
docker exec kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic poc.cdc.datastream.orders \
  --from-beginning \
  --max-messages 5
```

## Connectors Overview

| Name | Variant | Topic Prefix | SMT |
|------|---------|--------------|-----|
| `kc-datastream-cdc` | DataStream API | `poc.cdc.datastream` | `EnrichmentTransform` |
| `kc-table-api-cdc` | Table API | `poc.cdc.tableapi` | `EnrichmentTransform` |
| `kc-sql-api-cdc` | SQL API | `poc.cdc.sqlapi` | `EnrichmentTransform` |
| `kc-outbox-cdc` | Outbox Pattern | `poc.cdc.outbox.*` | `OutboxRoutingTransform` |
| `kc-yaml-pipeline-cdc` | YAML Pipeline | `poc.cdc.yaml` | `EnrichmentTransform` |

## Side-by-Side Comparison

Run Flink and Kafka Connect variants together:

```bash
# Terminal 1: Start all services (includes Flink)
cd docker && ./gradlew all

# Terminal 2: Deploy Kafka Connect
cd docker/kafka-connect && ./build-and-deploy.sh

# Terminal 3: Compare outputs
watch -n 1 'curl -s http://localhost:8083/connectors | jq length'
```

## Troubleshooting

### Kafka Connect won't start
```bash
docker logs kafka-connect
# Check for classpath/plugin issues, Java version mismatch
```

### Connectors stuck in "RUNNING" but no data
```bash
# 1. Check connector logs
docker logs kafka-connect | grep -i "error\|exception"

# 2. Verify MySQL is working
docker exec mysql mysql -uflink -pflink -e "SELECT COUNT(*) FROM poc_db.orders"

# 3. Check if Kafka topic exists
docker exec kafka kafka-topics --bootstrap-server localhost:9092 --describe --topic poc.cdc.datastream.orders
```

### Custom SMT not found
```bash
# Verify JAR location in image
docker exec kafka-connect ls -la /usr/share/java/custom-smts/

# Rebuild Docker image
cd docker && docker compose build --no-cache kafka-connect
```

## Next Steps

- Read [KAFKA_CONNECT.md](../KAFKA_CONNECT.md) for detailed docs
- Compare throughput/latency with Flink variants
- Customize SMTs for additional transformations
- Set up monitoring with Prometheus/Grafana
