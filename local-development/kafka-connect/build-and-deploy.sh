#!/bin/bash
set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

echo "=== Building and Deploying Kafka Connect Variants ==="
echo ""

# 1. Build SMTs with Gradle
echo "Step 1: Building custom SMTs..."
cd "$PROJECT_ROOT"

./gradlew :kafka-connect-smts:shadowJar -q
if [ $? -eq 0 ]; then
    echo "✓ SMTs built successfully"
else
    echo "✗ SMT build failed"
    exit 1
fi

# 2. Build Kafka Connect image
echo ""
echo "Step 2: Building Kafka Connect Docker image..."
cd "$SCRIPT_DIR"

docker build -t poc/kafka-connect:latest . --build-arg BUILDKIT_INLINE_CACHE=1
if [ $? -eq 0 ]; then
    echo "✓ Kafka Connect image built successfully"
else
    echo "✗ Docker build failed"
    exit 1
fi

# 3. Start services
echo ""
echo "Step 3: Starting Docker Compose services..."
cd "$PROJECT_ROOT/docker"

docker compose up -d mysql kafka
if [ $? -eq 0 ]; then
    echo "✓ MySQL and Kafka started"
else
    echo "✗ Docker Compose up failed"
    exit 1
fi

# Wait for services
echo ""
echo "Waiting for services to be healthy..."
sleep 5

# 4. Start Kafka Connect
echo ""
echo "Step 4: Starting Kafka Connect..."
docker compose up -d kafka-connect

# Wait for Kafka Connect REST API
echo "Waiting for Kafka Connect REST API..."
for i in {1..30}; do
    if curl -sf http://localhost:8083 > /dev/null 2>&1; then
        echo "✓ Kafka Connect is ready"
        break
    fi
    if [ $i -eq 30 ]; then
        echo "✗ Kafka Connect did not become ready"
        exit 1
    fi
    sleep 1
done

# 5. Deploy connectors
echo ""
echo "Step 5: Deploying connectors..."
"$SCRIPT_DIR/deploy-connectors.sh"

echo ""
echo "=== Deployment Complete ==="
echo ""
echo "📊 Kafka Connect UI:    http://localhost:8083"
echo "📊 Kafka UI:            http://localhost:8080"
echo "🗄️  MySQL:               localhost:3306 (user: flink, password: flink)"
echo ""
echo "View connector status:"
echo "  curl http://localhost:8083/connectors"
echo ""
echo "View topics:"
echo "  docker exec kafka kafka-topics --bootstrap-server localhost:9092 --list | grep poc"
echo ""
echo "Monitor events:"
echo "  docker exec kafka kafka-console-consumer --bootstrap-server localhost:9092 --topic poc.cdc.datastream.orders --from-beginning"
