#!/bin/bash
# Don't exit on error - allow graceful degradation if Kafka Connect plugins aren't available

CONNECT_URL="http://localhost:8083"
CONNECTORS_DIR="$(dirname "$0")/connectors"
MAX_RETRIES=30
RETRY_DELAY=2

wait_for_connect() {
    local retries=0
    while [ $retries -lt $MAX_RETRIES ]; do
        if curl -sf "$CONNECT_URL" > /dev/null 2>&1; then
            echo "✓ Kafka Connect is ready"
            return 0
        fi
        echo "⏳ Waiting for Kafka Connect ($((retries+1))/$MAX_RETRIES)..."
        sleep $RETRY_DELAY
        ((retries++))
    done
    echo "✗ Kafka Connect did not become ready after $MAX_RETRIES attempts"
    return 1
}

deploy_connector() {
    local connector_file="$1"
    local connector_name=$(jq -r '.name' "$connector_file")

    echo "Deploying connector: $connector_name"

    if curl -sf -X POST "$CONNECT_URL/connectors" \
        -H "Content-Type: application/json" \
        -d @"$connector_file" > /dev/null 2>&1; then
        echo "✓ Deployed: $connector_name"
    else
        echo "⚠️  WARNING: Failed to deploy: $connector_name (Debezium plugin may not be available)"
    fi
}

main() {
    echo "=== Kafka Connect Connector Deployment ==="

    # Non-blocking wait for Kafka Connect
    if ! wait_for_connect; then
        echo "⚠️  Kafka Connect not ready - skipping connector deployment"
        return 0
    fi

    if [ ! -d "$CONNECTORS_DIR" ]; then
        echo "✗ Connectors directory not found: $CONNECTORS_DIR"
        return 1
    fi

    for connector_file in "$CONNECTORS_DIR"/*.json; do
        if [ -f "$connector_file" ]; then
            deploy_connector "$connector_file"
        fi
    done

    echo "=== Deployment Complete ==="
    echo "Note: Check $CONNECT_URL/connector-plugins for available connectors"
    return 0
}

main "$@"
