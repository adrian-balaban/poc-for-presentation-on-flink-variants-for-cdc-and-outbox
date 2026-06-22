#!/bin/bash
set -e

CONNECT_URL="http://localhost:8083"
CONNECTORS_DIR="$(dirname "$0")/connectors"
# DB_HOST: hostname of MySQL as seen from inside the Kafka Connect container.
# Defaults to "mysql" for Podman bridge networking (service name in podman-compose.yml).
DB_HOST="${DB_HOST:-mysql}"
MAX_RETRIES=90
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

    local payload
    payload=$(sed "s/\"database.hostname\": \"localhost\"/\"database.hostname\": \"${DB_HOST}\"/" "$connector_file")

    local response
    local http_code
    response=$(curl -s -o /tmp/connect_response_$$.json -w "%{http_code}" -X POST "$CONNECT_URL/connectors" \
        -H "Content-Type: application/json" \
        -d "$payload")
    http_code="$response"
    if [ "$http_code" = "201" ]; then
        echo "✓ Deployed: $connector_name"
    else
        echo "✗ Failed to deploy: $connector_name (HTTP $http_code)"
        cat /tmp/connect_response_$$.json 2>/dev/null && echo
        rm -f /tmp/connect_response_$$.json
        return 1
    fi
    rm -f /tmp/connect_response_$$.json
}

main() {
    echo "=== Kafka Connect Connector Deployment ==="

    wait_for_connect || exit 1

    if [ ! -d "$CONNECTORS_DIR" ]; then
        echo "✗ Connectors directory not found: $CONNECTORS_DIR"
        exit 1
    fi

    # Deploy connectors in parallel to save time
    local pids=()
    for connector_file in "$CONNECTORS_DIR"/*.json; do
        if [ -f "$connector_file" ]; then
            deploy_connector "$connector_file" &
            pids+=($!)
        fi
    done

    # Wait for all deployments to complete
    local failed=0
    for pid in "${pids[@]}"; do
        if ! wait "$pid"; then
            ((failed++))
        fi
    done

    if [ $failed -gt 0 ]; then
        echo "✗ $failed connector(s) failed to deploy"
        exit 1
    fi

    echo "=== Deployment Complete ==="
    echo "Connectors status: $CONNECT_URL/connectors"
}

main "$@"
