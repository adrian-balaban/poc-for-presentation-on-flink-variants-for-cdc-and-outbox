#!/usr/bin/env bash
# Open (or close) host port-forwards to the flink-cdc-poc k8s stack running in
# the local kind cluster. The k8s stack binds no host ports itself — host access
# to every service (Flink JMs, Kafka Connect, MinIO, Grafana, Prometheus, MySQL,
# Kafka) is via kubectl port-forward to non-conflicting high ports.
#
# Port-forwards run in the background; PIDs are recorded in .port-forward-pids
# next to this script so `stop` can tear them down. Idempotent: `start` is safe
# to re-run (already-listening forwards are skipped), and `stop` is safe to run
# when nothing is forwarded.
#
# Coexists with the running Podman Compose stack — these high ports (13xxx /
# 18xxx / 19xxx / 13306 / 19092 / 9001) do not collide with the Podman ports
# (3001 / 8080 / 8081 / 8083 / 9090 / 9092 / 3306 / 9001). The one shared port
# is MinIO console 9001: if the Podman stack is also up, comment out the MinIO
# forward below (or stop Podman MinIO) before running this.
#
# Usage:
#   ./local-development-k8s/port-forward.sh start   # open all forwards
#   ./local-development-k8s/port-forward.sh stop    # close all forwards
#   ./local-development-k8s/port-forward.sh status  # show what is listening
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PID_FILE="$SCRIPT_DIR/.port-forward-pids"
KUBE_CONTEXT="${KUBE_CONTEXT:-kind-flink-cdc-poc}"

# Each forward: "namespace|target|localPort:remotePort|label"
#   target  — a kubectl port-forward resource spec, e.g. svc/mysql or deploy/foo
#   label   — free text shown in the status line (may contain spaces)
FORWARDS=(
  "poc|svc/mysql|13306:3306|MySQL"
  "poc|svc/poc-kafka-kafka-external-bootstrap|19092:9094|Kafka (external nodeport listener; advertisedHost=localhost)"
  "poc|svc/datastream-cdc-rest|18081:8081|Flink UI (DataStream)"
  "poc|svc/table-api-cdc-rest|18082:8081|Flink UI (Table API)"
  "poc|svc/sql-api-cdc-rest|18083:8081|Flink UI (SQL API)"
  "poc|svc/outbox-cdc-rest|18084:8081|Flink UI (Outbox)"
  "poc|svc/yaml-pipeline-cdc-rest|18085:8081|Flink UI (YAML Pipeline)"
  "poc|svc/poc-connect-connect-api|18086:8083|Kafka Connect REST"
  "poc|svc/minio|9001:9001|MinIO console"
  "monitoring|svc/kube-prometheus-stack-grafana|13001:80|Grafana (admin/admin)"
  "monitoring|svc/kube-prometheus-stack-prometheus|19090:9090|Prometheus"
)

port_in_use() { ss -ltn 2>/dev/null | awk '{print $4}' | grep -qE "(^|:)$1\$"; }

start() {
  command -v kubectl >/dev/null || { echo "✗ kubectl not installed" >&2; exit 1; }
  kubectl config use-context "$KUBE_CONTEXT" >/dev/null 2>&1 || true
  : > "$PID_FILE"
  echo "▶ opening k8s port-forwards (context: $KUBE_CONTEXT)..."
  for entry in "${FORWARDS[@]}"; do
    IFS='|' read -r ns target ports label <<< "$entry"
    local_port="${ports%%:*}"
    if port_in_use "$local_port"; then
      printf "  ↷ :%s already listening — skip %s\n" "$local_port" "$target"
      continue
    fi
    kubectl -n "$ns" port-forward "$target" "$ports" >/dev/null 2>&1 &
    pid=$!
    # give it a moment to bind; confirm it is listening
    for _ in 1 2 3 4 5 6 7 8 9 10; do
      port_in_use "$local_port" && break
      sleep 0.3
    done
    if port_in_use "$local_port"; then
      printf "  ✓ %-11s %-38s → localhost:%-6s %s\n" "$ns" "$target" "$local_port" "$label"
      echo "$pid $local_port $ns $target" >> "$PID_FILE"
    else
      printf "  ✗ %-11s %-38s → localhost:%-6s FAILED (pod not ready?)\n" "$ns" "$target" "$local_port"
      kill "$pid" 2>/dev/null || true
    fi
  done
  echo
  echo "Grafana dashboard : http://localhost:13001/d/flink-cdc-poc-monitoring"
  echo "Prometheus alerts : http://localhost:19090/alerts"
  echo
  echo "Stop with: ./local-development-k8s/port-forward.sh stop"
}

stop() {
  [ -f "$PID_FILE" ] || { echo "✓ nothing forwarded (no pid file)"; exit 0; }
  echo "▶ closing k8s port-forwards..."
  while IFS=' ' read -r pid local_port ns target; do
    [ -n "$pid" ] || continue
    if kill "$pid" 2>/dev/null; then
      printf "  ✓ stopped :%s (%s:%s)\n" "$local_port" "$ns" "$target"
    else
      printf "  ↷ :%s already gone\n" "$local_port"
    fi
  done < "$PID_FILE"
  rm -f "$PID_FILE"
  echo "✓ done"
}

status() {
  echo "k8s port-forwards listening (context: $KUBE_CONTEXT):"
  for entry in "${FORWARDS[@]}"; do
    parts=( $entry ); local_port="${parts[1]%%:*}"
    target="${parts[0]#*:}"; label="$parts[2] $parts[3] $parts[4]"
    if port_in_use "$local_port"; then
      printf "  ✓ :%s up   %s  %s\n" "$local_port" "$target" "$label"
    else
      printf "  ✗ :%s down %s\n" "$local_port" "$target"
    fi
  done
}

case "${1:-start}" in
  start)  start ;;
  stop)   stop ;;
  status) status ;;
  *) echo "Usage: $0 {start|stop|status}" >&2; exit 2 ;;
esac