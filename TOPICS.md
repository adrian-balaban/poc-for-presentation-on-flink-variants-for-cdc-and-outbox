# Kafka Topics

## Topic naming convention

```
poc.<engine>.<variant>[.<table|destination>]
```

- `<engine>` — `flink` for the Flink jobs, `kc` for the Kafka Connect connectors.
  The two engines use **separate top-level namespaces** (`poc.flink.*` vs `poc.kc.*`)
  so the same variant's Flink job and KC connector never collide when both run
  simultaneously.
- The Flink prefix comes from `KAFKA_TOPIC_PREFIX` (default `poc.flink`, set in
  `JobConfig.fromEnv()` and in each k8s FlinkDeployment env). The KC prefix comes
  from each connector's `topic.prefix` in `local-development-podman/kafka-connect/connectors/*.json`.

## Topic map

| Variant | Flink topic(s) | KC topic(s) |
|---------|---------------|------------|
| **DataStream** | `poc.flink.datastream.orders` | `poc.kc.datastream.orders` (prefix `poc.kc.datastream`) |
| **Table API** | `poc.flink.table-api.orders` | `poc.kc.table-api.orders` (prefix `poc.kc.table-api`) |
| **SQL API** | `poc.flink.sql-api.orders`, `poc.flink.sql-api.customers` | `poc.kc.sql-api.orders` (prefix `poc.kc.sql-api`) |
| **Outbox** | `poc.flink.outbox.outbox-events` (all events) | `poc.kc.outbox.orders-svc`, `poc.kc.outbox.payment-svc` (routed by `destination` field) |
| **YAML Pipeline** | `poc.flink.yaml-pipeline.orders` | `poc.kc.yaml-pipeline.orders` (prefix `poc.kc.yaml-pipeline`) |

## Why two namespaces

All 5 Flink jobs and all 5 Kafka Connect connectors run **at the same time** against
the same MySQL + Kafka. If a Flink job and its KC counterpart wrote to the same
topic, a variant's component test could read messages produced by the *other* engine
(e.g. a Flink message has a `variant` field; a Debezium/SMT message does not), making
the assertion pass or fail for the wrong reason. Splitting at the top level
(`poc.flink.*` vs `poc.kc.*`) keeps every producer's output in its own topic tree.

## Notes

- **DataStream Flink**: all CDC events go to a single topic `poc.flink.datastream.orders`; the `topic` field embedded in each JSON message records the same value.
- **SQL API Flink**: two separate topics, one per table; `orders` and `customers` are independent Flink sinks.
- **Outbox Flink**: all events go to `poc.flink.outbox.outbox-events`; the `OutboxRouter` logs where each event *should* route but the POC sends everything to one topic for simplicity.
- **Outbox KC**: the `OutboxRoutingTransform` SMT actually routes each event to a separate topic (`poc.kc.outbox.<destination>`) using the `destination` field in the payload.
- **KC enrichment variants** (DataStream / Table API / SQL API / YAML): the `EnrichmentTransform` SMT renames the Debezium default topic `<prefix>.<db>.<table>` to `<prefix>.<table>`, so e.g. `poc.kc.datastream.poc_db.orders` becomes `poc.kc.datastream.orders`.
- **YAML Flink**: the `pipeline.yaml` sink sets `topic: ${KAFKA_TOPIC_PREFIX}.yaml-pipeline.orders`; the `.orders` suffix is a literal part of the topic name (not appended by Flink CDC in this single-table pipeline).
