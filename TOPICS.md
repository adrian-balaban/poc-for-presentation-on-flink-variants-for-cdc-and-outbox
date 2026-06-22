# Kafka Topics

## Topic naming convention

```
poc.cdc.<variant>.<flink|kc>[.<table>]
```

Each variant has distinct topics for the Flink job and the Kafka Connect connector.

## Topic map

| Variant | Flink topic(s) | KC topic(s) |
|---------|---------------|------------|
| **DataStream** | `poc.cdc.datastream.flink` | `poc.cdc.datastream.kc.orders` (prefix `poc.cdc.datastream.kc`) |
| **Table API** | `poc.cdc.table-api.flink` | `poc.cdc.table-api.kc.orders` (prefix `poc.cdc.table-api.kc`) |
| **SQL API** | `poc.cdc.sql-api.flink.orders`, `poc.cdc.sql-api.flink.customers` | prefix `poc.cdc.sql-api.kc` → Debezium appends `.<db>.<table>` |
| **Outbox** | `poc.cdc.outbox.flink` (all events) | `poc.cdc.outbox.kc.orders-svc`, `poc.cdc.outbox.kc.payment-svc` (routed by `destination` field) |
| **YAML Pipeline** | `poc.cdc.yaml.flink.orders` | prefix `poc.cdc.yaml.kc` |

## Migration from previous naming

The table below shows what each topic was called before the rename (2026-06-22) and what it is now.

| Variant | Old Flink | Old KC | New Flink | New KC |
|---------|-----------|--------|-----------|--------|
| DataStream | `poc.cdc.datastream` | `poc.cdc.datastream` ⚠️ same | `poc.cdc.datastream.flink` | `poc.cdc.datastream.kc` |
| Table API | `poc.cdc.table-api` | `poc.cdc.tableapi` | `poc.cdc.table-api.flink` | `poc.cdc.table-api.kc` |
| SQL API | `poc.cdc.sql-api.orders` | `poc.cdc.sqlapi` | `poc.cdc.sql-api.flink.orders` | `poc.cdc.sql-api.kc` |
| Outbox | prefix `poc.cdc.outbox` ⚠️ same prefix as KC | prefix `poc.cdc.outbox` ⚠️ | prefix `poc.cdc.outbox.flink` → `.orders-svc`, `.payment-svc` | prefix `poc.cdc.outbox.kc` → `.orders-svc`, `.payment-svc` |
| YAML Pipeline | `poc.cdc.yaml.orders` | `poc.kc.yaml` (prefix) | `poc.cdc.yaml.flink.orders` | `poc.cdc.yaml.kc` |

**Why the rename was needed:**
- DataStream and Outbox previously shared identical topic names between Flink and KC — actual collision risk when both run simultaneously
- Table API and SQL API had inconsistent naming (no hyphen vs hyphen; no table suffix)
- YAML KC prefix `poc.kc.yaml` deviated from the `poc.cdc.*` namespace

## Notes

- **DataStream Flink**: all CDC events go to a single topic `poc.cdc.datastream.flink`; the `topic` field embedded in each JSON message records the same value
- **SQL API Flink**: two separate topics, one per table; `orders` and `customers` are independent Flink sinks
- **Outbox Flink**: all events go to `poc.cdc.outbox.flink`; the `OutboxRouter` logs where each event *should* route (`poc.cdc.outbox.flink.<destination>`) but the POC sends everything to one topic for simplicity
- **Outbox KC**: `OutboxRoutingTransform` SMT actually routes each event to a separate topic (`poc.cdc.outbox.kc.<destination>`) using the `destination` field in the payload
- **YAML Flink**: the `pipeline.yaml` sink sets `topic: ${KAFKA_TOPIC_PREFIX}.yaml.flink.orders`; the `.orders` suffix is a literal part of the topic name (not appended by Flink CDC in this single-table pipeline)
