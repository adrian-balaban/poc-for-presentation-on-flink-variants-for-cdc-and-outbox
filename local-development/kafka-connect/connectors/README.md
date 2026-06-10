# Kafka Connect Connector Configurations

These JSON files are deployed by `deploy-connectors.sh` (or `./gradlew all`).

## Connector matrix

| File | Server-ID | Server-Name | Variant Name | Topic Prefix |
|------|-----------|-------------|--------------|--------------|
| kc-datastream-cdc.json | 5900 | mysql | datastream-cdc | poc.cdc.datastream |
| kc-table-api-cdc.json | 6000 | mysql-tableapi | table-api-cdc | poc.cdc.tableapi |
| kc-sql-api-cdc.json | 5800 | mysql-sqlapi | sql-api-cdc | poc.cdc.sqlapi |
| kc-outbox-cdc.json | 5600 | mysql-outbox | outbox-cdc | poc.cdc.outbox |
| kc-yaml-pipeline-cdc.json | 5700 | mysql-yaml | yaml-pipeline-cdc | poc.cdc.yaml |

To add a new variant: copy an existing file, update the fields above, choose a server-ID from the reserved range, and register it in `CLAUDE.md`.

## Common settings

All connectors share these defaults:

| Setting | Value | Notes |
|---------|-------|-------|
| `connector.class` | `io.debezium.connector.mysql.MySqlConnector` | |
| `database.hostname` | `localhost` | Replaced with `$DB_HOST` by `deploy-connectors.sh`; use `mysql` on Podman bridge |
| `database.port` | `3306` | |
| `database.user` | `flink` | Needs `REPLICATION SLAVE`, `REPLICATION CLIENT`, `RELOAD`, `LOCK TABLES` |
| `database.password` | `flink` | |
| `database.include.list` | `poc_db` | |
| `snapshot.mode` | `initial` | |
| `transforms` | `enrichment` | `EnrichmentTransform` (CDC variants) or `OutboxRoutingTransform` (outbox) |
| `key.converter` | `JsonConverter` | `schemas.enable=false` |
| `value.converter` | `JsonConverter` | `schemas.enable=false` |
| `decimal.handling.mode` | `string` | |
| `include.schema.changes` | `false` | |
| `schema.history.internal.kafka.bootstrap.servers` | `kafka:29092` | Required by Debezium 2.x |
| `schema.history.internal.kafka.topic` | `dbhistory.<variant>` | One topic per connector; stores MySQL DDL history |

## Notes

- `database.hostname` is stored as `"localhost"` in the JSON files; `deploy-connectors.sh` replaces it with `$DB_HOST` at deploy time so the same files work regardless of network topology.
- `schema.history.internal.*` is mandatory in Debezium 2.x — without it the connector fails to start.
- Each connector must have a **unique** `database.server.id`; ranges are documented in `CLAUDE.md`.
