# Kafka Connect Connector Configurations

These JSON files are generated from a shared template. Key differences per variant:

| File | Server-ID | Server-Name | Variant Name | Topic Prefix |
|------|-----------|-------------|--------------|--------------|
| kc-datastream-cdc.json | 5900 | mysql | datastream-cdc | poc.cdc.datastream |
| kc-table-api-cdc.json | 6000 | mysql-tableapi | table-api-cdc | poc.cdc.tableapi |
| kc-sql-api-cdc.json | 5800 | mysql-sqlapi | sql-api-cdc | poc.cdc.sqlapi |
| kc-outbox-cdc.json | 5600 | mysql-outbox | outbox-cdc | poc.cdc.outbox |
| kc-yaml-pipeline-cdc.json | 5700 | mysql-yaml | yaml-pipeline-cdc | poc.cdc.yaml |

To add a new variant, copy one of these files and update the above fields.

Common settings (shared across all connectors):
- `connector.class`: io.debezium.connector.mysql.MySqlConnector
- `database.hostname`: localhost
- `database.port`: 3306
- `database.user`: flink
- `database.password`: flink
- `database.include.list`: poc_db
- `table.include.list`: poc_db.orders (standard for CDC variants) or poc_db.outbox_events (outbox variant)
- `snapshot.mode`: initial
- `transforms`: enrichment (or routing for outbox)
- `key.converter`: org.apache.kafka.connect.json.JsonConverter
- `value.converter`: org.apache.kafka.connect.json.JsonConverter
