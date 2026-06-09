package poc.component;

import lombok.extern.slf4j.Slf4j;
import org.json.JSONObject;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.sql.Connection;
import java.sql.Statement;
import java.time.Duration;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Component test for Kafka Connect SQL API variant.
 *
 * Verifies: MySQL binlog → Debezium connector → EnrichmentTransform SMT → Kafka topic.
 * Server-ID 5800 is reserved for kc-sql-api-cdc connector.
 */
@Slf4j
@DisplayName("Kafka Connect SQL API CDC Test")
class KafkaConnectSqlApiTest extends KafkaConnectBase {

    private static final String CONNECTOR_NAME = "kc-sql-api-cdc";
    private static final String TABLES = "poc_db.orders";
    private static final String TOPIC = "poc.cdc.sqlapi.orders";

    @Test
    @Timeout(120)
    void connector_capturesSnapshot_andPublishesEnrichedEventToKafka() throws Exception {
        // Ensure connector is deployed and running
        deploySqlApiConnector();
        waitForConnectorRunning(CONNECTOR_NAME, Duration.ofSeconds(60));

        // Insert a test row
        try (Connection c = flinkConn(); Statement s = c.createStatement()) {
            s.executeUpdate(
                "INSERT INTO poc_db.orders (customer_id, amount, status) VALUES (777, 77.77, 'KC-SQL-TEST')");
            log.info("Inserted test row into {}", TABLES);
        }

        // Poll Kafka topic
        List<String> messages = pollKafka(TOPIC, 1, Duration.ofSeconds(60));
        assertThat(messages).isNotEmpty();

        // Verify enrichment applied
        String message = messages.get(0);
        JSONObject obj = new JSONObject(message);

        assertThat(message).contains("KC-SQL-TEST");
        assertThat(obj.optString("variant")).isEqualTo("sql-api-cdc");
        assertThat(obj.optString("topic")).contains("poc.cdc.sqlapi");
        assertThat(obj.has("transformed_at")).isTrue();

        log.info("SQL API variant: Kafka Connect produced enriched event with variant={}",
            obj.optString("variant"));
    }

    private void deploySqlApiConnector() throws Exception {
        String config = """
            {
              "name": "kc-sql-api-cdc",
              "config": {
                "connector.class": "io.debezium.connector.mysql.MySqlConnector",
                "database.hostname": "localhost",
                "database.port": 3306,
                "database.user": "flink",
                "database.password": "flink",
                "database.server.id": "5800",
                "database.server.name": "mysql-sqlapi",
                "database.include.list": "poc_db",
                "table.include.list": "poc_db.orders",
                "snapshot.mode": "initial",
                "transforms": "enrichment",
                "transforms.enrichment.type": "poc.kafka.connect.EnrichmentTransform",
                "transforms.enrichment.variant.name": "sql-api-cdc",
                "transforms.enrichment.topic.prefix": "poc.cdc.sqlapi",
                "key.converter": "org.apache.kafka.connect.json.JsonConverter",
                "key.converter.schemas.enable": false,
                "value.converter": "org.apache.kafka.connect.json.JsonConverter",
                "value.converter.schemas.enable": false,
                "decimal.handling.mode": "string",
                "include.schema.changes": false,
                "topic.prefix": "poc.cdc.sqlapi"
              }
            }
            """;
        deployConnector(CONNECTOR_NAME, config);
    }
}
