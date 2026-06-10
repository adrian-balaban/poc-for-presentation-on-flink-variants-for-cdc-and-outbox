package poc.component;

import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.sql.Connection;
import java.sql.Statement;
import java.time.Duration;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Parameterized component tests for all Kafka Connect CDC variants.
 * Replaces 5 duplicate test classes with one parameterized test.
 */
@Slf4j
@DisplayName("Kafka Connect CDC Variants")
class KafkaConnectVariantTest extends KafkaConnectBase {

    @ParameterizedTest(name = "{0}: {1}")
    @CsvSource({
        "kc-datastream-cdc,5900,mysql,poc_db.orders,datastream-cdc,poc.cdc.datastream,DataStream CDC",
        "kc-table-api-cdc,6000,mysql-tableapi,poc_db.orders,table-api-cdc,poc.cdc.tableapi,Table API CDC",
        "kc-sql-api-cdc,5800,mysql-sqlapi,poc_db.orders,sql-api-cdc,poc.cdc.sqlapi,SQL API CDC",
        "kc-yaml-pipeline-cdc,5700,mysql-yaml,poc_db.orders,yaml-pipeline-cdc,poc.cdc.yaml,YAML Pipeline CDC",
    })
    @Timeout(120)
    void connector_capturesSnapshot_andPublishesEnrichedEventToKafka(
            String connectorName, String serverId, String serverName,
            String tableList, String variantName, String topicPrefix, String displayName) throws Exception {

        String topic = topicPrefix + ".orders";

        // Deploy connector using shared helper
        String config = buildDebeziumConnectorConfig(
            connectorName, serverId, serverName, tableList, variantName, topicPrefix);
        deployConnector(connectorName, config);
        waitForConnectorRunning(connectorName, Duration.ofSeconds(60));

        // Insert a test row
        try (Connection c = flinkConn(); Statement s = c.createStatement()) {
            s.executeUpdate(
                String.format("INSERT INTO poc_db.orders (customer_id, amount, status) VALUES (999, 99.99, 'KC-%s-TEST')",
                    variantName.toUpperCase().replace("-", "_")));
            log.info("Inserted test row for {}", displayName);
        }

        // Poll Kafka topic
        List<String> messages = pollKafka(topic, 1, Duration.ofSeconds(60));
        assertThat(messages).isNotEmpty();

        // Verify enrichment using shared helper
        String message = messages.get(0);
        assertEnrichmentMetadata("Enrichment metadata", message, variantName, topicPrefix);
        log.info("{}: Kafka Connect produced enriched event", displayName);
    }
}
