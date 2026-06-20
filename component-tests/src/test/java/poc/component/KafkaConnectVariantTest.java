package poc.component;

import static org.assertj.core.api.Assertions.assertThat;

import java.sql.Connection;
import java.sql.Statement;
import java.time.Duration;
import lombok.extern.slf4j.Slf4j;
import org.json.JSONObject;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

/**
 * Parameterized component tests for all Kafka Connect CDC variants. Replaces 5 duplicate test
 * classes with one parameterized test.
 */
@Slf4j
@DisplayName("Kafka Connect CDC Variants")
class KafkaConnectVariantTest extends KafkaConnectBase {

  @ParameterizedTest(name = "{0}: {1}")
  @CsvSource({
    "kc-datastream-cdc,5510,mysql,poc_db.orders,datastream-cdc,poc.cdc.datastream,DataStream CDC",
    "kc-table-api-cdc,5520,mysql-tableapi,poc_db.orders,table-api-cdc,poc.cdc.tableapi,Table API CDC",
    "kc-sql-api-cdc,5530,mysql-sqlapi,poc_db.orders,sql-api-cdc,poc.cdc.sqlapi,SQL API CDC",
    "kc-yaml-pipeline-cdc,5540,mysql-yaml,poc_db.orders,yaml-pipeline-cdc,poc.kc.yaml,YAML Pipeline CDC",
  })
  @Timeout(120)
  void connector_capturesSnapshot_andPublishesEnrichedEventToKafka(
      String connectorName,
      String serverId,
      String serverName,
      String tableList,
      String variantName,
      String topicPrefix,
      String displayName)
      throws Exception {

    String topic = topicPrefix + ".orders";

    // Deploy connector using shared helper
    String config =
        buildDebeziumConnectorConfig(
            connectorName, serverId, serverName, tableList, variantName, topicPrefix);
    deployConnector(connectorName, config);
    waitForConnectorRunning(connectorName, Duration.ofSeconds(60));

    // Insert a test row with a unique status tag for this variant
    String expectedStatus =
        String.format("KC-%s-TEST", variantName.toUpperCase().replace("-", "_"));
    try (Connection c = flinkConn();
        Statement s = c.createStatement()) {
      s.executeUpdate(
          String.format(
              "INSERT INTO poc_db.orders (customer_id, amount, status) VALUES (999, 99.99, '%s')",
              expectedStatus));
      log.info("Inserted test row for {}", displayName);
    }

    // Wait for the specific test row rather than taking the first available message.
    // The topic may contain stale snapshot messages from previous connector runs that
    // pre-date the EnrichmentTransform, so filtering by status ensures we validate
    // enrichment on a message produced by the current connector deployment.
    String message =
        waitForKafkaMessage(
            topic,
            Duration.ofSeconds(60),
            m -> {
              try {
                JSONObject after = new JSONObject(m).optJSONObject("after");
                return after != null && expectedStatus.equals(after.optString("status"));
              } catch (Exception e) {
                return false;
              }
            });
    assertThat(message)
        .as("expected enriched CDC message with status=" + expectedStatus)
        .isNotNull();

    // Verify enrichment using shared helper
    assertEnrichmentMetadata("Enrichment metadata", message, variantName, topicPrefix);
    log.info("{}: Kafka Connect produced enriched event", displayName);
  }
}
