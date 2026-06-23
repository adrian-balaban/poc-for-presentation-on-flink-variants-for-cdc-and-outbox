package poc.component;

import static org.assertj.core.api.Assertions.assertThat;

import java.sql.Connection;
import java.sql.Statement;
import java.time.Duration;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.json.JSONObject;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * Component test for Kafka Connect Outbox variant.
 *
 * <p>Verifies: MySQL outbox_events → Debezium connector → OutboxRoutingTransform SMT → dynamic
 * topic routing (poc.kc.outbox.<destination>).
 *
 * <p>Server-ID 5550 is reserved for kc-outbox-cdc connector.
 */
@Slf4j
@DisplayName("Kafka Connect Outbox CDC Test")
class KafkaConnectOutboxTest extends KafkaConnectBase {

  private static final String CONNECTOR_NAME = "kc-outbox-cdc";
  private static final String OUTBOX_TABLE = "poc_db.outbox_events";

  @Test
  @Timeout(120)
  void connector_routesByDestination_andPublishesToDynamicTopics() throws Exception {
    // Ensure connector is deployed and running
    deployOutboxConnector();
    waitForConnectorRunning(CONNECTOR_NAME, Duration.ofSeconds(60));

    // Insert outbox events with different destinations
    try (Connection c = flinkConn();
        Statement s = c.createStatement()) {
      s.executeUpdate(
          "INSERT INTO poc_db.outbox_events (destination, payload) "
              + "VALUES ('orders-svc', '{\"id\": 1, \"amount\": 100.0}')");
      s.executeUpdate(
          "INSERT INTO poc_db.outbox_events (destination, payload) "
              + "VALUES ('payment-svc', '{\"id\": 2, \"status\": \"paid\"}')");
      log.info("Inserted test outbox events");
    }

    // Poll orders-svc topic
    List<String> ordersMessages = pollKafka("poc.kc.outbox.orders-svc", 1, Duration.ofSeconds(60));
    assertThat(ordersMessages).isNotEmpty();

    // Verify routing metadata
    String ordersMsg = ordersMessages.get(0);
    JSONObject ordersObj = new JSONObject(ordersMsg);
    assertThat(ordersObj.optString("_route_destination")).isEqualTo("orders-svc");
    assertThat(ordersObj.optString("_route_topic")).isEqualTo("poc.kc.outbox.orders-svc");
    assertThat(ordersObj.has("_routed_at")).isTrue();

    // Poll payment-svc topic
    List<String> paymentMessages =
        pollKafka("poc.kc.outbox.payment-svc", 1, Duration.ofSeconds(60));
    assertThat(paymentMessages).isNotEmpty();

    String paymentMsg = paymentMessages.get(0);
    JSONObject paymentObj = new JSONObject(paymentMsg);
    assertThat(paymentObj.optString("_route_destination")).isEqualTo("payment-svc");
    assertThat(paymentObj.optString("_route_topic")).isEqualTo("poc.kc.outbox.payment-svc");

    log.info("Outbox variant: Events routed to correct topics by destination");
  }

  private void deployOutboxConnector() throws Exception {
    String config =
            """
            {
              "name": "kc-outbox-cdc",
              "config": {
                "connector.class": "io.debezium.connector.mysql.MySqlConnector",
                "database.hostname": "localhost",
                "database.port": 3306,
                "database.user": "flink",
                "database.password": "flink",
                "database.server.id": "5550",
                "database.server.name": "mysql-outbox",
                "database.include.list": "poc_db",
                "table.include.list": "poc_db.outbox_events",
                "snapshot.mode": "initial",
                "transforms": "routing",
                "transforms.routing.type": "poc.kafka.connect.OutboxRoutingTransform",
                "transforms.routing.topic.prefix": "poc.kc.outbox",
                "transforms.routing.destination.field": "destination",
                "key.converter": "org.apache.kafka.connect.json.JsonConverter",
                "key.converter.schemas.enable": false,
                "value.converter": "org.apache.kafka.connect.json.JsonConverter",
                "value.converter.schemas.enable": false,
                "decimal.handling.mode": "string",
                "include.schema.changes": false,
                "schema.history.internal.kafka.bootstrap.servers": "SCHEMA_HISTORY_PLACEHOLDER",
                "schema.history.internal.kafka.topic": "dbhistory.outbox",
                "topic.prefix": "poc.kc.outbox"
              }
            }
            """
            .replace("\"localhost\"", "\"" + DB_HOST + "\"")
            .replace("SCHEMA_HISTORY_PLACEHOLDER", SCHEMA_HISTORY_BOOTSTRAP);
    deployConnector(CONNECTOR_NAME, config);
  }
}
