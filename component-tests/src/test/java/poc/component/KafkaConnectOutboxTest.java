package poc.component;

import static org.assertj.core.api.Assertions.assertThat;

import java.sql.Connection;
import java.sql.Statement;
import java.time.Duration;
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

  @Test
  @Timeout(120)
  void connector_routesByDestination_andPublishesToDynamicTopics() throws Exception {
    // Ensure connector is deployed and running
    deployOutboxConnector();
    waitForConnectorRunning(CONNECTOR_NAME, Duration.ofSeconds(60));

    // Unique per-run payload marker so the predicate selects this run's events out of the
    // shared, never-truncated outbox topics (which retain events from prior runs).
    long uid = uniqueId();
    String ordersPayload = String.format("{\"id\":%d,\"amount\":100.0}", uid);
    String paymentPayload = String.format("{\"id\":%d,\"status\":\"paid\"}", uid);

    // Insert outbox events with different destinations
    try (Connection c = flinkConn();
        Statement s = c.createStatement()) {
      s.executeUpdate(
          String.format(
              "INSERT INTO poc_db.outbox_events (destination, payload) VALUES ('orders-svc', '%s')",
              ordersPayload));
      s.executeUpdate(
          String.format(
              "INSERT INTO poc_db.outbox_events (destination, payload) VALUES ('payment-svc', '%s')",
              paymentPayload));
      log.info("Inserted test outbox events for uid={}", uid);
    }

    // Wait for this run's orders-svc event; match on the unique payload marker.
    String ordersMsg =
        waitForKafkaMessage(
            "poc.kc.outbox.orders-svc",
            Duration.ofSeconds(60),
            m -> {
              try {
                return m != null && m.contains(String.valueOf(uid)) && m.contains("orders-svc");
              } catch (Exception e) {
                return false;
              }
            });
    assertThat(ordersMsg).as("expected orders-svc outbox event for uid=" + uid).isNotNull();

    // Verify routing metadata
    JSONObject ordersObj = new JSONObject(ordersMsg);
    assertThat(ordersObj.optString("_route_destination")).isEqualTo("orders-svc");
    assertThat(ordersObj.optString("_route_topic")).isEqualTo("poc.kc.outbox.orders-svc");
    assertThat(ordersObj.has("_routed_at")).isTrue();

    // Wait for this run's payment-svc event.
    String paymentMsg =
        waitForKafkaMessage(
            "poc.kc.outbox.payment-svc",
            Duration.ofSeconds(60),
            m -> {
              try {
                return m != null && m.contains(String.valueOf(uid)) && m.contains("payment-svc");
              } catch (Exception e) {
                return false;
              }
            });
    assertThat(paymentMsg).as("expected payment-svc outbox event for uid=" + uid).isNotNull();

    JSONObject paymentObj = new JSONObject(paymentMsg);
    assertThat(paymentObj.optString("_route_destination")).isEqualTo("payment-svc");
    assertThat(paymentObj.optString("_route_topic")).isEqualTo("poc.kc.outbox.payment-svc");

    log.info("Outbox variant: Events routed to correct topics by destination");
  }

  private void deployOutboxConnector() throws Exception {
    String config = buildOutboxConnectorConfig(CONNECTOR_NAME, "5550", "mysql-outbox");
    deployConnector(CONNECTOR_NAME, config);
  }
}
