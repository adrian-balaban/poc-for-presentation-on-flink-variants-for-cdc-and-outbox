package poc.component;

import static org.assertj.core.api.Assertions.assertThat;

import java.sql.Connection;
import java.sql.ResultSet;
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

    // Capture the auto-increment row id (PK) of each inserted outbox event so the predicate can
    // match on after.id exactly rather than a bare numeric substring. The outbox topics are shared
    // and never truncated, so a substring match on a random uid could collide with a prior run's
    // ts_ms/pos/after.id field — and the routing-metadata assertions are identical for every
    // orders-svc message, so a stale match would pass silently. The PK is unique and unambiguous;
    // this mirrors CdcParityTest's exact-field matching.
    long uid = uniqueId();
    String ordersPayload = String.format("{\"id\":%d,\"amount\":100.0}", uid);
    String paymentPayload = String.format("{\"id\":%d,\"status\":\"paid\"}", uid);

    long ordersRowId;
    long paymentRowId;
    try (Connection c = flinkConn();
        Statement s = c.createStatement()) {
      s.executeUpdate(
          String.format(
              "INSERT INTO poc_db.outbox_events (destination, payload) VALUES ('orders-svc', '%s')",
              ordersPayload),
          Statement.RETURN_GENERATED_KEYS);
      try (ResultSet keys = s.getGeneratedKeys()) {
        assertThat(keys.next()).as("got generated id for orders-svc event").isTrue();
        ordersRowId = keys.getLong(1);
      }
      s.executeUpdate(
          String.format(
              "INSERT INTO poc_db.outbox_events (destination, payload) VALUES ('payment-svc', '%s')",
              paymentPayload),
          Statement.RETURN_GENERATED_KEYS);
      try (ResultSet keys = s.getGeneratedKeys()) {
        assertThat(keys.next()).as("got generated id for payment-svc event").isTrue();
        paymentRowId = keys.getLong(1);
      }
      log.info(
          "Inserted test outbox events uid={} (ordersRowId={}, paymentRowId={})",
          uid,
          ordersRowId,
          paymentRowId);
    }

    // Wait for this run's orders-svc event; match on the exact PK + destination, not a substring.
    final long ordersId = ordersRowId;
    String ordersMsg =
        waitForKafkaMessage(
            "poc.kc.outbox.orders-svc",
            Duration.ofSeconds(60),
            m -> {
              JSONObject a = afterOf(m);
              return a != null
                  && a.optLong("id") == ordersId
                  && "orders-svc".equals(a.optString("destination"));
            });
    assertThat(ordersMsg)
        .as("expected orders-svc outbox event for row id=" + ordersRowId)
        .isNotNull();

    // Verify routing metadata
    JSONObject ordersObj = new JSONObject(ordersMsg);
    assertThat(ordersObj.optString("_route_destination")).isEqualTo("orders-svc");
    assertThat(ordersObj.optString("_route_topic")).isEqualTo("poc.kc.outbox.orders-svc");
    assertThat(ordersObj.has("_routed_at")).isTrue();

    // Wait for this run's payment-svc event.
    final long paymentId = paymentRowId;
    String paymentMsg =
        waitForKafkaMessage(
            "poc.kc.outbox.payment-svc",
            Duration.ofSeconds(60),
            m -> {
              JSONObject a = afterOf(m);
              return a != null
                  && a.optLong("id") == paymentId
                  && "payment-svc".equals(a.optString("destination"));
            });
    assertThat(paymentMsg)
        .as("expected payment-svc outbox event for row id=" + paymentRowId)
        .isNotNull();

    JSONObject paymentObj = new JSONObject(paymentMsg);
    assertThat(paymentObj.optString("_route_destination")).isEqualTo("payment-svc");
    assertThat(paymentObj.optString("_route_topic")).isEqualTo("poc.kc.outbox.payment-svc");

    log.info("Outbox variant: Events routed to correct topics by destination");
  }

  private static JSONObject afterOf(String msg) {
    try {
      return new JSONObject(msg).optJSONObject("after");
    } catch (Exception e) {
      return null;
    }
  }

  private void deployOutboxConnector() throws Exception {
    String config = buildOutboxConnectorConfig(CONNECTOR_NAME, "5550", "mysql-outbox");
    deployConnector(CONNECTOR_NAME, config);
  }
}
