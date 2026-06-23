package poc.component;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Path;
import java.sql.Connection;
import java.sql.Statement;
import java.time.Duration;
import lombok.extern.slf4j.Slf4j;
import org.json.JSONObject;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * Component test for Outbox variant.
 *
 * <p>Submits the fat-jar to the Flink JobManager container (localhost:8081) via REST and verifies
 * outbox events reach Kafka. Job remains running after the test.
 *
 * <p>Routing note: unlike the Kafka Connect outbox variant (which uses the OutboxRoutingTransform
 * SMT to fan out to per-destination topics {@code poc.kc.outbox.<destination>}), the Flink {@code
 * OutboxJob} deliberately sinks <em>all</em> events to a single topic {@code
 * poc.flink.outbox.outbox-events} — {@code OutboxRouter} only logs the destination→topic mapping
 * for this POC. So the test asserts on the single topic that each event's {@code destination} field
 * is faithfully preserved in the Debezium {@code after} object for two distinct destinations, not
 * that events land on separate sub-topics.
 */
@Slf4j
@DisplayName("Flink DataStream API v.1 : Outbox Test")
class DataStreamOutboxTest extends FlinkTestBase {

  private static final Path JAR = jarPath("variant-flink-datastream-api-v1-outbox-job");
  private static final String TOPIC = "poc.flink.outbox.outbox-events";

  private static JSONObject afterOf(String msg) {
    try {
      return new JSONObject(msg).optJSONObject("after");
    } catch (Exception e) {
      return null;
    }
  }

  @Test
  @Timeout(120)
  void outboxSource_preservesDestinationField_forMultipleDestinations() throws Exception {
    // Unique destinations so the predicates reliably select this test's events out of the seeded
    // payments/notifications/audit snapshot rows already on the topic.
    long stamp = System.currentTimeMillis() % 1_000_000;
    String destA = "payments-" + stamp;
    String destB = "notifications-" + stamp;
    try (Connection c = flinkConn();
        Statement s = c.createStatement()) {
      s.executeUpdate(
          String.format(
              "INSERT INTO poc_db.outbox_events (destination, payload) "
                  + "VALUES ('%s', '{\"order_id\":%d}')",
              destA, stamp));
      s.executeUpdate(
          String.format(
              "INSERT INTO poc_db.outbox_events (destination, payload) "
                  + "VALUES ('%s', '{\"order_id\":%d}')",
              destB, stamp));
    }

    ensureJobRunning(
        JAR, "poc.outbox.OutboxJob", "Flink DataStream API v.1 Outbox Job", Duration.ofSeconds(30));

    // Both events land on the single outbox topic — assert each arrives with its destination
    // preserved in the after object. A regression that dropped or mis-parsed the destination
    // field would now fail instead of passing on a generic "payments" substring.
    String msgA =
        waitForKafkaMessage(
            TOPIC,
            Duration.ofSeconds(60),
            m -> {
              JSONObject a = afterOf(m);
              return a != null && destA.equals(a.optString("destination"));
            });
    assertThat(msgA).as("expected outbox event with destination=" + destA).isNotNull();
    assertThat(afterOf(msgA).optString("destination")).isEqualTo(destA);

    String msgB =
        waitForKafkaMessage(
            TOPIC,
            Duration.ofSeconds(60),
            m -> {
              JSONObject a = afterOf(m);
              return a != null && destB.equals(a.optString("destination"));
            });
    assertThat(msgB).as("expected outbox event with destination=" + destB).isNotNull();
    assertThat(afterOf(msgB).optString("destination")).isEqualTo(destB);

    log.info("Outbox CDC: validated destination field preservation for {} and {}", destA, destB);
  }
}
