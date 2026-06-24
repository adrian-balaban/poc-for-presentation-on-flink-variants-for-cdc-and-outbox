package poc.component;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Path;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.time.Duration;
import lombok.extern.slf4j.Slf4j;
import org.json.JSONObject;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * Parity test — the POC's central thesis is that Flink CDC can replace the Kafka Connect Debezium
 * connector for the same CDC pattern. This test exercises both engines against the <em>same</em>
 * inserted row and asserts they emit semantically equivalent Debezium {@code after} payloads, so
 * drift in field names, envelope shape, or value encoding between engines is caught.
 *
 * <p>Compares only fields whose encoding is identical across both engines: {@code customer_id}
 * (BIGINT → long) and {@code status} (STRING). {@code amount} (DECIMAL) is intentionally excluded —
 * the KC connector uses {@code decimal.handling.mode=string} ("11.11") while the Flink DataStream
 * job uses the Debezium default decimal encoding, so the two representations differ by design and
 * are not a parity bug.
 *
 * <p>Extends {@link KafkaConnectBase} for the KC REST client and reuses the package-private {@code
 * FlinkTestBase.flink} client + {@code ensureJobRunning} for the Flink side, asserting both are
 * available before running (skips gracefully if either engine is down).
 */
@Slf4j
@DisplayName("Flink vs Kafka Connect CDC Parity")
class CdcParityTest extends KafkaConnectBase {

  private static final Path JAR = FlinkTestBase.jarPath("variant-flink-datastream-api-v1-cdc-job");
  private static final String JOB_NAME = "Flink DataStream API v.1 CDC Job";
  private static final String FLINK_TOPIC = "poc.flink.datastream.orders";
  private static final String KC_TOPIC = "poc.kc.datastream.orders";
  private static final String CONNECTOR_NAME = "kc-datastream-cdc";

  private static volatile Boolean flinkAvailableForParity = null;

  @BeforeEach
  void verifyFlinkAvailableForParity() {
    if (flinkAvailableForParity == null) {
      flinkAvailableForParity = FlinkTestBase.flink.isAvailable();
    }
    Assumptions.assumeTrue(
        flinkAvailableForParity,
        "Flink JobManager not available — skipping parity test. Set FLINK_REST_URL or run the Podman stack.");
  }

  private static JSONObject afterOf(String msg) {
    try {
      JSONObject a = new JSONObject(msg).optJSONObject("after");
      return a != null && a.has("id") ? a : null;
    } catch (Exception e) {
      return null;
    }
  }

  @Test
  @Timeout(180)
  void flinkAndKafkaConnect_emitEquivalentAfterPayloadForSameRow() throws Exception {
    // Ensure both engines are running against poc_db.orders.
    FlinkTestBase.ensureJobRunning(
        JAR, "poc.datastream.DataStreamCdcJob", JOB_NAME, Duration.ofSeconds(120));
    String config =
        buildDebeziumConnectorConfig(
            CONNECTOR_NAME,
            "5510",
            "mysql",
            "poc_db.orders",
            "datastream-cdc",
            "poc.kc.datastream");
    deployConnector(CONNECTOR_NAME, config);
    waitForConnectorRunning(CONNECTOR_NAME, Duration.ofSeconds(60));

    long stamp = uniqueId();
    String parityMarker = "PARITY-" + stamp;
    long rowId;
    try (Connection c = flinkConn();
        Statement s = c.createStatement()) {
      s.executeUpdate(
          String.format(
              "INSERT INTO poc_db.orders (customer_id, amount, status) VALUES (%d, 12.34, '%s')",
              stamp, parityMarker),
          Statement.RETURN_GENERATED_KEYS);
      try (ResultSet keys = s.getGeneratedKeys()) {
        assertThat(keys.next()).isTrue();
        rowId = keys.getLong(1);
      }
    }

    // Locate the same row in both engines' output by id.
    JSONObject flinkAfter =
        afterOf(
            waitForKafkaMessage(
                FLINK_TOPIC,
                Duration.ofSeconds(60),
                m -> {
                  JSONObject a = afterOf(m);
                  return a != null && a.optLong("id") == rowId;
                }));
    JSONObject kcAfter =
        afterOf(
            waitForKafkaMessage(
                KC_TOPIC,
                Duration.ofSeconds(60),
                m -> {
                  JSONObject a = afterOf(m);
                  return a != null && a.optLong("id") == rowId;
                }));

    assertThat(flinkAfter).as("Flink emitted an after payload for id=" + rowId).isNotNull();
    assertThat(kcAfter).as("Kafka Connect emitted an after payload for id=" + rowId).isNotNull();

    // Parity on identity + payload fields that share encoding across engines.
    assertThat(flinkAfter.optLong("id")).isEqualTo(rowId);
    assertThat(kcAfter.optLong("id")).isEqualTo(rowId);
    assertThat(flinkAfter.optLong("customer_id"))
        .as("customer_id matches across engines")
        .isEqualTo(kcAfter.optLong("customer_id"));
    assertThat(flinkAfter.optString("status"))
        .as("status matches across engines")
        .isEqualTo(kcAfter.optString("status"))
        .isEqualTo(parityMarker);

    log.info(
        "Parity verified for id={}: Flink and KC agree on customer_id={} status='{}'",
        rowId,
        flinkAfter.optLong("customer_id"),
        parityMarker);
  }
}
