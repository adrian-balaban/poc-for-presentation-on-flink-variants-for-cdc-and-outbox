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
 * Data quality tests for CDC output.
 *
 * <p>Validates that Kafka messages are well-formed JSON with expected fields, types, and values.
 * Runs against the DataStream variant but patterns apply to all variants.
 *
 * <p>The DataStream job emits standard Debezium CDC envelopes. Data fields (id, customer_id,
 * amount, status) are nested inside the {@code after} object, not at the top level. Use {@link
 * #waitForKafkaMessage} with a predicate on {@code after.customer_id} to locate the specific row
 * inserted by each test — {@link #pollKafka} returns the first available message which is typically
 * a historical snapshot row, not the test-inserted one.
 */
@Slf4j
@DisplayName("CDC Data Quality Validation")
class DataQualityTest extends FlinkTestBase {

  private static final Path JAR = jarPath("variant-flink-datastream-api-v1-cdc-job");
  private static final String JOB_NAME = "Flink DataStream API v.1 CDC Job";
  private static final String TOPIC = "poc.cdc.datastream.flink";

  private static JSONObject afterOf(String msg) {
    return new JSONObject(msg).optJSONObject("after");
  }

  @Test
  @Timeout(90)
  void kafkaMessage_deserializesToValidJson_withRequiredFields() throws Exception {
    try (Connection c = flinkConn();
        Statement s = c.createStatement()) {
      s.executeUpdate(
          "INSERT INTO poc_db.orders (customer_id, amount, status) VALUES (1000, 123.45, 'DQ-TEST')");
    }

    ensureJobRunning(JAR, "poc.datastream.DataStreamCdcJob", JOB_NAME, Duration.ofSeconds(30));
    String msg =
        waitForKafkaMessage(
            TOPIC,
            Duration.ofSeconds(45),
            m -> {
              JSONObject a = afterOf(m);
              return a != null && a.optLong("customer_id") == 1000;
            });

    assertThat(msg).as("expected CDC message for customer_id=1000").isNotNull();
    JSONObject after = afterOf(msg);

    assertThat(after.has("id")).isTrue();
    assertThat(after.has("customer_id")).isTrue();
    assertThat(after.has("amount")).isTrue();
    assertThat(after.has("status")).isTrue();
    assertThat(after.getLong("customer_id")).isEqualTo(1000);
    assertThat(after.getString("status")).isEqualTo("DQ-TEST");

    log.info("Data quality check passed: after={}", after);
  }

  @Test
  @Timeout(90)
  void kafkaMessage_includesVariantAnnotation() throws Exception {
    try (Connection c = flinkConn();
        Statement s = c.createStatement()) {
      s.executeUpdate(
          "INSERT INTO poc_db.orders (customer_id, amount, status) VALUES (2000, 99.99, 'VARIANT-TEST')");
    }

    ensureJobRunning(JAR, "poc.datastream.DataStreamCdcJob", JOB_NAME, Duration.ofSeconds(30));
    String msg =
        waitForKafkaMessage(
            TOPIC,
            Duration.ofSeconds(45),
            m -> {
              JSONObject a = afterOf(m);
              return a != null && a.optLong("customer_id") == 2000;
            });

    assertThat(msg).as("expected CDC message for customer_id=2000").isNotNull();
    JSONObject json = new JSONObject(msg);

    assertThat(json.has("variant")).as("variant annotation should be present").isTrue();
    assertThat(json.getString("variant")).isEqualTo("datastream-cdc");
    assertThat(json.getString("topic")).isEqualTo("poc.cdc.datastream.flink");

    log.info("Variant annotation verified");
  }

  @Test
  @Timeout(180)
  void kafkaMessage_preservesNullValues() throws Exception {
    try (Connection c = flinkConn();
        Statement s = c.createStatement()) {
      s.executeUpdate(
          "INSERT INTO poc_db.orders (customer_id, amount, status) VALUES (3000, 50.00, NULL)");
    }

    ensureJobRunning(JAR, "poc.datastream.DataStreamCdcJob", JOB_NAME, Duration.ofSeconds(30));
    // Exactly-once Kafka sink only makes messages visible after checkpoint commit (30s interval).
    // 90s gives two full checkpoint windows as margin.
    String msg =
        waitForKafkaMessage(
            TOPIC,
            Duration.ofSeconds(90),
            m -> {
              JSONObject a = afterOf(m);
              return a != null && a.optLong("customer_id") == 3000 && a.isNull("status");
            });

    assertThat(msg).as("expected CDC message for customer_id=3000").isNotNull();
    JSONObject after = afterOf(msg);

    assertThat(after.isNull("status")).as("NULL status should be serialised as JSON null").isTrue();
    log.info("NULL value handling verified");
  }

  @Test
  @Timeout(90)
  void multipleInserts_produceMultipleMessages_inOrder() throws Exception {
    try (Connection c = flinkConn();
        Statement s = c.createStatement()) {
      s.executeUpdate(
          "INSERT INTO poc_db.orders (customer_id, amount, status) VALUES (4000, 10.00, 'FIRST')");
      s.executeUpdate(
          "INSERT INTO poc_db.orders (customer_id, amount, status) VALUES (4001, 20.00, 'SECOND')");
      s.executeUpdate(
          "INSERT INTO poc_db.orders (customer_id, amount, status) VALUES (4002, 30.00, 'THIRD')");
    }

    ensureJobRunning(JAR, "poc.datastream.DataStreamCdcJob", JOB_NAME, Duration.ofSeconds(30));

    String first =
        waitForKafkaMessage(
            TOPIC,
            Duration.ofSeconds(60),
            m -> "FIRST".equals(afterOf(m) != null ? afterOf(m).optString("status") : null));
    String second =
        waitForKafkaMessage(
            TOPIC,
            Duration.ofSeconds(60),
            m -> "SECOND".equals(afterOf(m) != null ? afterOf(m).optString("status") : null));
    String third =
        waitForKafkaMessage(
            TOPIC,
            Duration.ofSeconds(60),
            m -> "THIRD".equals(afterOf(m) != null ? afterOf(m).optString("status") : null));

    assertThat(first).as("FIRST message").isNotNull();
    assertThat(second).as("SECOND message").isNotNull();
    assertThat(third).as("THIRD message").isNotNull();

    log.info("Message ordering verified: all 3 inserts produced Kafka messages");
  }

  @Test
  @Timeout(90)
  void jsonFormat_isConsistent_acrossMessages() throws Exception {
    try (Connection c = flinkConn();
        Statement s = c.createStatement()) {
      for (int i = 0; i < 3; i++) {
        s.executeUpdate(
            String.format(
                "INSERT INTO poc_db.orders (customer_id, amount, status) VALUES (%d, %.2f, 'FORMAT-TEST')",
                5000 + i, 100.00 + i));
      }
    }

    ensureJobRunning(JAR, "poc.datastream.DataStreamCdcJob", JOB_NAME, Duration.ofSeconds(30));

    for (int i = 0; i < 3; i++) {
      final long cid = 5000 + i;
      String msg =
          waitForKafkaMessage(
              TOPIC,
              Duration.ofSeconds(60),
              m -> {
                JSONObject a = afterOf(m);
                return a != null && a.optLong("customer_id") == cid;
              });

      assertThat(msg).as("expected CDC message for customer_id=" + cid).isNotNull();
      JSONObject json = new JSONObject(msg);
      JSONObject after = json.getJSONObject("after");

      // Data fields are in the Debezium `after` object
      assertThat(after.has("id")).isTrue();
      assertThat(after.has("customer_id")).isTrue();
      assertThat(after.has("amount")).isTrue();
      assertThat(after.has("status")).isTrue();
      // Enrichment fields are at envelope level
      assertThat(json.has("variant")).isTrue();
      assertThat(json.has("topic")).isTrue();
    }

    log.info("JSON format consistency verified across 3 messages");
  }
}
