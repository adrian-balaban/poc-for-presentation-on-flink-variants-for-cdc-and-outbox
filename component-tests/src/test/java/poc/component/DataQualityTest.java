package poc.component;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Path;
import java.sql.Connection;
import java.sql.Statement;
import java.time.Duration;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.KafkaConsumer;
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
  private static final String TOPIC = "poc.flink.datastream.orders";

  private static JSONObject afterOf(String msg) {
    return new JSONObject(msg).optJSONObject("after");
  }

  @Test
  @Timeout(90)
  void kafkaMessage_deserializesToValidJson_withRequiredFields() throws Exception {
    // Per-run customer_id so the predicate selects this run's row out of the shared,
    // never-truncated topic (which retains rows from prior runs and the other tests here).
    final long cid = uniqueId();
    final String marker = "DQ-TEST-" + cid;
    try (Connection c = flinkConn();
        Statement s = c.createStatement()) {
      s.executeUpdate(
          String.format(
              "INSERT INTO poc_db.orders (customer_id, amount, status) VALUES (%d, 123.45, '%s')",
              cid, marker));
    }

    ensureJobRunning(JAR, "poc.datastream.DataStreamCdcJob", JOB_NAME, Duration.ofSeconds(30));
    String msg =
        waitForKafkaMessage(
            TOPIC,
            Duration.ofSeconds(45),
            m -> {
              JSONObject a = afterOf(m);
              return a != null && a.optLong("customer_id") == cid;
            });

    assertThat(msg).as("expected CDC message for customer_id=" + cid).isNotNull();
    JSONObject after = afterOf(msg);

    assertThat(after.has("id")).isTrue();
    assertThat(after.has("customer_id")).isTrue();
    assertThat(after.has("amount")).isTrue();
    assertThat(after.has("status")).isTrue();
    assertThat(after.getLong("customer_id")).isEqualTo(cid);
    assertThat(after.getString("status")).isEqualTo(marker);

    log.info("Data quality check passed: after={}", after);
  }

  @Test
  @Timeout(90)
  void kafkaMessage_includesVariantAnnotation() throws Exception {
    final long cid = uniqueId();
    try (Connection c = flinkConn();
        Statement s = c.createStatement()) {
      s.executeUpdate(
          String.format(
              "INSERT INTO poc_db.orders (customer_id, amount, status) VALUES (%d, 99.99, 'VARIANT-TEST-%d')",
              cid, cid));
    }

    ensureJobRunning(JAR, "poc.datastream.DataStreamCdcJob", JOB_NAME, Duration.ofSeconds(30));
    String msg =
        waitForKafkaMessage(
            TOPIC,
            Duration.ofSeconds(45),
            m -> {
              JSONObject a = afterOf(m);
              return a != null && a.optLong("customer_id") == cid;
            });

    assertThat(msg).as("expected CDC message for customer_id=" + cid).isNotNull();
    JSONObject json = new JSONObject(msg);

    assertThat(json.has("variant")).as("variant annotation should be present").isTrue();
    assertThat(json.getString("variant")).isEqualTo("datastream-cdc");
    assertThat(json.getString("topic")).isEqualTo("poc.flink.datastream.orders");

    log.info("Variant annotation verified");
  }

  @Test
  @Timeout(180)
  void kafkaMessage_preservesNullValues() throws Exception {
    final long cid = uniqueId();
    try (Connection c = flinkConn();
        Statement s = c.createStatement()) {
      s.executeUpdate(
          String.format(
              "INSERT INTO poc_db.orders (customer_id, amount, status) VALUES (%d, 50.00, NULL)",
              cid));
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
              return a != null && a.optLong("customer_id") == cid && a.isNull("status");
            });

    assertThat(msg).as("expected CDC message for customer_id=" + cid).isNotNull();
    JSONObject after = afterOf(msg);

    assertThat(after.isNull("status")).as("NULL status should be serialised as JSON null").isTrue();
    log.info("NULL value handling verified");
  }

  @Test
  @Timeout(90)
  void multipleInserts_produceMultipleMessages_inOrder() throws Exception {
    final long stamp = uniqueId();
    final String first = "FIRST-" + stamp;
    final String second = "SECOND-" + stamp;
    final String third = "THIRD-" + stamp;
    try (Connection c = flinkConn();
        Statement s = c.createStatement()) {
      s.executeUpdate(
          String.format(
              "INSERT INTO poc_db.orders (customer_id, amount, status) VALUES (%d, 10.00, '%s')",
              stamp, first));
      s.executeUpdate(
          String.format(
              "INSERT INTO poc_db.orders (customer_id, amount, status) VALUES (%d, 20.00, '%s')",
              stamp, second));
      s.executeUpdate(
          String.format(
              "INSERT INTO poc_db.orders (customer_id, amount, status) VALUES (%d, 30.00, '%s')",
              stamp, third));
    }

    ensureJobRunning(JAR, "poc.datastream.DataStreamCdcJob", JOB_NAME, Duration.ofSeconds(30));

    // Verify the three inserts reach Kafka in insertion order. poc.flink.datastream.orders has a
    // single partition, so Kafka offset order equals the sink's production order, which for a CDC
    // source equals binlog (insertion) order; the MySQL auto-increment after.id must therefore
    // increase in lockstep with the offset. Both are asserted strictly increasing across
    // consecutive
    // markers — that is the ordering invariant the test name promises, which the previous three
    // independent waitForKafkaMessage calls (each with a fresh consumer group) never checked.
    assertEmittedInOrder(TOPIC, Duration.ofSeconds(90), first, second, third);

    log.info("Message ordering verified: 3 inserts produced Kafka messages in order");
  }

  /**
   * Consumes {@code topic} from earliest until every {@code expectedStatuses} marker has been
   * located, recording each marker's Kafka offset and {@code after.id}. Then asserts the markers
   * were emitted in the given order. {@code poc.flink.datastream.orders} has a single partition, so
   * Kafka offset order equals the sink's production order, which for a CDC source equals binlog
   * (insertion) order; the MySQL auto-increment {@code after.id} must therefore increase in
   * lockstep with the offset. Both are asserted strictly increasing across consecutive markers.
   */
  private void assertEmittedInOrder(String topic, Duration timeout, String... expectedStatuses) {
    Properties props = new Properties();
    props.put("bootstrap.servers", KAFKA_BOOTSTRAP);
    props.put("group.id", "order-" + UUID.randomUUID());
    props.put("auto.offset.reset", "earliest");
    props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
    props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

    Map<String, Long> markerToOffset = new HashMap<>();
    Map<String, Long> markerToId = new HashMap<>();
    Set<String> need = new HashSet<>(Arrays.asList(expectedStatuses));
    long deadline = System.currentTimeMillis() + timeout.toMillis();
    try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
      consumer.subscribe(List.of(topic));
      while (!need.isEmpty() && System.currentTimeMillis() < deadline) {
        for (var record : consumer.poll(Duration.ofMillis(500))) {
          JSONObject a;
          try {
            a = new JSONObject(record.value()).optJSONObject("after");
          } catch (Exception e) {
            continue;
          }
          if (a == null) {
            continue;
          }
          // optString returns "" for absent/null status, which cannot match a non-empty marker.
          String status = a.optString("status");
          if (!status.isEmpty() && need.remove(status)) {
            markerToOffset.put(status, record.offset());
            markerToId.put(status, a.optLong("id"));
          }
        }
      }
    }

    for (String marker : expectedStatuses) {
      assertThat(markerToOffset.containsKey(marker))
          .as("located CDC event with status " + marker)
          .isTrue();
    }
    // Single-partition topic → offset order is the sink's production order = binlog order.
    for (int i = 0; i + 1 < expectedStatuses.length; i++) {
      String prev = expectedStatuses[i];
      String next = expectedStatuses[i + 1];
      long prevOff = markerToOffset.get(prev);
      long nextOff = markerToOffset.get(next);
      long prevId = markerToId.get(prev);
      long nextId = markerToId.get(next);
      assertThat(nextOff)
          .as("offset of %s (%d) > offset of %s (%d)", next, nextOff, prev, prevOff)
          .isGreaterThan(prevOff);
      assertThat(nextId)
          .as("after.id of %s (%d) > after.id of %s (%d)", next, nextId, prev, prevId)
          .isGreaterThan(prevId);
    }
  }

  @Test
  @Timeout(90)
  void jsonFormat_isConsistent_acrossMessages() throws Exception {
    final long base = uniqueId();
    try (Connection c = flinkConn();
        Statement s = c.createStatement()) {
      for (int i = 0; i < 3; i++) {
        s.executeUpdate(
            String.format(
                "INSERT INTO poc_db.orders (customer_id, amount, status) VALUES (%d, %.2f, 'FORMAT-TEST-%d')",
                base + i, 100.00 + i, base));
      }
    }

    ensureJobRunning(JAR, "poc.datastream.DataStreamCdcJob", JOB_NAME, Duration.ofSeconds(30));

    for (int i = 0; i < 3; i++) {
      final long cid = base + i;
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
