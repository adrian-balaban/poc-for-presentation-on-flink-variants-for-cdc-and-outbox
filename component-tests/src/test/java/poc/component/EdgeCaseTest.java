package poc.component;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Path;
import java.sql.Connection;
import java.sql.Statement;
import java.time.Duration;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import java.util.function.Predicate;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.json.JSONObject;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * Edge-case and continued-operation tests for the DataStream CDC variant.
 *
 * <p>These do <em>not</em> inject infrastructure failures (killing MySQL/Kafka mid-run is flaky and
 * environment-dependent — see {@link ExactlyOnceInvariantTest} for the deterministic exactly-once
 * substitute and {@link JobHealthTest} for restart-loop detection). Instead they assert the job
 * keeps producing after a quiet period and a write burst, and that awkward payloads — large strings
 * and embedded quotes/apostrophes — survive the CDC round-trip intact.
 *
 * <p>Every test tags its rows with a per-run {@code stamp} from {@link ContainerBase#uniqueId()} so
 * its {@code waitForKafkaMessage} predicates select only this run's rows out of the shared,
 * never-truncated {@code poc.flink.datastream.orders} topic, which retains rows from prior runs and
 * from the other tests reading the same topic.
 */
@Slf4j
@DisplayName("CDC Edge Cases & Continued Operation")
class EdgeCaseTest extends FlinkTestBase {

  private static final Path JAR = jarPath("variant-flink-datastream-api-v1-cdc-job");
  private static final String JOB_NAME = "Flink DataStream API v.1 CDC Job";
  private static final String TOPIC = "poc.flink.datastream.orders";

  private static JSONObject afterOf(String msg) {
    try {
      return new JSONObject(msg).optJSONObject("after");
    } catch (Exception e) {
      return null;
    }
  }

  @Test
  @Timeout(120)
  void jobContinuesAfterQuietPeriod() throws Exception {
    long stamp = uniqueId();
    String before = "BEFORE-DELAY-" + stamp;
    String after = "AFTER-DELAY-" + stamp;

    // Insert initial row before job starts.
    try (Connection c = flinkConn();
        Statement s = c.createStatement()) {
      s.executeUpdate(
          String.format(
              "INSERT INTO poc_db.orders (customer_id, amount, status) VALUES (%d, 1.00, '%s')",
              stamp, before));
    }

    ensureJobRunning(JAR, "poc.datastream.DataStreamCdcJob", JOB_NAME, Duration.ofSeconds(30));
    assertThat(waitForKafkaMessage(TOPIC, Duration.ofSeconds(45), m -> m.contains(before)))
        .as("expected BEFORE-DELAY event for stamp=" + stamp)
        .isNotNull();

    // Quiet period, then insert again — the job must still emit the new row.
    Thread.sleep(5000);
    try (Connection c = flinkConn();
        Statement s = c.createStatement()) {
      s.executeUpdate(
          String.format(
              "INSERT INTO poc_db.orders (customer_id, amount, status) VALUES (%d, 2.00, '%s')",
              stamp, after));
    }

    assertThat(waitForKafkaMessage(TOPIC, Duration.ofSeconds(45), m -> m.contains(after)))
        .as("expected AFTER-DELAY event for stamp=" + stamp)
        .isNotNull();

    log.info("Job kept producing across a quiet period (stamp={})", stamp);
  }

  @Test
  @Timeout(120)
  void jobStaysBusyUnderLoadAndKeepsProducing() throws Exception {
    ensureJobRunning(JAR, "poc.datastream.DataStreamCdcJob", JOB_NAME, Duration.ofSeconds(30));

    long stamp = uniqueId();
    String burst = "BURST-" + stamp;
    String afterBurst = "AFTER-BURST-" + stamp;
    int burstCount = 20;

    // Write burst of rows tagged with this run's stamp.
    try (Connection c = flinkConn();
        Statement s = c.createStatement()) {
      for (int i = 0; i < burstCount; i++) {
        s.executeUpdate(
            String.format(
                "INSERT INTO poc_db.orders (customer_id, amount, status) VALUES (%d, %.2f, '%s')",
                stamp, 50.00 + i, burst));
      }
    }

    // All burst rows carry the same customer_id (stamp); count distinct emissions for it.
    int seen =
        countMatching(TOPIC, Duration.ofSeconds(90), m -> burst.equals(statusOf(m)), burstCount);
    assertThat(seen)
        .as("all " + burstCount + " burst rows reached Kafka")
        .isGreaterThanOrEqualTo(burstCount);

    // Insert after the burst to verify continued operation.
    try (Connection c = flinkConn();
        Statement s = c.createStatement()) {
      s.executeUpdate(
          String.format(
              "INSERT INTO poc_db.orders (customer_id, amount, status) VALUES (%d, 99.99, '%s')",
              stamp, afterBurst));
    }

    assertThat(waitForKafkaMessage(TOPIC, Duration.ofSeconds(45), m -> m.contains(afterBurst)))
        .as("expected AFTER-BURST event for stamp=" + stamp)
        .isNotNull();

    log.info("Job kept producing after a {}-row burst (stamp={})", burstCount, stamp);
  }

  @Test
  @Timeout(120)
  void largePayload_isProcessedSuccessfully() throws Exception {
    long stamp = uniqueId();
    // ~506 bytes: 6-char prefix + stamp + dash + 500 'X'.
    String largeStatus = "LARGE-" + stamp + "-" + "X".repeat(500);
    try (Connection c = flinkConn();
        Statement s = c.createStatement()) {
      s.executeUpdate(
          String.format(
              "INSERT INTO poc_db.orders (customer_id, amount, status) VALUES (%d, 77.77, '%s')",
              stamp, largeStatus));
    }

    ensureJobRunning(JAR, "poc.datastream.DataStreamCdcJob", JOB_NAME, Duration.ofSeconds(30));
    String msg = waitForKafkaMessage(TOPIC, Duration.ofSeconds(45), m -> m.contains(largeStatus));

    assertThat(msg).as("CDC message containing large status payload").isNotNull();
    assertThat(msg).contains(largeStatus);

    log.info("Large payload test passed: processed ~{} byte status", largeStatus.length());
  }

  @Test
  @Timeout(120)
  void specialCharactersInData_arePreserved() throws Exception {
    long stamp = uniqueId();
    String quotes = "QUOTES-" + stamp;
    String specialStatus = "TEST-WITH-\"" + quotes + "\"-AND-'APOSTROPHES'";
    try (Connection c = flinkConn();
        Statement s = c.createStatement()) {
      s.executeUpdate(
          String.format(
              "INSERT INTO poc_db.orders (customer_id, amount, status) VALUES (%d, 44.44, '%s')",
              stamp, specialStatus.replace("'", "\\'")));
    }

    ensureJobRunning(JAR, "poc.datastream.DataStreamCdcJob", JOB_NAME, Duration.ofSeconds(30));
    String msg = waitForKafkaMessage(TOPIC, Duration.ofSeconds(45), m -> m.contains(quotes));

    assertThat(msg).as("CDC message containing special characters").isNotNull();
    // The embedded double-quotes must survive into the after.status field intact.
    JSONObject after = afterOf(msg);
    assertThat(after).as("after object present").isNotNull();
    assertThat(after.optString("status")).isEqualTo(specialStatus);

    log.info("Special character handling verified (stamp={})", stamp);
  }

  @Test
  @Timeout(120)
  void consecutiveSnapshots_remainConsistent() throws Exception {
    long stamp = uniqueId();
    String snap1Marker = "SNAP1-" + stamp;
    String snap2Marker = "SNAP2-" + stamp;
    try (Connection c = flinkConn();
        Statement s = c.createStatement()) {
      s.executeUpdate(
          String.format(
              "INSERT INTO poc_db.orders (customer_id, amount, status) VALUES (%d, 11.11, '%s')",
              stamp, snap1Marker));
    }

    ensureJobRunning(JAR, "poc.datastream.DataStreamCdcJob", JOB_NAME, Duration.ofSeconds(30));
    String snap1 = waitForKafkaMessage(TOPIC, Duration.ofSeconds(45), m -> m.contains(snap1Marker));

    try (Connection c = flinkConn();
        Statement s = c.createStatement()) {
      s.executeUpdate(
          String.format(
              "INSERT INTO poc_db.orders (customer_id, amount, status) VALUES (%d, 22.22, '%s')",
              stamp, snap2Marker));
    }

    String snap2 = waitForKafkaMessage(TOPIC, Duration.ofSeconds(45), m -> m.contains(snap2Marker));

    assertThat(snap1).as("CDC message containing " + snap1Marker).isNotNull();
    assertThat(snap2).as("CDC message containing " + snap2Marker).isNotNull();

    log.info("Snapshot consistency verified: both binlog inserts produced Kafka messages");
  }

  private static String statusOf(String msg) {
    JSONObject a = afterOf(msg);
    return a == null ? null : a.optString("status", null);
  }

  /**
   * Counts messages matching {@code filter} on {@code topic} from earliest, returning as soon as
   * {@code minCount} matches have been seen or the timeout expires. Unlike {@link #pollKafka}
   * (which stops at the first {@code minCount} messages regardless of content), this keeps scanning
   * until it has counted the target number of <em>matching</em> messages.
   */
  private static int countMatching(
      String topic, Duration timeout, Predicate<String> filter, int minCount) {
    Properties props = new Properties();
    props.put("bootstrap.servers", KAFKA_BOOTSTRAP);
    props.put("group.id", "err-" + UUID.randomUUID());
    props.put("auto.offset.reset", "earliest");
    props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
    props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

    int count = 0;
    long deadline = System.currentTimeMillis() + timeout.toMillis();
    try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
      consumer.subscribe(List.of(topic));
      while (count < minCount && System.currentTimeMillis() < deadline) {
        var records = consumer.poll(Duration.ofMillis(500));
        for (var record : records) {
          if (filter.test(record.value())) {
            count++;
          }
        }
      }
    }
    return count;
  }
}
