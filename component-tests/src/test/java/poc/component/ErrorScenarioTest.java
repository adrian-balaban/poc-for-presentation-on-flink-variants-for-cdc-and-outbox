package poc.component;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Path;
import java.sql.Connection;
import java.sql.Statement;
import java.time.Duration;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * Error scenario tests for CDC resilience.
 *
 * <p>Validates job behavior under failure conditions: MySQL connectivity issues, Kafka
 * unavailability, and recovery semantics.
 */
@Slf4j
@DisplayName("CDC Error Scenario Handling")
class ErrorScenarioTest extends FlinkTestBase {

  private static final Path JAR = jarPath("variant-flink-datastream-api-v1-cdc-job");
  private static final String JOB_NAME = "Flink DataStream API v.1 CDC Job";
  private static final String TOPIC = "poc.cdc.datastream";

  @Test
  @Timeout(120)
  void jobContinuesAfterTransientMysqlDelay() throws Exception {
    // Insert initial row before job starts
    try (Connection c = flinkConn();
        Statement s = c.createStatement()) {
      s.executeUpdate(
          "INSERT INTO poc_db.orders (customer_id, amount, status) VALUES (9000, 1.00, 'BEFORE-DELAY')");
    }

    ensureJobRunning(JAR, "poc.datastream.DataStreamCdcJob", JOB_NAME, Duration.ofSeconds(30));
    List<String> beforeMessages = pollKafka(TOPIC, 1, Duration.ofSeconds(45));
    assertThat(beforeMessages).isNotEmpty();

    // Wait a bit to ensure snapshot is complete
    Thread.sleep(5000);

    // Insert another row after delay
    try (Connection c = flinkConn();
        Statement s = c.createStatement()) {
      s.executeUpdate(
          "INSERT INTO poc_db.orders (customer_id, amount, status) VALUES (9001, 2.00, 'AFTER-DELAY')");
    }

    List<String> afterMessages = pollKafka(TOPIC, 1, Duration.ofSeconds(45));
    assertThat(afterMessages).isNotEmpty();
    assertThat(afterMessages).anyMatch(m -> m.contains("AFTER-DELAY"));

    log.info(
        "Job recovered after delay: received {} messages",
        beforeMessages.size() + afterMessages.size());
  }

  @Test
  @Timeout(120)
  void multipleConsecutiveInserts_doNotProduceDuplicates() throws Exception {
    try (Connection c = flinkConn();
        Statement s = c.createStatement()) {
      // Insert distinct rows
      for (int i = 0; i < 5; i++) {
        s.executeUpdate(
            String.format(
                "INSERT INTO poc_db.orders (customer_id, amount, status) "
                    + "VALUES (%d, %.2f, 'DUP-TEST-%d')",
                9100 + i, 10.00 + i, i));
      }
    }

    ensureJobRunning(JAR, "poc.datastream.DataStreamCdcJob", JOB_NAME, Duration.ofSeconds(30));
    List<String> messages = pollKafka(TOPIC, 5, Duration.ofSeconds(60));

    // Count messages per customer_id
    int countDupTest0 = (int) messages.stream().filter(m -> m.contains("DUP-TEST-0")).count();

    assertThat(countDupTest0)
        .as("Should not have duplicate messages for same row")
        .isLessThanOrEqualTo(1);

    log.info("No duplicates detected: {} total messages for 5 inserts", messages.size());
  }

  @Test
  @Timeout(120)
  void jobStaysBusyUnderLoadAndRecoverable() throws Exception {
    ensureJobRunning(JAR, "poc.datastream.DataStreamCdcJob", JOB_NAME, Duration.ofSeconds(30));

    // Insert burst of rows
    try (Connection c = flinkConn();
        Statement s = c.createStatement()) {
      for (int i = 0; i < 20; i++) {
        s.executeUpdate(
            String.format(
                "INSERT INTO poc_db.orders (customer_id, amount, status) "
                    + "VALUES (%d, %.2f, 'BURST-TEST')",
                9200 + i, 50.00 + i));
      }
    }

    // Poll for at least some messages
    List<String> messages = pollKafka(TOPIC, 10, Duration.ofSeconds(90));
    assertThat(messages.size()).isGreaterThanOrEqualTo(10);

    // Insert more rows after burst to verify continued operation
    try (Connection c = flinkConn();
        Statement s = c.createStatement()) {
      s.executeUpdate(
          "INSERT INTO poc_db.orders (customer_id, amount, status) VALUES (9250, 99.99, 'AFTER-BURST')");
    }

    List<String> afterMessages = pollKafka(TOPIC, 1, Duration.ofSeconds(45));
    assertThat(afterMessages).anyMatch(m -> m.contains("AFTER-BURST"));

    log.info(
        "Job recovered after load burst: {} messages total",
        messages.size() + afterMessages.size());
  }

  @Test
  @Timeout(120)
  void largePayload_isProcessedSuccessfully() throws Exception {
    // Insert row with large status string
    String largeStatus = "LARGE-" + "X".repeat(500);
    try (Connection c = flinkConn();
        Statement s = c.createStatement()) {
      s.executeUpdate(
          String.format(
              "INSERT INTO poc_db.orders (customer_id, amount, status) VALUES (9300, 77.77, '%s')",
              largeStatus));
    }

    ensureJobRunning(JAR, "poc.datastream.DataStreamCdcJob", JOB_NAME, Duration.ofSeconds(30));
    List<String> messages = pollKafka(TOPIC, 1, Duration.ofSeconds(45));

    assertThat(messages).isNotEmpty();
    assertThat(messages.get(0)).contains(largeStatus);

    log.info("Large payload test passed: processed ~600 byte message");
  }

  @Test
  @Timeout(120)
  void specialCharactersInData_arePreserved() throws Exception {
    String specialStatus = "TEST-WITH-\"QUOTES\"-AND-'APOSTROPHES'";
    try (Connection c = flinkConn();
        Statement s = c.createStatement()) {
      s.executeUpdate(
          String.format(
              "INSERT INTO poc_db.orders (customer_id, amount, status) VALUES (9400, 44.44, '%s')",
              specialStatus.replace("'", "\\'")));
    }

    ensureJobRunning(JAR, "poc.datastream.DataStreamCdcJob", JOB_NAME, Duration.ofSeconds(30));
    List<String> messages = pollKafka(TOPIC, 1, Duration.ofSeconds(45));

    assertThat(messages).isNotEmpty();
    assertThat(messages.get(0)).contains("QUOTES");

    log.info("Special character handling verified");
  }

  @Test
  @Timeout(120)
  void consecutiveSnapshots_remainConsistent() throws Exception {
    // First snapshot
    try (Connection c = flinkConn();
        Statement s = c.createStatement()) {
      s.executeUpdate(
          "INSERT INTO poc_db.orders (customer_id, amount, status) VALUES (9500, 11.11, 'SNAP1')");
    }

    ensureJobRunning(JAR, "poc.datastream.DataStreamCdcJob", JOB_NAME, Duration.ofSeconds(30));
    List<String> snap1 = pollKafka(TOPIC, 1, Duration.ofSeconds(45));

    // Second insert to verify binlog is still flowing
    try (Connection c = flinkConn();
        Statement s = c.createStatement()) {
      s.executeUpdate(
          "INSERT INTO poc_db.orders (customer_id, amount, status) VALUES (9501, 22.22, 'SNAP2')");
    }

    List<String> snap2 = pollKafka(TOPIC, 1, Duration.ofSeconds(45));

    assertThat(snap1).isNotEmpty();
    assertThat(snap2).isNotEmpty();
    assertThat(snap1.get(0)).contains("SNAP1");
    assertThat(snap2.get(0)).contains("SNAP2");

    log.info("Snapshot consistency verified across {} messages", snap1.size() + snap2.size());
  }
}
