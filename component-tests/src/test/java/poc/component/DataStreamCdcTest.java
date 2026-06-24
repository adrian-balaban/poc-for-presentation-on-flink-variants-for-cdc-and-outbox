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
 * Component test for DataStream CDC variant.
 *
 * <p>Submits the fat-jar to the Flink JobManager container (localhost:8081) via REST and verifies
 * CDC events reach Kafka. Jobs remain running after the test. Server-ID range: uses JobConfig
 * default (5900-5999) from the flink-jm container env.
 */
@Slf4j
@DisplayName("Flink DataStream API v.1 : CDC Test")
class DataStreamCdcTest extends FlinkTestBase {

  private static final Path JAR = jarPath("variant-flink-datastream-api-v1-cdc-job");
  private static final String JOB_NAME = "Flink DataStream API v.1 CDC Job";
  private static final String TOPIC = "poc.flink.datastream.orders";

  @Test
  @Timeout(90)
  void cdcSource_capturesSnapshotRow_andPublishesEnrichedEventToKafka() throws Exception {
    // Unique marker so the predicate selects this run's row, not a retained row from a prior run
    // on the shared, never-truncated topic.
    String marker = "DS-TEST-" + uniqueId();
    try (Connection c = flinkConn();
        Statement s = c.createStatement()) {
      s.executeUpdate(
          String.format(
              "INSERT INTO poc_db.orders (customer_id, amount, status) VALUES (1, 11.11, '%s')",
              marker));
    }

    ensureJobRunning(JAR, "poc.datastream.DataStreamCdcJob", JOB_NAME, Duration.ofSeconds(120));
    String msg = waitForKafkaMessage(TOPIC, Duration.ofSeconds(45), m -> m.contains(marker));
    assertThat(msg).as("expected CDC event containing " + marker).isNotNull();
    log.info("DataStream CDC: received Kafka message for {}", marker);
  }

  @Test
  @Timeout(90)
  void cdcSource_capturesBinlogInsert_afterSnapshotComplete() throws Exception {
    ensureJobRunning(JAR, "poc.datastream.DataStreamCdcJob", JOB_NAME, Duration.ofSeconds(120));

    String marker = "BINLOG-TEST-" + uniqueId();
    try (Connection c = flinkConn();
        Statement s = c.createStatement()) {
      s.executeUpdate(
          String.format(
              "INSERT INTO poc_db.orders (customer_id, amount, status) VALUES (2, 22.22, '%s')",
              marker));
    }

    String msg = waitForKafkaMessage(TOPIC, Duration.ofSeconds(45), m -> m.contains(marker));
    assertThat(msg).as("expected binlog CDC event containing " + marker).isNotNull();
    log.info("DataStream CDC binlog test: received Kafka message for {}", marker);
  }

  /**
   * Verifies the multi-table pass-through: {@code DataStreamCdcJob} reads {@code
   * orders,customers,outbox_events} (the {@code MYSQL_TABLES} default) and {@code CdcEventRouter}
   * is a pass-through that tags every event as {@code datastream-cdc} to the single {@code
   * poc.flink.datastream.orders} topic. A {@code customers} insert must therefore appear on that
   * topic with the customer row fields in the Debezium {@code after} object — previously untested,
   * so a regression that dropped non-orders tables would be invisible.
   */
  @Test
  @Timeout(90)
  void cdcSource_capturesCustomersTable_andPublishesToOrdersTopic() throws Exception {
    ensureJobRunning(JAR, "poc.datastream.DataStreamCdcJob", JOB_NAME, Duration.ofSeconds(120));

    String name = "CustPass-" + uniqueId();
    try (Connection c = flinkConn();
        Statement s = c.createStatement()) {
      s.executeUpdate(
          String.format(
              "INSERT INTO poc_db.customers (name, email) VALUES ('%s', '%s@example.com')",
              name, name.toLowerCase()));
    }

    String msg =
        waitForKafkaMessage(
            TOPIC,
            Duration.ofSeconds(60),
            m -> {
              try {
                JSONObject a = new JSONObject(m).optJSONObject("after");
                return a != null && name.equals(a.optString("name"));
              } catch (Exception e) {
                return false;
              }
            });
    assertThat(msg)
        .as("expected customers CDC event on the orders topic for name=" + name)
        .isNotNull();
    JSONObject after = new JSONObject(msg).getJSONObject("after");
    assertThat(after.optString("name")).isEqualTo(name);
    assertThat(new JSONObject(msg).optString("variant")).isEqualTo("datastream-cdc");
    log.info("DataStream CDC customers pass-through: validated event for name={}", name);
  }
}
