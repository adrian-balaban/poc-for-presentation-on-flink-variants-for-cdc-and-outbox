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
  private static final String TOPIC = "poc.cdc.datastream";

  @Test
  @Timeout(90)
  void cdcSource_capturesSnapshotRow_andPublishesEnrichedEventToKafka() throws Exception {
    try (Connection c = flinkConn();
        Statement s = c.createStatement()) {
      s.executeUpdate(
          "INSERT INTO poc_db.orders (customer_id, amount, status) VALUES (1, 11.11, 'DS-TEST')");
    }

    ensureJobRunning(JAR, "poc.datastream.DataStreamCdcJob", JOB_NAME, Duration.ofSeconds(120));
    List<String> messages = pollKafka(TOPIC, 1, Duration.ofSeconds(45));
    assertThat(messages).isNotEmpty();
    assertThat(messages).anyMatch(m -> m.contains("DS-TEST"));
    log.info("DataStream CDC: {} Kafka message(s) received", messages.size());
  }

  @Test
  @Timeout(90)
  void cdcSource_capturesBinlogInsert_afterSnapshotComplete() throws Exception {
    ensureJobRunning(JAR, "poc.datastream.DataStreamCdcJob", JOB_NAME, Duration.ofSeconds(120));

    try (Connection c = flinkConn();
        Statement s = c.createStatement()) {
      s.executeUpdate(
          "INSERT INTO poc_db.orders (customer_id, amount, status) VALUES (2, 22.22, 'BINLOG-TEST')");
    }

    List<String> messages = pollKafka(TOPIC, 1, Duration.ofSeconds(45));
    assertThat(messages).isNotEmpty();
    assertThat(messages).anyMatch(m -> m.contains("BINLOG-TEST"));
    log.info("DataStream CDC binlog test: {} Kafka message(s) received", messages.size());
  }
}
