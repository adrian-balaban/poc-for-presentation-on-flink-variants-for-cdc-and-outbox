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
 * Component test for Outbox variant.
 *
 * <p>Submits the fat-jar to the Flink JobManager container (localhost:8081) via REST and verifies
 * outbox events reach Kafka routed by destination. Job remains running after the test.
 */
@Slf4j
@DisplayName("Flink DataStream API v.1 : Outbox Test")
class DataStreamOutboxTest extends FlinkTestBase {

  private static final Path JAR = jarPath("variant-flink-datastream-api-v1-outbox-job");
  private static final String TOPIC = "poc.cdc.outbox.flink";

  @Test
  @Timeout(90)
  void outboxSource_capturesOutboxEvents_andPublishesToKafka() throws Exception {
    try (Connection c = flinkConn();
        Statement s = c.createStatement()) {
      s.executeUpdate(
          "INSERT INTO poc_db.outbox_events (destination, payload) VALUES ('payments', '{\"order_id\":1}')");
    }

    ensureJobRunning(
        JAR, "poc.outbox.OutboxJob", "Flink DataStream API v.1 Outbox Job", Duration.ofSeconds(30));
    // outbox_events is seeded with payments/notifications/audit rows and the
    // test inserts another payments row, so the snapshot emits several events.
    List<String> messages = pollKafka(TOPIC, 4, Duration.ofSeconds(45));
    assertThat(messages).isNotEmpty();
    assertThat(messages).anyMatch(m -> m.contains("payments"));
    log.info("Outbox CDC: {} Kafka message(s) received", messages.size());
  }
}
