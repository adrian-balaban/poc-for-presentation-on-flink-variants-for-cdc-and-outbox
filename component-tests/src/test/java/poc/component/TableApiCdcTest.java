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
 * Component test for Table API CDC variant.
 *
 * <p>Submits the fat-jar to the Flink JobManager container (localhost:8081) via REST, inserts a
 * test row, and verifies the CDC event reaches the Kafka upsert topic. Job remains running.
 * Server-ID 6000-6099 is hardcoded in the DDL inside TableApiCdcJob.
 */
@Slf4j
@DisplayName("Flink Table API : CDC Test")
class TableApiCdcTest extends FlinkTestBase {

  private static final Path JAR = jarPath("variant-flink-table-api-cdc-job");
  private static final String TOPIC = "poc.cdc.table-api.flink";

  @Test
  @Timeout(90)
  void tableApiPipeline_capturesOrder_andPublishesToKafka() throws Exception {
    try (Connection c = flinkConn();
        Statement s = c.createStatement()) {
      s.executeUpdate(
          "INSERT INTO poc_db.orders (customer_id, amount, status) VALUES (1, 55.55, 'TA-TEST')");
    }

    ensureJobRunning(
        JAR, "poc.tableapi.TableApiCdcJob", "Flink Table API CDC Job", Duration.ofSeconds(30));
    List<String> messages = pollKafka(TOPIC, 1, Duration.ofSeconds(45));
    assertThat(messages).isNotEmpty();
    assertThat(messages).anyMatch(m -> m.contains("TA-TEST"));
    log.info("Table API CDC: {} Kafka message(s) received", messages.size());
  }
}
