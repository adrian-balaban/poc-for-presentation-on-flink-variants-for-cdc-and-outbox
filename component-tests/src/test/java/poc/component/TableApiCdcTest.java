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
 * Component test for Table API CDC variant.
 *
 * <p>Submits the fat-jar to the Flink JobManager container (localhost:8081) via REST, inserts a
 * test row, and verifies the CDC event reaches the Kafka upsert topic. Job remains running.
 * Server-ID 6000-6099 is hardcoded in the DDL inside TableApiCdcJob.
 *
 * <p>Output shape (upsert-kafka, plain row JSON — NOT a Debezium envelope): {@code {id,
 * customer_id, amount, status, created_at, job_variant:"table-api"}}. Enrichment contract for this
 * variant: the variant name is carried in the {@code job_variant} field, not {@code variant} (which
 * only the DataStream router emits). This test pins that contract.
 */
@Slf4j
@DisplayName("Flink Table API : CDC Test")
class TableApiCdcTest extends FlinkTestBase {

  private static final Path JAR = jarPath("variant-flink-table-api-cdc-job");
  private static final String TOPIC = "poc.flink.table-api.orders";

  /**
   * Parse an upsert-kafka row JSON defensively — tombstone values (null) for other keys can appear
   * in the topic and would otherwise throw during predicate evaluation.
   */
  private static JSONObject rowOf(String msg) {
    try {
      return new JSONObject(msg);
    } catch (Exception e) {
      return null;
    }
  }

  @Test
  @Timeout(90)
  void tableApiPipeline_capturesOrderSnapshot_andPublishesValidRowToKafka() throws Exception {
    // Unique customer_id so the predicate reliably selects this test's row, not a stale snapshot
    // row from another test on the same topic (jobs are never cancelled between tests).
    long cid = System.currentTimeMillis() % 1_000_000;
    String marker = "TA-SNAP-" + cid;
    try (Connection c = flinkConn();
        Statement s = c.createStatement()) {
      s.executeUpdate(
          String.format(
              "INSERT INTO poc_db.orders (customer_id, amount, status) VALUES (%d, 55.55, '%s')",
              cid, marker));
    }

    ensureJobRunning(
        JAR, "poc.tableapi.TableApiCdcJob", "Flink Table API CDC Job", Duration.ofSeconds(30));

    String msg =
        waitForKafkaMessage(
            TOPIC,
            Duration.ofSeconds(60),
            m -> {
              JSONObject r = rowOf(m);
              return r != null
                  && r.optLong("customer_id") == cid
                  && marker.equals(r.optString("status"));
            });
    assertThat(msg).as("expected upsert-kafka row for customer_id=" + cid).isNotNull();

    JSONObject row = rowOf(msg);
    assertThat(row.has("id")).as("id field present").isTrue();
    assertThat(row.has("customer_id")).isTrue();
    assertThat(row.has("amount")).isTrue();
    assertThat(row.has("status")).isTrue();
    assertThat(row.optLong("customer_id")).isEqualTo(cid);
    assertThat(row.optString("status")).isEqualTo(marker);
    // Enrichment contract: Table API tags the variant via job_variant, not variant.
    assertThat(row.optString("job_variant"))
        .as("job_variant enrichment field")
        .isEqualTo("table-api");
    log.info("Table API CDC snapshot: validated row JSON for customer_id={}", cid);
  }

  @Test
  @Timeout(90)
  void tableApiPipeline_capturesBinlogInsert_afterSnapshotComplete() throws Exception {
    ensureJobRunning(
        JAR, "poc.tableapi.TableApiCdcJob", "Flink Table API CDC Job", Duration.ofSeconds(30));

    long cid = System.currentTimeMillis() % 1_000_000;
    String marker = "TA-BINLOG-" + cid;
    try (Connection c = flinkConn();
        Statement s = c.createStatement()) {
      s.executeUpdate(
          String.format(
              "INSERT INTO poc_db.orders (customer_id, amount, status) VALUES (%d, 66.66, '%s')",
              cid, marker));
    }

    String msg =
        waitForKafkaMessage(
            TOPIC,
            Duration.ofSeconds(60),
            m -> {
              JSONObject r = rowOf(m);
              return r != null
                  && r.optLong("customer_id") == cid
                  && marker.equals(r.optString("status"));
            });
    assertThat(msg).as("expected binlog CDC row for customer_id=" + cid).isNotNull();
    assertThat(rowOf(msg).optString("status")).isEqualTo(marker);
    log.info("Table API CDC binlog: validated row JSON for customer_id={}", cid);
  }
}
