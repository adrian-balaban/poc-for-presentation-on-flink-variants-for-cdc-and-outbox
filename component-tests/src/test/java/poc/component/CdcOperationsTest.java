package poc.component;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Path;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.time.Duration;
import lombok.extern.slf4j.Slf4j;
import org.json.JSONObject;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * Verifies the DataStream CDC variant captures all three mutation types — INSERT, UPDATE, DELETE —
 * not just inserts.
 *
 * <p>The DataStream job uses the Debezium {@code JsonDebeziumDeserializationSchema} on an
 * <em>append </em> {@code KafkaSink}, so every mutation is emitted as a separate Debezium envelope
 * on {@code poc.flink.datastream.orders} with an {@code op} field: {@code "c"} (create), {@code
 * "u"} (update, both {@code before} and {@code after}), {@code "d"} (delete, {@code before} only,
 * {@code after} null). This test asserts each op for the same row id.
 *
 * <p>Sink-semantics note: the Table API and SQL API variants use {@code upsert-kafka} (keyed by
 * id), where UPDATE overwrites the value for the key and DELETE writes a tombstone (null value) — a
 * different shape from the append op-envelope asserted here. Those variants are therefore out of
 * scope for this test; the DataStream append path is where op-field semantics are cleanest.
 */
@Slf4j
@DisplayName("CDC Mutation Operations (INSERT/UPDATE/DELETE)")
class CdcOperationsTest extends FlinkTestBase {

  private static final Path JAR = jarPath("variant-flink-datastream-api-v1-cdc-job");
  private static final String JOB_NAME = "Flink DataStream API v.1 CDC Job";
  private static final String TOPIC = "poc.flink.datastream.orders";

  private static String opOf(String msg) {
    try {
      return new JSONObject(msg).optString("op");
    } catch (Exception e) {
      return null;
    }
  }

  private static long idFromAfter(String msg) {
    try {
      JSONObject a = new JSONObject(msg).optJSONObject("after");
      return a == null ? -1 : a.optLong("id");
    } catch (Exception e) {
      return -1;
    }
  }

  private static long idFromBefore(String msg) {
    try {
      JSONObject b = new JSONObject(msg).optJSONObject("before");
      return b == null ? -1 : b.optLong("id");
    } catch (Exception e) {
      return -1;
    }
  }

  @Test
  @Timeout(300)
  void dataStreamJob_capturesInsertUpdateAndDeleteOps() throws Exception {
    ensureJobRunning(JAR, "poc.datastream.DataStreamCdcJob", JOB_NAME, Duration.ofSeconds(120));

    // Wait for at least one snapshot message before inserting. If the job was freshly submitted it
    // needs to complete the snapshot phase before binlog events flow; without this warm-up the 60 s
    // poll window for the INSERT op=c can expire before the binlog event arrives.
    pollKafka(TOPIC, 1, Duration.ofSeconds(90));

    long stamp = uniqueId();
    String inserted = "OP-INSERT-" + stamp;
    String updated = "OP-UPDATE-" + stamp;
    long rowId;

    // ── INSERT ─────────────────────────────────────────────────────────────
    try (Connection c = flinkConn();
        Statement s = c.createStatement()) {
      s.executeUpdate(
          String.format(
              "INSERT INTO poc_db.orders (customer_id, amount, status) VALUES (%d, 1.01, '%s')",
              stamp, inserted),
          Statement.RETURN_GENERATED_KEYS);
      try (ResultSet keys = s.getGeneratedKeys()) {
        assertThat(keys.next()).as("got generated id for inserted row").isTrue();
        rowId = keys.getLong(1);
      }
    }

    String insertMsg =
        waitForKafkaMessage(
            TOPIC, Duration.ofSeconds(90), m -> "c".equals(opOf(m)) && idFromAfter(m) == rowId);
    assertThat(insertMsg).as("expected INSERT (op=c) event for id=" + rowId).isNotNull();
    assertThat(new JSONObject(insertMsg).getJSONObject("after").optString("status"))
        .isEqualTo(inserted);

    // ── UPDATE ─────────────────────────────────────────────────────────────
    try (Connection c = flinkConn();
        Statement s = c.createStatement()) {
      assertThat(
              s.executeUpdate(
                  String.format(
                      "UPDATE poc_db.orders SET status='%s' WHERE id=%d", updated, rowId)))
          .as("update affected exactly one row")
          .isEqualTo(1);
    }

    String updateMsg =
        waitForKafkaMessage(
            TOPIC, Duration.ofSeconds(60), m -> "u".equals(opOf(m)) && idFromAfter(m) == rowId);
    assertThat(updateMsg).as("expected UPDATE (op=u) event for id=" + rowId).isNotNull();
    JSONObject updateAfter = new JSONObject(updateMsg).getJSONObject("after");
    assertThat(updateAfter.optLong("id")).isEqualTo(rowId);
    assertThat(updateAfter.optString("status")).isEqualTo(updated);

    // ── DELETE ─────────────────────────────────────────────────────────────
    try (Connection c = flinkConn();
        Statement s = c.createStatement()) {
      assertThat(s.executeUpdate(String.format("DELETE FROM poc_db.orders WHERE id=%d", rowId)))
          .as("delete affected exactly one row")
          .isEqualTo(1);
    }

    String deleteMsg =
        waitForKafkaMessage(
            TOPIC, Duration.ofSeconds(60), m -> "d".equals(opOf(m)) && idFromBefore(m) == rowId);
    assertThat(deleteMsg).as("expected DELETE (op=d) event for id=" + rowId).isNotNull();
    JSONObject deleteJson = new JSONObject(deleteMsg);
    assertThat(deleteJson.optJSONObject("after")).as("delete event after should be null").isNull();
    assertThat(deleteJson.getJSONObject("before").optLong("id")).isEqualTo(rowId);

    log.info("CDC ops verified for id={}: INSERT(op=c), UPDATE(op=u), DELETE(op=d)", rowId);
  }
}
