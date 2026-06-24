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
 * Component test for SQL API CDC variant (StatementSet — single JobGraph for two tables).
 *
 * <p>Submits the fat-jar to the Flink JobManager container (localhost:8081) via REST. Server-IDs
 * 5800-5849 (orders) and 5850-5899 (customers) are hardcoded in SqlApiCdcJob DDL.
 *
 * <p>Output shape (upsert-kafka, plain row JSON): orders → {@code {id, customer_id, amount, status,
 * created_at, job_variant:"sql-api"}} on {@code poc.flink.sql-api.orders}; customers → {@code {id,
 * name, email, job_variant:"sql-api"}} on {@code poc.flink.sql-api.customers}. Both halves of the
 * StatementSet are asserted here (previously only the orders sink was checked, leaving the
 * customers INSERT untested).
 */
@Slf4j
@DisplayName("Flink SQL API : CDC Test")
class SqlApiCdcTest extends FlinkTestBase {

  private static final Path JAR = jarPath("variant-flink-sql-api-cdc-job");
  private static final String ORDERS_TOPIC = "poc.flink.sql-api.orders";
  private static final String CUSTOMERS_TOPIC = "poc.flink.sql-api.customers";

  private static JSONObject rowOf(String msg) {
    try {
      return new JSONObject(msg);
    } catch (Exception e) {
      return null;
    }
  }

  @Test
  @Timeout(120)
  void sqlApiPipeline_capturesBothTables_andPublishesValidRowsToSeparateKafkaTopics()
      throws Exception {
    long cid = uniqueId();
    String orderMarker = "SQL-SNAP-" + cid;
    String customerName = "SqlCust-" + cid;
    try (Connection c = flinkConn();
        Statement s = c.createStatement()) {
      s.executeUpdate(
          String.format(
              "INSERT INTO poc_db.orders (customer_id, amount, status) VALUES (%d, 33.33, '%s')",
              cid, orderMarker));
      s.executeUpdate(
          String.format(
              "INSERT INTO poc_db.customers (name, email) VALUES ('%s', 'sql-%d@example.com')",
              customerName, cid));
    }

    ensureJobRunning(
        JAR, "poc.sqlapi.SqlApiCdcJob", "Flink Sql API CDC Job", Duration.ofSeconds(30));

    // ── orders sink ───────────────────────────────────────────────────────
    String orderMsg =
        waitForKafkaMessage(
            ORDERS_TOPIC,
            Duration.ofSeconds(60),
            m -> {
              JSONObject r = rowOf(m);
              return r != null
                  && r.optLong("customer_id") == cid
                  && orderMarker.equals(r.optString("status"));
            });
    assertThat(orderMsg).as("expected orders row for customer_id=" + cid).isNotNull();
    JSONObject orderRow = rowOf(orderMsg);
    assertThat(orderRow.has("id")).isTrue();
    assertThat(orderRow.has("amount")).isTrue();
    assertThat(orderRow.optLong("customer_id")).isEqualTo(cid);
    assertThat(orderRow.optString("status")).isEqualTo(orderMarker);
    assertThat(orderRow.optString("job_variant")).isEqualTo("sql-api");

    // ── customers sink (previously untested — half the StatementSet) ──────
    String customerMsg =
        waitForKafkaMessage(
            CUSTOMERS_TOPIC,
            Duration.ofSeconds(60),
            m -> {
              JSONObject r = rowOf(m);
              return r != null && customerName.equals(r.optString("name"));
            });
    assertThat(customerMsg).as("expected customers row for name=" + customerName).isNotNull();
    JSONObject customerRow = rowOf(customerMsg);
    assertThat(customerRow.has("id")).isTrue();
    assertThat(customerRow.optString("name")).isEqualTo(customerName);
    assertThat(customerRow.optString("email")).isEqualTo("sql-" + cid + "@example.com");
    assertThat(customerRow.optString("job_variant")).isEqualTo("sql-api");

    log.info(
        "SQL API CDC: validated both StatementSet sinks — orders cid={} and customers name={}",
        cid,
        customerName);
  }

  @Test
  @Timeout(90)
  void sqlApiPipeline_capturesBinlogInsert_afterSnapshotComplete() throws Exception {
    ensureJobRunning(
        JAR, "poc.sqlapi.SqlApiCdcJob", "Flink Sql API CDC Job", Duration.ofSeconds(30));

    long cid = uniqueId();
    String marker = "SQL-BINLOG-" + cid;
    try (Connection c = flinkConn();
        Statement s = c.createStatement()) {
      s.executeUpdate(
          String.format(
              "INSERT INTO poc_db.orders (customer_id, amount, status) VALUES (%d, 44.44, '%s')",
              cid, marker));
    }

    String msg =
        waitForKafkaMessage(
            ORDERS_TOPIC,
            Duration.ofSeconds(60),
            m -> {
              JSONObject r = rowOf(m);
              return r != null
                  && r.optLong("customer_id") == cid
                  && marker.equals(r.optString("status"));
            });
    assertThat(msg).as("expected binlog orders row for customer_id=" + cid).isNotNull();
    assertThat(rowOf(msg).optString("status")).isEqualTo(marker);
    log.info("SQL API CDC binlog: validated orders row for customer_id={}", cid);
  }
}
