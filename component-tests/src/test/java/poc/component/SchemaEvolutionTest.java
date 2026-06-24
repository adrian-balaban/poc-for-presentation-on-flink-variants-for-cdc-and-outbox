package poc.component;

import static org.assertj.core.api.Assertions.assertThat;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.time.Duration;
import lombok.extern.slf4j.Slf4j;
import org.json.JSONObject;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * Schema-evolution coverage for the Kafka Connect Debezium path — previously untested, so an ALTER
 * TABLE that Debezium failed to propagate would be invisible.
 *
 * <p>Uses a <em>dedicated</em> table {@code poc_db.schema_evo} and a dedicated KC connector
 * (server-id 5560, topic prefix {@code poc.kc.schema-evo}) so the test does not perturb the shared
 * {@code orders} table that all 10 Flink+KC connectors read. The {@code flink} DB user lacks
 * CREATE/ALTER/DROP privileges (init.sql grants only SELECT + LOCK TABLES), so DDL is issued via a
 * root connection; data inserts use the normal {@code flinkConn()}.
 *
 * <p>Flow: create the table fresh → deploy connector (initial snapshot of empty table) → insert a
 * pre-ALTER row and assert it carries no {@code new_field} → {@code ALTER TABLE ADD COLUMN
 * new_field} → insert a post-ALTER row and assert the Debezium {@code after} now contains {@code
 * new_field} with the inserted value. This proves Debezium's schema history evolves the row schema
 * across a binlog DDL event.
 */
@Slf4j
@DisplayName("Schema Evolution (Kafka Connect Debezium)")
class SchemaEvolutionTest extends KafkaConnectBase {

  private static final String CONNECTOR_NAME = "kc-schema-evo-cdc";
  private static final String TABLE = "poc_db.schema_evo";
  private static final String TOPIC = "poc.kc.schema-evo.schema_evo";

  private static Connection rootConn() throws Exception {
    String host = System.getenv().getOrDefault("MYSQL_HOST", "localhost");
    int port = Integer.parseInt(System.getenv().getOrDefault("MYSQL_PORT", "3306"));
    String user = System.getenv().getOrDefault("MYSQL_ROOT_USER", "root");
    String password = System.getenv().getOrDefault("MYSQL_ROOT_PASSWORD", "root");
    return DriverManager.getConnection(
        String.format(
            "jdbc:mysql://%s:%d/poc_db?allowPublicKeyRetrieval=true&useSSL=false", host, port),
        user,
        password);
  }

  private static JSONObject afterOf(String msg) {
    try {
      JSONObject a = new JSONObject(msg).optJSONObject("after");
      return a != null && a.has("id") ? a : null;
    } catch (Exception e) {
      return null;
    }
  }

  @Test
  @Timeout(180)
  void debeziumPropagatesAddColumnToEventPayload() throws Exception {
    // Fresh dedicated table so the connector's initial snapshot is clean.
    try (Connection c = rootConn();
        Statement s = c.createStatement()) {
      s.execute("DROP TABLE IF EXISTS poc_db.schema_evo");
      s.execute(
          "CREATE TABLE poc_db.schema_evo ("
              + "id BIGINT NOT NULL AUTO_INCREMENT, "
              + "customer_id BIGINT NOT NULL, "
              + "amount DECIMAL(10,2) NOT NULL, "
              + "status VARCHAR(1024), "
              + "created_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3), "
              + "PRIMARY KEY (id))");
    }

    // Deploy a dedicated connector against the isolated table.
    String config =
        buildDebeziumConnectorConfig(
            CONNECTOR_NAME, "5560", "mysql-schema-evo", TABLE, "schema-evo", "poc.kc.schema-evo");
    deployConnector(CONNECTOR_NAME, config);
    waitForConnectorRunning(CONNECTOR_NAME, Duration.ofSeconds(60));

    // ── Pre-ALTER insert: no new_field in the payload ─────────────────────────
    long preId;
    String preMarker = "PRE-ALTER-" + java.util.UUID.randomUUID();
    try (Connection c = flinkConn();
        Statement s = c.createStatement()) {
      s.executeUpdate(
          String.format(
              "INSERT INTO poc_db.schema_evo (customer_id, amount, status) VALUES (1, 1.10, '%s')",
              preMarker),
          Statement.RETURN_GENERATED_KEYS);
      try (ResultSet keys = s.getGeneratedKeys()) {
        assertThat(keys.next()).isTrue();
        preId = keys.getLong(1);
      }
    }
    // Select by the unique status marker, NOT by id: this test DROPs+CREATEs the table each run,
    // which resets AUTO_INCREMENT, so preId is always 1 and postId always 2. Matching on id alone
    // would pick up a retained message at the same low id from a previous run on this
    // never-truncated topic (observed: post-ALTER assertion read a prior run's new_field value).
    String preMsg =
        waitForKafkaMessage(
            TOPIC,
            Duration.ofSeconds(60),
            m -> {
              JSONObject a = afterOf(m);
              return a != null && preMarker.equals(a.optString("status"));
            });
    assertThat(preMsg).as("pre-ALTER row reached Kafka for marker=" + preMarker).isNotNull();
    JSONObject preAfter = afterOf(preMsg);
    assertThat(preAfter.optLong("id")).isEqualTo(preId);
    assertThat(preAfter.has("new_field")).as("pre-ALTER row must NOT carry new_field").isFalse();

    // ── ALTER TABLE ADD COLUMN ────────────────────────────────────────────────
    try (Connection c = rootConn();
        Statement s = c.createStatement()) {
      s.execute("ALTER TABLE poc_db.schema_evo ADD COLUMN new_field VARCHAR(64)");
    }

    // ── Post-ALTER insert: new_field must appear in the payload ───────────────
    long postId;
    String postUuid = java.util.UUID.randomUUID().toString().replace("-", "").substring(0, 12);
    String postMarker = "POST-ALTER-" + postUuid;
    String postValue = "POST-" + postUuid;
    try (Connection c = flinkConn();
        Statement s = c.createStatement()) {
      s.executeUpdate(
          String.format(
              "INSERT INTO poc_db.schema_evo (customer_id, amount, status, new_field) "
                  + "VALUES (2, 2.20, '%s', '%s')",
              postMarker, postValue),
          Statement.RETURN_GENERATED_KEYS);
      try (ResultSet keys = s.getGeneratedKeys()) {
        assertThat(keys.next()).isTrue();
        postId = keys.getLong(1);
      }
    }
    // Match on the unique post-ALTER status marker, not id (see comment above on AUTO_INCREMENT
    // reset). This guarantees we read THIS run's post-ALTER row, so the new_field assertion below
    // reflects the current schema evolution rather than a stale prior-run message.
    String postMsg =
        waitForKafkaMessage(
            TOPIC,
            Duration.ofSeconds(60),
            m -> {
              JSONObject a = afterOf(m);
              return a != null && postMarker.equals(a.optString("status"));
            });
    assertThat(postMsg).as("post-ALTER row reached Kafka for marker=" + postMarker).isNotNull();
    JSONObject postAfter = afterOf(postMsg);
    assertThat(postAfter.optLong("id")).isEqualTo(postId);
    assertThat(postAfter.has("new_field"))
        .as("post-ALTER row MUST carry new_field (schema evolution propagated)")
        .isTrue();
    assertThat(postAfter.optString("new_field")).isEqualTo(postValue);

    log.info(
        "Schema evolution verified: pre-ALTER id={} (no new_field), post-ALTER id={} (new_field='{}')",
        preId,
        postId,
        postValue);
  }
}
