package poc.component;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Path;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.time.Duration;
import java.util.function.Function;
import java.util.stream.Stream;
import lombok.extern.slf4j.Slf4j;
import org.json.JSONObject;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Parity test — the POC's central thesis is that Flink CDC can replace the Kafka Connect Debezium
 * connector for the <em>same</em> CDC pattern. This test exercises both engines against the same
 * inserted row, for every variant, and asserts they emit semantically equivalent payloads, so drift
 * in field names, envelope shape, or value encoding between engines is caught.
 *
 * <p>Covers all five variants:
 *
 * <ul>
 *   <li><b>DataStream, Table API, SQL API, YAML Pipeline</b> — order-row parity ({@link
 *       #flinkAndKafkaConnect_emitEquivalentPayloadForSameRow}). Insert one row into {@code
 *       poc_db.orders}; assert both engines emit the same {@code customer_id} and {@code status}
 *       for that row id.
 *   <li><b>Outbox</b> — outbox-row parity ({@link
 *       #flinkAndKafkaConnect_emitEquivalentOutboxPayloadForSameRow}). The two engines route
 *       differently (Flink sinks all events to one topic; KC fans out per destination), so this
 *       asserts the same {@code after} payload for the same row id on both routing shapes.
 * </ul>
 *
 * <p>Compares only fields whose encoding is identical across both engines: {@code customer_id}
 * (BIGINT → long) and {@code status} (STRING) for order rows; {@code id}, {@code destination}, and
 * {@code payload} for outbox rows. {@code amount} (DECIMAL) is intentionally excluded from order
 * parity — the KC connector uses {@code decimal.handling.mode=string} ("12.34") while the Flink
 * DataStream / YAML jobs use the Debezium default decimal encoding, so the two representations
 * differ by design and are not a parity bug. (Table API and SQL API emit {@code amount} as a plain
 * number, but KC still emits it as a string, so it is excluded uniformly across all order variants
 * rather than per-variant.) The outbox {@code payload} column is MySQL {@code JSON}, which both
 * engines' Debezium sources serialize as a canonical JSON string, so it is safe to compare.
 *
 * <p><b>Envelope-shape note.</b> The Flink variants do not all emit the same envelope: DataStream
 * and YAML emit a Debezium envelope ({@code {before, after:{...}, source, ...}}) while Table API
 * and SQL API emit a flat upsert-kafka row ({@code {id, customer_id, amount, status, ...}}). The KC
 * connector always emits a Debezium envelope. Each variant therefore carries a {@link
 * FlinkRowExtractor} that returns the object holding {@code id}/{@code customer_id}/{@code status}
 * for that variant's shape, so the parity assertion is shape-agnostic.
 *
 * <p>Extends {@link KafkaConnectBase} for the KC REST client and reuses the package-private {@code
 * FlinkTestBase.flink} client + {@code ensureJobRunning} for the Flink side, asserting both are
 * available before running (skips gracefully if either engine is down).
 */
@Slf4j
@DisplayName("Flink vs Kafka Connect CDC Parity")
class CdcParityTest extends KafkaConnectBase {

  private static volatile Boolean flinkAvailableForParity = null;

  @BeforeEach
  void verifyFlinkAvailableForParity() {
    if (flinkAvailableForParity == null) {
      flinkAvailableForParity = FlinkTestBase.flink.isAvailable();
    }
    Assumptions.assumeTrue(
        flinkAvailableForParity,
        "Flink JobManager not available — skipping parity test. Set FLINK_REST_URL or run the Podman stack.");
  }

  /**
   * Extracts the object carrying {@code id}/{@code customer_id}/{@code status} from a Flink row.
   */
  @FunctionalInterface
  interface FlinkRowExtractor extends Function<String, JSONObject> {}

  /** Debezium envelope variants (DataStream, YAML, Outbox, and all KC connectors). */
  private static JSONObject afterOf(String msg) {
    try {
      JSONObject a = new JSONObject(msg).optJSONObject("after");
      return a != null && a.has("id") ? a : null;
    } catch (Exception e) {
      return null;
    }
  }

  /** Flat upsert-kafka row variants (Table API, SQL API) — the row itself carries {@code id}. */
  private static JSONObject rowOf(String msg) {
    try {
      JSONObject r = new JSONObject(msg);
      return r.has("id") ? r : null;
    } catch (Exception e) {
      return null;
    }
  }

  /** Per-variant parity specification for the four order-emitting variants. */
  record ParitySpec(
      String label,
      Path flinkJar, // null for the YAML pipeline (submitted by flink-cdc-submitter, no fat-jar)
      String flinkEntry,
      String flinkJobName,
      String flinkTopic,
      String kcConnector,
      String kcServerId,
      String kcServerName,
      String kcVariant,
      String kcTopicPrefix,
      String kcTopic,
      FlinkRowExtractor flinkExtractor) {
    @Override
    public String toString() {
      return label;
    }
  }

  static Stream<ParitySpec> orderVariants() {
    return Stream.of(
        new ParitySpec(
            "DataStream",
            FlinkTestBase.jarPath("variant-flink-datastream-api-v1-cdc-job"),
            "poc.datastream.DataStreamCdcJob",
            "Flink DataStream API v.1 CDC Job",
            "poc.flink.datastream.orders",
            "kc-datastream-cdc",
            "5510",
            "mysql",
            "datastream-cdc",
            "poc.kc.datastream",
            "poc.kc.datastream.orders",
            CdcParityTest::afterOf),
        new ParitySpec(
            "Table API",
            FlinkTestBase.jarPath("variant-flink-table-api-cdc-job"),
            "poc.tableapi.TableApiCdcJob",
            "Flink Table API CDC Job",
            "poc.flink.table-api.orders",
            "kc-table-api-cdc",
            "5520",
            "mysql-tableapi",
            "table-api-cdc",
            "poc.kc.table-api",
            "poc.kc.table-api.orders",
            CdcParityTest::rowOf),
        new ParitySpec(
            "SQL API",
            FlinkTestBase.jarPath("variant-flink-sql-api-cdc-job"),
            "poc.sqlapi.SqlApiCdcJob",
            "Flink Sql API CDC Job",
            "poc.flink.sql-api.orders",
            "kc-sql-api-cdc",
            "5530",
            "mysql-sqlapi",
            "sql-api-cdc",
            "poc.kc.sql-api",
            "poc.kc.sql-api.orders",
            CdcParityTest::rowOf),
        new ParitySpec(
            "YAML Pipeline",
            null, // no fat-jar — the flink-cdc-submitter container runs flink-cdc.sh pipeline.yaml
            null,
            "Flink CDC YAML Pipeline CDC Job",
            "poc.flink.yaml-pipeline.orders",
            "kc-yaml-pipeline-cdc",
            "5540",
            "mysql-yaml",
            "yaml-pipeline-cdc",
            "poc.kc.yaml-pipeline",
            "poc.kc.yaml-pipeline.orders",
            CdcParityTest::afterOf));
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("orderVariants")
  @Timeout(180)
  void flinkAndKafkaConnect_emitEquivalentPayloadForSameRow(ParitySpec spec) throws Exception {
    // Start (or reuse) the Flink job for this variant. The YAML pipeline has no fat-jar — it is
    // submitted at stack start by the flink-cdc-submitter container, so we do not call
    // ensureJobRunning for it; if the submitter did not start the job the Flink-side wait below
    // times out and the assertion reports it (mirroring YamlPipelineCdcTest).
    if (spec.flinkJar() != null) {
      FlinkTestBase.ensureJobRunning(
          spec.flinkJar(), spec.flinkEntry(), spec.flinkJobName(), Duration.ofSeconds(120));
    }
    String config =
        buildDebeziumConnectorConfig(
            spec.kcConnector(),
            spec.kcServerId(),
            spec.kcServerName(),
            "poc_db.orders",
            spec.kcVariant(),
            spec.kcTopicPrefix());
    deployConnector(spec.kcConnector(), config);
    waitForConnectorRunning(spec.kcConnector(), Duration.ofSeconds(60));

    long stamp = uniqueId();
    String parityMarker = "PARITY-" + stamp;
    long rowId;
    try (Connection c = flinkConn();
        Statement s = c.createStatement()) {
      s.executeUpdate(
          String.format(
              "INSERT INTO poc_db.orders (customer_id, amount, status) VALUES (%d, 12.34, '%s')",
              stamp, parityMarker),
          Statement.RETURN_GENERATED_KEYS);
      try (ResultSet keys = s.getGeneratedKeys()) {
        assertThat(keys.next()).isTrue();
        rowId = keys.getLong(1);
      }
    }

    // Locate the same row in both engines' output by id. The Flink side uses the variant-specific
    // extractor (envelope `after` for DataStream/YAML, flat row for Table API/SQL API); the KC side
    // always emits a Debezium envelope, so it uses `afterOf`.
    JSONObject flinkFields =
        spec.flinkExtractor()
            .apply(
                waitForKafkaMessage(
                    spec.flinkTopic(),
                    Duration.ofSeconds(60),
                    m -> {
                      JSONObject f = spec.flinkExtractor().apply(m);
                      return f != null && f.optLong("id") == rowId;
                    }));
    JSONObject kcAfter =
        afterOf(
            waitForKafkaMessage(
                spec.kcTopic(),
                Duration.ofSeconds(60),
                m -> {
                  JSONObject a = afterOf(m);
                  return a != null && a.optLong("id") == rowId;
                }));

    assertThat(flinkFields)
        .as("Flink emitted a payload for id=" + rowId + " on " + spec.flinkTopic())
        .isNotNull();
    assertThat(kcAfter)
        .as("Kafka Connect emitted an after payload for id=" + rowId + " on " + spec.kcTopic())
        .isNotNull();

    // Parity on identity + payload fields that share encoding across engines (amount excluded —
    // decimal encoding differs by design; see class Javadoc).
    assertThat(flinkFields.optLong("id")).isEqualTo(rowId);
    assertThat(kcAfter.optLong("id")).isEqualTo(rowId);
    assertThat(flinkFields.optLong("customer_id"))
        .as("[" + spec + "] customer_id matches across engines")
        .isEqualTo(kcAfter.optLong("customer_id"));
    assertThat(flinkFields.optString("status"))
        .as("[" + spec + "] status matches across engines")
        .isEqualTo(kcAfter.optString("status"))
        .isEqualTo(parityMarker);

    log.info(
        "[{}] Parity verified for id={}: Flink and KC agree on customer_id={} status='{}'",
        spec,
        rowId,
        flinkFields.optLong("customer_id"),
        parityMarker);
  }

  // ── Outbox parity ──────────────────────────────────────────────────────────
  // Structurally different from order parity: the two engines route differently. Flink's OutboxJob
  // sinks ALL events to a single topic (poc.flink.outbox.outbox-events) with the destination
  // preserved in `after`; KC's OutboxRoutingTransform fans out to per-destination topics
  // (poc.kc.outbox.<destination>). Parity = the same inserted outbox row yields the same `after`
  // payload (id, destination, payload) on both engines, despite the routing-shape difference.
  private static final Path OUTBOX_JAR =
      FlinkTestBase.jarPath("variant-flink-datastream-api-v1-outbox-job");
  private static final String OUTBOX_JOB_NAME = "Flink DataStream API v.1 Outbox Job";
  private static final String OUTBOX_FLINK_TOPIC = "poc.flink.outbox.outbox-events";
  private static final String OUTBOX_CONNECTOR = "kc-outbox-cdc";

  @Test
  @Timeout(180)
  void flinkAndKafkaConnect_emitEquivalentOutboxPayloadForSameRow() throws Exception {
    FlinkTestBase.ensureJobRunning(
        OUTBOX_JAR, "poc.outbox.OutboxJob", OUTBOX_JOB_NAME, Duration.ofSeconds(120));
    deployConnector(
        OUTBOX_CONNECTOR, buildOutboxConnectorConfig(OUTBOX_CONNECTOR, "5550", "mysql-outbox"));
    waitForConnectorRunning(OUTBOX_CONNECTOR, Duration.ofSeconds(60));

    long stamp = uniqueId();
    String dest = "parity-" + stamp;
    String payload = String.format("{\"order_id\":%d}", stamp);
    long rowId;
    try (Connection c = flinkConn();
        Statement s = c.createStatement()) {
      s.executeUpdate(
          String.format(
              "INSERT INTO poc_db.outbox_events (destination, payload) VALUES ('%s', '%s')",
              dest, payload),
          Statement.RETURN_GENERATED_KEYS);
      try (ResultSet keys = s.getGeneratedKeys()) {
        assertThat(keys.next()).isTrue();
        rowId = keys.getLong(1);
      }
    }

    // Flink: single topic, destination preserved in `after`.
    JSONObject flinkAfter =
        afterOf(
            waitForKafkaMessage(
                OUTBOX_FLINK_TOPIC,
                Duration.ofSeconds(60),
                m -> {
                  JSONObject a = afterOf(m);
                  return a != null
                      && a.optLong("id") == rowId
                      && dest.equals(a.optString("destination"));
                }));
    // KC: per-destination topic. Same `after` payload, different routing shape.
    String kcTopic = "poc.kc.outbox." + dest;
    JSONObject kcAfter =
        afterOf(
            waitForKafkaMessage(
                kcTopic,
                Duration.ofSeconds(60),
                m -> {
                  JSONObject a = afterOf(m);
                  return a != null
                      && a.optLong("id") == rowId
                      && dest.equals(a.optString("destination"));
                }));

    assertThat(flinkAfter)
        .as("Flink emitted an outbox after payload for id=" + rowId + " on " + OUTBOX_FLINK_TOPIC)
        .isNotNull();
    assertThat(kcAfter)
        .as("Kafka Connect emitted an outbox after payload for id=" + rowId + " on " + kcTopic)
        .isNotNull();

    assertThat(flinkAfter.optLong("id")).isEqualTo(rowId);
    assertThat(kcAfter.optLong("id")).isEqualTo(rowId);
    assertThat(flinkAfter.optString("destination"))
        .as("destination matches across engines")
        .isEqualTo(kcAfter.optString("destination"))
        .isEqualTo(dest);
    // payload is a MySQL JSON column — both engines' Debezium sources serialize it as a canonical
    // JSON string, so the cross-engine value matches (it need not match the inserted literal).
    assertThat(flinkAfter.optString("payload"))
        .as("payload matches across engines")
        .isEqualTo(kcAfter.optString("payload"));

    log.info(
        "Outbox parity verified for id={}: Flink (single topic) and KC (per-destination {}) agree on destination + payload",
        rowId,
        dest);
  }
}
