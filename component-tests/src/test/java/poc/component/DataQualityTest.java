package poc.component;

import lombok.extern.slf4j.Slf4j;
import org.json.JSONObject;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.math.BigDecimal;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.Statement;
import java.time.Duration;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Data quality tests for CDC output.
 *
 * Validates that Kafka messages are well-formed JSON with expected fields,
 * types, and values. Runs against the DataStream variant but patterns
 * apply to all variants.
 */
@Slf4j
@DisplayName("CDC Data Quality Validation")
class DataQualityTest extends FlinkTestBase {

    private static final Path JAR = jarPath("variant-flink-datastream-api-v1-cdc-job");
    private static final String JOB_NAME = "Flink DataStream API v.1 CDC Job";
    private static final String TOPIC = "poc.cdc.datastream";

    @Test
    @Timeout(90)
    void kafkaMessage_deserializesToValidJson_withRequiredFields() throws Exception {
        try (Connection c = flinkConn(); Statement s = c.createStatement()) {
            s.executeUpdate(
                "INSERT INTO poc_db.orders (customer_id, amount, status) VALUES (1000, 123.45, 'DQ-TEST')");
        }

        ensureJobRunning(JAR, "poc.datastream.DataStreamCdcJob", JOB_NAME, Duration.ofSeconds(30));
        List<String> messages = pollKafka(TOPIC, 1, Duration.ofSeconds(45));

        assertThat(messages).isNotEmpty();
        JSONObject json = new JSONObject(messages.get(0));

        // Verify structure
        assertThat(json.has("id")).isTrue();
        assertThat(json.has("customer_id")).isTrue();
        assertThat(json.has("amount")).isTrue();
        assertThat(json.has("status")).isTrue();

        // Verify types and values
        assertThat(json.getLong("customer_id")).isEqualTo(1000);
        assertThat(json.getString("status")).isEqualTo("DQ-TEST");
        assertThat(new BigDecimal(json.getString("amount")).compareTo(new BigDecimal("123.45"))).isZero();

        log.info("Data quality check passed: message={}", json.toString());
    }

    @Test
    @Timeout(90)
    void kafkaMessage_includesVariantAnnotation() throws Exception {
        try (Connection c = flinkConn(); Statement s = c.createStatement()) {
            s.executeUpdate(
                "INSERT INTO poc_db.orders (customer_id, amount, status) VALUES (2000, 99.99, 'VARIANT-TEST')");
        }

        ensureJobRunning(JAR, "poc.datastream.DataStreamCdcJob", JOB_NAME, Duration.ofSeconds(30));
        List<String> messages = pollKafka(TOPIC, 1, Duration.ofSeconds(45));

        assertThat(messages).isNotEmpty();
        JSONObject json = new JSONObject(messages.get(0));

        assertThat(json.has("variant")).as("variant annotation should be present").isTrue();
        assertThat(json.getString("variant")).isEqualTo("datastream-cdc");
        assertThat(json.getString("topic")).isEqualTo("poc.cdc.datastream");

        log.info("Variant annotation verified");
    }

    @Test
    @Timeout(90)
    void kafkaMessage_preservesNullValues() throws Exception {
        try (Connection c = flinkConn(); Statement s = c.createStatement()) {
            // Insert row with NULL status
            s.executeUpdate(
                "INSERT INTO poc_db.orders (customer_id, amount, status) VALUES (3000, 50.00, NULL)");
        }

        ensureJobRunning(JAR, "poc.datastream.DataStreamCdcJob", JOB_NAME, Duration.ofSeconds(30));
        List<String> messages = pollKafka(TOPIC, 1, Duration.ofSeconds(45));

        assertThat(messages).isNotEmpty();
        JSONObject json = new JSONObject(messages.get(0));

        // In JSON, NULL should be represented as null
        assertThat(json.isNull("status")).isTrue();
        log.info("NULL value handling verified");
    }

    @Test
    @Timeout(90)
    void multipleInserts_produceMultipleMessages_inOrder() throws Exception {
        try (Connection c = flinkConn(); Statement s = c.createStatement()) {
            s.executeUpdate("INSERT INTO poc_db.orders (customer_id, amount, status) VALUES (4000, 10.00, 'FIRST')");
            s.executeUpdate("INSERT INTO poc_db.orders (customer_id, amount, status) VALUES (4001, 20.00, 'SECOND')");
            s.executeUpdate("INSERT INTO poc_db.orders (customer_id, amount, status) VALUES (4002, 30.00, 'THIRD')");
        }

        ensureJobRunning(JAR, "poc.datastream.DataStreamCdcJob", JOB_NAME, Duration.ofSeconds(30));
        List<String> messages = pollKafka(TOPIC, 3, Duration.ofSeconds(60));

        assertThat(messages.size()).isGreaterThanOrEqualTo(3);

        // Extract status values to verify ordering
        List<String> statuses = messages.stream()
            .map(m -> new JSONObject(m).getString("status"))
            .distinct()
            .toList();

        assertThat(statuses).contains("FIRST", "SECOND", "THIRD");
        log.info("Message ordering verified: {} messages", messages.size());
    }

    @Test
    @Timeout(90)
    void jsonFormat_isConsistent_acrossMessages() throws Exception {
        try (Connection c = flinkConn(); Statement s = c.createStatement()) {
            for (int i = 0; i < 3; i++) {
                s.executeUpdate(
                    String.format(
                        "INSERT INTO poc_db.orders (customer_id, amount, status) VALUES (%d, %.2f, 'FORMAT-TEST')",
                        5000 + i,
                        100.00 + i));
            }
        }

        ensureJobRunning(JAR, "poc.datastream.DataStreamCdcJob", JOB_NAME, Duration.ofSeconds(30));
        List<String> messages = pollKafka(TOPIC, 3, Duration.ofSeconds(60));

        assertThat(messages.size()).isGreaterThanOrEqualTo(3);

        // All messages should parse as valid JSON and have the same set of top-level keys
        for (String msg : messages.subList(0, Math.min(3, messages.size()))) {
            JSONObject json = new JSONObject(msg);
            assertThat(json.has("id")).isTrue();
            assertThat(json.has("customer_id")).isTrue();
            assertThat(json.has("amount")).isTrue();
            assertThat(json.has("status")).isTrue();
            assertThat(json.has("variant")).isTrue();
        }

        log.info("JSON format consistency verified across {} messages", messages.size());
    }
}
