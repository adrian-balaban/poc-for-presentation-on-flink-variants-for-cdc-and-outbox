package poc.component;

import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.nio.file.Path;
import java.sql.Connection;
import java.sql.Statement;
import java.time.Duration;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Component test for SQL API CDC variant (StatementSet — single JobGraph for two tables).
 *
 * Submits the fat-jar to the Flink JobManager container (localhost:8081) via REST.
 * Server-IDs 5800-5849 (orders) and 5850-5899 (customers) are hardcoded in SqlApiCdcJob DDL.
 */
@Slf4j
@DisplayName("Flink SQL API : CDC Test")
class SqlApiCdcTest extends FlinkTestBase {

    private static final Path JAR = jarPath("variant-flink-sql-api-cdc-job");
    private static final String ORDERS_TOPIC = "poc.cdc.sql-api.orders";

    @Test
    @Timeout(90)
    void sqlApiPipeline_capturesBothTables_andPublishesToSeparateKafkaTopics() throws Exception {
        try (Connection c = flinkConn(); Statement s = c.createStatement()) {
            s.executeUpdate("INSERT INTO poc_db.orders (customer_id, amount, status) VALUES (1, 33.33, 'SQL-TEST')");
            s.executeUpdate("INSERT INTO poc_db.customers (name, email) VALUES ('TestUser', 'test@example.com')");
        }

        ensureJobRunning(JAR, "poc.sqlapi.SqlApiCdcJob",
            "Flink Sql API CDC Job", Duration.ofSeconds(30));
        List<String> messages = pollKafka(ORDERS_TOPIC, 1, Duration.ofSeconds(45));
        assertThat(messages).isNotEmpty();
        assertThat(messages).anyMatch(m -> m.contains("SQL-TEST"));
        log.info("SQL API CDC: {} Kafka message(s) received on {}", messages.size(), ORDERS_TOPIC);
    }
}
