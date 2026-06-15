package poc.component;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Assumptions;
import poc.common.config.JobConfig;

import java.sql.*;
import java.time.*;
import java.util.*;

/**
 * Base class for component tests that use existing Docker infrastructure.
 * Tests connect to MySQL + Kafka already running via docker-compose.
 * This avoids Testcontainers Docker client initialization issues.
 *
 * Server-ID ranges reserved for component tests: 7000–7099 (not in CLAUDE.md prod ranges).
 */
@Slf4j
public abstract class ContainerBase {

    private static final String MYSQL_HOST = "localhost";
    private static final int MYSQL_PORT = 3306;
    private static final String MYSQL_USER = "flink";
    private static final String MYSQL_PASSWORD = "flink";
    private static final String MYSQL_DATABASE = "poc_db";

    private static final String KAFKA_BOOTSTRAP = "localhost:9092";

    private static volatile boolean schemaInitialized = false;
    private static volatile Boolean dockerAvailable = null;
    private static final Object dockerCheckLock = new Object();

    @BeforeEach
    void verifyDockerAvailable() {
        if (dockerAvailable == null) {
            synchronized (dockerCheckLock) {
                if (dockerAvailable == null) {
                    dockerAvailable = checkDockerConnectivity();
                }
            }
        }
        Assumptions.assumeTrue(dockerAvailable,
            "Podman stack (MySQL/Kafka) not available — skipping component tests. " +
            "Run: cd local-development && podman-compose -f podman-compose.yml up -d");
    }

    private static boolean checkDockerConnectivity() {
        try (Connection c = DriverManager.getConnection(
            String.format("jdbc:mysql://%s:%d/%s?allowPublicKeyRetrieval=true&useSSL=false",
                MYSQL_HOST, MYSQL_PORT, MYSQL_DATABASE),
            MYSQL_USER, MYSQL_PASSWORD)) {
            log.info("Docker containers are available");
            return true;
        } catch (SQLException e) {
            log.warn("Docker containers not available: {}", e.getMessage());
            return false;
        }
    }

    /** Ensure schema exists (call once per test suite). */
    protected static synchronized void ensureSchema() {
        if (schemaInitialized) return;
        try {
            initSchema();
            schemaInitialized = true;
            log.info("Schema initialized");
        } catch (SQLException e) {
            throw new RuntimeException("Schema init failed", e);
        }
    }

    private static void initSchema() throws SQLException {
        try (Connection c = createConnection(); Statement s = c.createStatement()) {
            // Tables already exist in docker-compose setup. Just verify connectivity.
            s.executeQuery("SELECT COUNT(*) FROM poc_db.orders");
            log.info("Schema verification successful - tables exist");
        }
    }

    // ── Helpers ───────────────────────────────────────────────────────────────

    private static Connection createConnection() throws SQLException {
        return DriverManager.getConnection(
            String.format("jdbc:mysql://%s:%d/%s?allowPublicKeyRetrieval=true&useSSL=false",
                MYSQL_HOST, MYSQL_PORT, MYSQL_DATABASE),
            MYSQL_USER, MYSQL_PASSWORD);
    }

    /** JDBC connection as the flink user. */
    protected static Connection flinkConn() throws SQLException {
        ensureSchema();
        return createConnection();
    }

    /**
     * Builds a JobConfig wired to localhost MySQL + Kafka.
     *
     * @param serverId  server-id range for this variant's Flink CDC source (e.g. "7000-7009")
     * @param tables    MySQL table list (e.g. "poc_db.orders")
     */
    protected static JobConfig testConfig(String serverId, String tables) {
        ensureSchema();
        return new JobConfig.Builder()
            .mysqlHost(MYSQL_HOST)
            .mysqlPort(MYSQL_PORT)
            .mysqlUser(MYSQL_USER)
            .mysqlPassword(MYSQL_PASSWORD)
            .mysqlDatabase(MYSQL_DATABASE)
            .mysqlTables(tables)
            .kafkaBootstrap(KAFKA_BOOTSTRAP)
            .kafkaTopicPrefix("test.cdc")
            .serverId(serverId)
            .build();
    }

    /**
     * Polls a Kafka topic until {@code minCount} messages arrive or the deadline passes.
     * Uses a fresh consumer group each call so offset resets to earliest.
     */
    protected static List<String> pollKafka(String topic, int minCount, Duration timeout) {
        ensureSchema();
        Properties props = new Properties();
        props.put("bootstrap.servers", KAFKA_BOOTSTRAP);
        props.put("group.id", "test-" + UUID.randomUUID());
        props.put("auto.offset.reset", "earliest");
        props.put("key.deserializer", StringDeserializer.class.getName());
        props.put("value.deserializer", StringDeserializer.class.getName());

        List<String> messages = new ArrayList<>();
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            consumer.subscribe(List.of(topic));
            Instant deadline = Instant.now().plus(timeout);
            while (messages.size() < minCount && Instant.now().isBefore(deadline)) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(500));
                records.forEach(r -> messages.add(r.value()));
            }
        }
        if (messages.size() < minCount) {
            log.warn("pollKafka timed out on topic '{}': got {}/{} messages within {}",
                topic, messages.size(), minCount, timeout);
        }
        return messages;
    }
}
