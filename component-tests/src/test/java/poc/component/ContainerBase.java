package poc.component;

import java.sql.*;
import java.time.*;
import java.util.*;
import java.util.function.Predicate;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeEach;
import poc.common.config.JobConfig;

/**
 * Base class for component tests that use existing container infrastructure. Tests connect to MySQL
 * + Kafka already running via podman-compose (default) or the k8s slice via kubectl port-forward.
 *
 * <p>The connection endpoints are env-overridable so the same tests can target either stack without
 * code changes: against the Podman stack the defaults apply; against the k8s slice set, e.g.,
 * {@code MYSQL_PORT=13306 KAFKA_BOOTSTRAP=localhost:19092} (the port-forward ports from K8S.md).
 * User/password/database are identical across both stacks and stay fixed.
 *
 * <p>Server-ID ranges reserved for component tests: 7000–7099 (not in CLAUDE.md prod ranges).
 */
@Slf4j
public abstract class ContainerBase {

  // Env-overridable with Podman-stack defaults — read once at class load, so set
  // them on the Gradle/test JVM invocation (e.g. MYSQL_PORT=13306 ./gradlew ...).
  private static final String MYSQL_HOST = System.getenv().getOrDefault("MYSQL_HOST", "localhost");
  private static final int MYSQL_PORT =
      Integer.parseInt(System.getenv().getOrDefault("MYSQL_PORT", "3306"));
  private static final String MYSQL_USER = "flink";
  private static final String MYSQL_PASSWORD = "flink";
  private static final String MYSQL_DATABASE = "poc_db";

  private static final String KAFKA_BOOTSTRAP =
      System.getenv().getOrDefault("KAFKA_BOOTSTRAP", "localhost:9092");

  private static volatile boolean schemaInitialized = false;
  private static volatile Boolean podmanAvailable = null;
  private static final Object podmanCheckLock = new Object();

  @BeforeEach
  void verifyPodmanAvailable() {
    if (podmanAvailable == null) {
      synchronized (podmanCheckLock) {
        if (podmanAvailable == null) {
          podmanAvailable = checkPodmanConnectivity();
        }
      }
    }
    Assumptions.assumeTrue(
        podmanAvailable,
        "CDC stack (MySQL/Kafka) not available — skipping component tests. "
            + "Run: cd local-development-podman && podman-compose -f podman-compose.yml up -d "
            + "(Podman), or ./local-development-k8s/deploy.sh + kubectl port-forward (k8s; see K8S.md)");
  }

  private static boolean checkPodmanConnectivity() {
    try (Connection c =
        DriverManager.getConnection(
            String.format(
                "jdbc:mysql://%s:%d/%s?allowPublicKeyRetrieval=true&useSSL=false",
                MYSQL_HOST, MYSQL_PORT, MYSQL_DATABASE),
            MYSQL_USER,
            MYSQL_PASSWORD)) {
      log.info("Podman stack is available");
      return true;
    } catch (SQLException e) {
      log.warn("Podman stack not available: {}", e.getMessage());
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
    try (Connection c = createConnection();
        Statement s = c.createStatement()) {
      // Tables already exist in podman-compose setup. Just verify connectivity.
      s.executeQuery("SELECT COUNT(*) FROM poc_db.orders");
      log.info("Schema verification successful - tables exist");
    }
  }

  // ── Helpers ───────────────────────────────────────────────────────────────

  private static Connection createConnection() throws SQLException {
    return DriverManager.getConnection(
        String.format(
            "jdbc:mysql://%s:%d/%s?allowPublicKeyRetrieval=true&useSSL=false",
            MYSQL_HOST, MYSQL_PORT, MYSQL_DATABASE),
        MYSQL_USER,
        MYSQL_PASSWORD);
  }

  /** JDBC connection as the flink user. */
  protected static Connection flinkConn() throws SQLException {
    ensureSchema();
    return createConnection();
  }

  /**
   * Builds a JobConfig wired to localhost MySQL + Kafka.
   *
   * @param serverId server-id range for this variant's Flink CDC source (e.g. "7000-7009")
   * @param tables MySQL table list (e.g. "poc_db.orders")
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
   * Polls a Kafka topic until {@code minCount} messages arrive or the deadline passes. Uses a fresh
   * consumer group each call so offset resets to earliest.
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
      log.warn(
          "pollKafka timed out on topic '{}': got {}/{} messages within {}",
          topic,
          messages.size(),
          minCount,
          timeout);
    }
    return messages;
  }

  /**
   * Scans a Kafka topic from the earliest offset until a message satisfying {@code filter} is
   * found, or the deadline passes.
   *
   * <p>Use this instead of {@link #pollKafka} when the test needs a <em>specific</em> message (e.g.
   * the row it just inserted) rather than just the first N available. The topic may contain many
   * historical snapshot messages, so taking the first message without filtering returns stale data.
   *
   * @return the first matching message, or {@code null} if none found within the timeout
   */
  protected static String waitForKafkaMessage(
      String topic, Duration timeout, Predicate<String> filter) {
    ensureSchema();
    Properties props = new Properties();
    props.put("bootstrap.servers", KAFKA_BOOTSTRAP);
    props.put("group.id", "test-" + UUID.randomUUID());
    props.put("auto.offset.reset", "earliest");
    props.put("key.deserializer", StringDeserializer.class.getName());
    props.put("value.deserializer", StringDeserializer.class.getName());

    try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
      consumer.subscribe(List.of(topic));
      Instant deadline = Instant.now().plus(timeout);
      while (Instant.now().isBefore(deadline)) {
        ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(500));
        for (var record : records) {
          if (filter.test(record.value())) {
            return record.value();
          }
        }
      }
    }
    log.warn("waitForKafkaMessage timed out on topic '{}' within {}", topic, timeout);
    return null;
  }
}
