package poc.component;

import lombok.extern.slf4j.Slf4j;
import org.json.JSONObject;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Assumptions;

import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.net.URI;
import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Base class for Kafka Connect component tests.
 * Extends ContainerBase for MySQL + Kafka access, adds Kafka Connect REST API client.
 */
@Slf4j
public abstract class KafkaConnectBase extends ContainerBase {

    // On Podman bridge networking the kafka-connect container reaches MySQL by service name.
    // Override with -Ddb.host=localhost for Docker host-network mode.
    static final String DB_HOST = System.getProperty("db.host",
            System.getenv().getOrDefault("DB_HOST", "mysql"));

    private static final String KAFKA_CONNECT_URL = "http://localhost:8083";
    private static volatile Boolean kafkaConnectAvailable = null;
    private static final Object connectCheckLock = new Object();
    protected static final HttpClient httpClient = HttpClient.newHttpClient();

    @BeforeEach
    void verifyKafkaConnectAvailable() {
        if (kafkaConnectAvailable == null) {
            synchronized (connectCheckLock) {
                if (kafkaConnectAvailable == null) {
                    kafkaConnectAvailable = checkKafkaConnectAvailable();
                }
            }
        }
        Assumptions.assumeTrue(kafkaConnectAvailable,
            "Kafka Connect not available — skipping test. Run: cd docker/kafka-connect && ./build-and-deploy.sh");
    }

    private static boolean checkKafkaConnectAvailable() {
        try {
            HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(KAFKA_CONNECT_URL))
                .timeout(Duration.ofSeconds(5))
                .GET()
                .build();
            HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
            boolean available = response.statusCode() == 200;
            if (available) {
                log.info("Kafka Connect is available");
            } else {
                log.warn("Kafka Connect returned status: {}", response.statusCode());
            }
            return available;
        } catch (Exception e) {
            log.warn("Kafka Connect not available: {}", e.getMessage());
            return false;
        }
    }

    /**
     * Deploy a connector to Kafka Connect via REST API.
     * @param connectorName name of the connector
     * @param connectorConfig JSON connector configuration
     */
    protected static void deployConnector(String connectorName, String connectorConfig) throws Exception {
        // Always do a fresh deploy: create (if missing) → stop → delete offsets → delete → recreate.
        // Stale offsets in poc-connect-offset survive connector deletion and cause Debezium to skip
        // the initial snapshot and fail with "db history topic is missing" on restart.
        HttpResponse<String> existsCheck = httpClient.send(
            HttpRequest.newBuilder()
                .uri(URI.create(KAFKA_CONNECT_URL + "/connectors/" + connectorName + "/status"))
                .timeout(Duration.ofSeconds(5)).GET().build(),
            HttpResponse.BodyHandlers.ofString());
        if (existsCheck.statusCode() != 200) {
            // Connector doesn't exist yet — create a stub so we can use the offsets-reset API.
            httpClient.send(
                HttpRequest.newBuilder()
                    .uri(URI.create(KAFKA_CONNECT_URL + "/connectors"))
                    .timeout(Duration.ofSeconds(10))
                    .header("Content-Type", "application/json")
                    .POST(HttpRequest.BodyPublishers.ofString(connectorConfig))
                    .build(),
                HttpResponse.BodyHandlers.ofString());
            Thread.sleep(2000);
        }
        // Stop → delete offsets → delete connector.
        httpClient.send(
            HttpRequest.newBuilder()
                .uri(URI.create(KAFKA_CONNECT_URL + "/connectors/" + connectorName + "/stop"))
                .timeout(Duration.ofSeconds(10)).PUT(HttpRequest.BodyPublishers.noBody()).build(),
            HttpResponse.BodyHandlers.ofString());
        Thread.sleep(1500);
        httpClient.send(
            HttpRequest.newBuilder()
                .uri(URI.create(KAFKA_CONNECT_URL + "/connectors/" + connectorName + "/offsets"))
                .timeout(Duration.ofSeconds(10)).DELETE().build(),
            HttpResponse.BodyHandlers.ofString());
        httpClient.send(
            HttpRequest.newBuilder()
                .uri(URI.create(KAFKA_CONNECT_URL + "/connectors/" + connectorName))
                .timeout(Duration.ofSeconds(10)).DELETE().build(),
            HttpResponse.BodyHandlers.ofString());
        Thread.sleep(1000);

        // Recreate fresh with no stored offsets.
        HttpResponse<String> response = httpClient.send(
            HttpRequest.newBuilder()
                .uri(URI.create(KAFKA_CONNECT_URL + "/connectors"))
                .timeout(Duration.ofSeconds(10))
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(connectorConfig))
                .build(),
            HttpResponse.BodyHandlers.ofString());
        if (response.statusCode() == 201) {
            log.info("Connector {} deployed fresh (offsets cleared)", connectorName);
        } else {
            log.error("Failed to deploy connector {}: {} {}", connectorName, response.statusCode(), response.body());
            throw new RuntimeException("Connector deployment failed: " + response.body());
        }
    }

    /**
     * Get connector status via REST API.
     */
    protected static String getConnectorStatus(String connectorName) throws Exception {
        HttpRequest request = HttpRequest.newBuilder()
            .uri(URI.create(KAFKA_CONNECT_URL + "/connectors/" + connectorName + "/status"))
            .timeout(Duration.ofSeconds(5))
            .GET()
            .build();

        HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
        return response.body();
    }

    /**
     * Wait for connector to be in RUNNING state.
     */
    protected static void waitForConnectorRunning(String connectorName, Duration timeout) throws Exception {
        long deadline = System.currentTimeMillis() + timeout.toMillis();
        while (System.currentTimeMillis() < deadline) {
            String statusJson = getConnectorStatus(connectorName);
            try {
                org.json.JSONObject status = new org.json.JSONObject(statusJson);
                boolean connectorRunning = "RUNNING".equals(
                        status.getJSONObject("connector").getString("state"));
                org.json.JSONArray tasks = status.optJSONArray("tasks");
                boolean allTasksRunning = tasks != null && tasks.length() > 0
                        && tasks.toList().stream().allMatch(t ->
                            "RUNNING".equals(((java.util.Map<?, ?>) t).get("state")));
                if (connectorRunning && allTasksRunning) {
                    log.info("Connector {} is RUNNING (connector + all tasks)", connectorName);
                    return;
                }
            } catch (Exception ignored) { }
            Thread.sleep(1000);
        }
        throw new RuntimeException("Connector " + connectorName + " did not reach RUNNING state. Status: "
                + getConnectorStatus(connectorName));
    }

    /**
     * Build Debezium connector config with variant-specific parameters.
     * Reduces copy-paste of JSON config across test classes.
     */
    protected static String buildDebeziumConnectorConfig(
            String connectorName, String serverId, String serverName,
            String tableList, String variantName, String topicPrefix) {
        return String.format("""
            {
              "name": "%s",
              "config": {
                "connector.class": "io.debezium.connector.mysql.MySqlConnector",
                "database.hostname": "%s",
                "database.port": 3306,
                "database.user": "flink",
                "database.password": "flink",
                "database.server.id": "%s",
                "database.server.name": "%s",
                "database.include.list": "poc_db",
                "table.include.list": "%s",
                "snapshot.mode": "initial",
                "transforms": "enrichment",
                "transforms.enrichment.type": "poc.kafka.connect.EnrichmentTransform",
                "transforms.enrichment.variant.name": "%s",
                "transforms.enrichment.topic.prefix": "%s",
                "key.converter": "org.apache.kafka.connect.json.JsonConverter",
                "key.converter.schemas.enable": false,
                "value.converter": "org.apache.kafka.connect.json.JsonConverter",
                "value.converter.schemas.enable": false,
                "decimal.handling.mode": "string",
                "include.schema.changes": false,
                "schema.history.internal.kafka.bootstrap.servers": "kafka:29092",
                "schema.history.internal.kafka.topic": "dbhistory.%s",
                "topic.prefix": "%s"
              }
            }
            """, connectorName, DB_HOST, serverId, serverName, tableList, variantName, topicPrefix, variantName, topicPrefix);
    }

    /**
     * Assert enrichment metadata on Kafka Connect output.
     * Reduces duplicate assertions across test classes.
     */
    protected static void assertEnrichmentMetadata(
            String message, String jsonValue, String expectedVariant, String topicPrefix) {
        assertThat(jsonValue).as(message).isNotEmpty();
        JSONObject obj = new JSONObject(jsonValue);
        assertThat(obj.optString("variant")).as("variant field").isEqualTo(expectedVariant);
        assertThat(obj.optString("topic")).as("topic field").contains(topicPrefix);
        assertThat(obj.has("transformed_at")).as("transformed_at field present").isTrue();
    }
}
