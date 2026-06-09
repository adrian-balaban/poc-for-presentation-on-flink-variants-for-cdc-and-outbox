package poc.common.config;

import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class JobConfigTest {

    // Lookup that returns null for every key — exercises all defaults
    private static final java.util.function.Function<String, String> EMPTY = k -> null;

    @Test
    void defaults_whenNoEnvVarsSet() {
        JobConfig cfg = JobConfig.fromEnv(EMPTY);

        assertEquals("localhost",     cfg.mysqlHost);
        assertEquals(3306,            cfg.mysqlPort);
        assertEquals("flink",         cfg.mysqlUser);
        assertEquals("flink",         cfg.mysqlPassword);
        assertEquals("poc_db",        cfg.mysqlDatabase);
        assertEquals("poc_db.orders", cfg.mysqlTables);
        assertEquals("localhost:9092", cfg.kafkaBootstrap);
        assertEquals("poc.cdc",       cfg.kafkaTopicPrefix);
        assertEquals("5900-5999",     cfg.serverId);
    }

    @Test
    void blankValues_fallBackToDefaults() {
        java.util.function.Function<String, String> blanks = k -> "  ";

        JobConfig cfg = JobConfig.fromEnv(blanks);

        assertEquals("localhost", cfg.mysqlHost);
        assertEquals(3306,        cfg.mysqlPort);
        assertEquals("poc.cdc",   cfg.kafkaTopicPrefix);
    }

    @Test
    void envValues_overrideDefaults() {
        Map<String, String> env = Map.of(
            "MYSQL_HOST",      "db.internal",
            "MYSQL_PORT",      "3307",
            "MYSQL_USER",      "admin",
            "MYSQL_PASSWORD",  "secret",
            "MYSQL_DATABASE",  "mydb",
            "MYSQL_TABLES",    "mydb.events",
            "KAFKA_BOOTSTRAP", "kafka:9092",
            "KAFKA_TOPIC_PREFIX", "my.prefix",
            "MYSQL_SERVER_ID", "1000-1099"
        );

        JobConfig cfg = JobConfig.fromEnv(env::get);

        assertEquals("db.internal",  cfg.mysqlHost);
        assertEquals(3307,            cfg.mysqlPort);
        assertEquals("admin",         cfg.mysqlUser);
        assertEquals("secret",        cfg.mysqlPassword);
        assertEquals("mydb",          cfg.mysqlDatabase);
        assertEquals("mydb.events",   cfg.mysqlTables);
        assertEquals("kafka:9092",    cfg.kafkaBootstrap);
        assertEquals("my.prefix",     cfg.kafkaTopicPrefix);
        assertEquals("1000-1099",     cfg.serverId);
    }

    @Test
    void publicFromEnv_returnsNonNull() {
        // Smoke-test the System.getenv path — env vars may or may not be set
        assertNotNull(JobConfig.fromEnv());
    }

    @Test
    void builder_setsAllFields() {
        JobConfig cfg = new JobConfig.Builder()
            .mysqlHost("h")
            .mysqlPort(5555)
            .mysqlUser("u")
            .mysqlPassword("p")
            .mysqlDatabase("db")
            .mysqlTables("db.t")
            .kafkaBootstrap("k:9092")
            .kafkaTopicPrefix("pfx")
            .serverId("1-9")
            .build();

        assertEquals("h",      cfg.mysqlHost);
        assertEquals(5555,     cfg.mysqlPort);
        assertEquals("u",      cfg.mysqlUser);
        assertEquals("p",      cfg.mysqlPassword);
        assertEquals("db",     cfg.mysqlDatabase);
        assertEquals("db.t",   cfg.mysqlTables);
        assertEquals("k:9092", cfg.kafkaBootstrap);
        assertEquals("pfx",    cfg.kafkaTopicPrefix);
        assertEquals("1-9",    cfg.serverId);
    }
}
