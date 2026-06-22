package poc.sqlapi;

import static poc.common.validation.DdlValidator.requireSafeDdl;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.StatementSet;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import poc.common.checkpoint.CheckpointConfigurer;
import poc.common.config.JobConfig;

/**
 * Variant 3 — SQL API CDC
 *
 * <p>Pure SQL job definition. Uses StatementSet so all INSERT statements compile into a single
 * JobGraph — one checkpoint, one recovery unit. Minimal Java boilerplate (~210 lines in real
 * connector, mostly DDL strings).
 *
 * <p>Server-ID ranges come from {@code JobConfig#sqlApiOrdersServerId} and {@code
 * JobConfig#sqlApiCustomersServerId}.
 */
public class SqlApiCdcJob {

  public static void main(String[] args) {
    JobConfig config = JobConfig.fromEnv();
    requireSafeDdl(config.mysqlHost, "MYSQL_HOST");
    requireSafeDdl(config.mysqlUser, "MYSQL_USER");
    requireSafeDdl(config.mysqlPassword, "MYSQL_PASSWORD");
    requireSafeDdl(config.mysqlDatabase, "MYSQL_DATABASE");
    requireSafeDdl(config.kafkaBootstrap, "KAFKA_BOOTSTRAP");
    requireSafeDdl(config.kafkaTopicPrefix, "KAFKA_TOPIC_PREFIX");
    requireSafeDdl(config.sqlApiOrdersServerId, "sqlApiOrdersServerId");
    requireSafeDdl(config.sqlApiCustomersServerId, "sqlApiCustomersServerId");

    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

    // Checkpoint configuration for exactly-once CDC semantics
    CheckpointConfigurer.applyExactlyOnce(env);

    tableEnv.getConfig().getConfiguration().setString("pipeline.name", "Flink Sql API CDC Job");

    // ── Sources ──────────────────────────────────────────────────────────

    tableEnv.executeSql(
        String.format(
            """
            CREATE TABLE src_orders (
                id          BIGINT,
                customer_id BIGINT,
                amount      DECIMAL(10,2),
                status      STRING,
                created_at  TIMESTAMP(3),
                PRIMARY KEY (id) NOT ENFORCED
            ) WITH (
                'connector'        = 'mysql-cdc',
                'hostname'         = '%s',
                'port'             = '%d',
                'username'         = '%s',
                'password'         = '%s',
                'database-name'    = '%s',
                'table-name'       = 'orders',
                'server-id'        = '%s',
                'server-time-zone' = 'UTC'
            )""",
            config.mysqlHost,
            config.mysqlPort,
            config.mysqlUser,
            config.mysqlPassword,
            config.mysqlDatabase,
            config.sqlApiOrdersServerId));

    tableEnv.executeSql(
        String.format(
            """
            CREATE TABLE src_customers (
                id         BIGINT,
                name       STRING,
                email      STRING,
                PRIMARY KEY (id) NOT ENFORCED
            ) WITH (
                'connector'        = 'mysql-cdc',
                'hostname'         = '%s',
                'port'             = '%d',
                'username'         = '%s',
                'password'         = '%s',
                'database-name'    = '%s',
                'table-name'       = 'customers',
                'server-id'        = '%s',
                'server-time-zone' = 'UTC'
            )""",
            config.mysqlHost,
            config.mysqlPort,
            config.mysqlUser,
            config.mysqlPassword,
            config.mysqlDatabase,
            config.sqlApiCustomersServerId));

    // ── Sinks ─────────────────────────────────────────────────────────────

    tableEnv.executeSql(
        String.format(
            """
            CREATE TABLE sink_orders (
                id          BIGINT,
                customer_id BIGINT,
                amount      DECIMAL(10,2),
                status      STRING,
                created_at  TIMESTAMP(3),
                job_variant STRING,
                PRIMARY KEY (id) NOT ENFORCED
            ) WITH (
                'connector'                    = 'upsert-kafka',
                'topic'                        = '%s.sql-api.flink.orders',
                'properties.bootstrap.servers' = '%s',
                'key.format'                   = 'json',
                'value.format'                 = 'json'
            )""",
            config.kafkaTopicPrefix, config.kafkaBootstrap));

    tableEnv.executeSql(
        String.format(
            """
            CREATE TABLE sink_customers (
                id          BIGINT,
                name        STRING,
                email       STRING,
                job_variant STRING,
                PRIMARY KEY (id) NOT ENFORCED
            ) WITH (
                'connector'                    = 'upsert-kafka',
                'topic'                        = '%s.sql-api.flink.customers',
                'properties.bootstrap.servers' = '%s',
                'key.format'                   = 'json',
                'value.format'                 = 'json'
            )""",
            config.kafkaTopicPrefix, config.kafkaBootstrap));

    // ── StatementSet → single JobGraph ────────────────────────────────────

    StatementSet stmts = tableEnv.createStatementSet();

    stmts.addInsertSql(
        """
            INSERT INTO sink_orders
            SELECT id, customer_id, amount, status, created_at, 'sql-api' AS job_variant
            FROM src_orders""");

    stmts.addInsertSql(
        """
            INSERT INTO sink_customers
            SELECT id, name, email, 'sql-api' AS job_variant
            FROM src_customers""");

    stmts.execute();
  }
}
