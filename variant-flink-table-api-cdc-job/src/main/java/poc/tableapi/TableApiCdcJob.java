package poc.tableapi;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import poc.common.checkpoint.CheckpointConfigurer;
import poc.common.config.JobConfig;

import static poc.common.validation.DdlValidator.requireSafeDdl;

/**
 * Variant 2 — Table API CDC
 *
 * Uses Flink's Table API with the mysql-cdc connector. Schema is declared in SQL DDL;
 * Flink handles deserialization automatically. Good for connectors that may grow
 * SQL joins or aggregations. ~220 lines in the real connector vs ~100 for DataStream.
 *
 * Server-ID range comes from {@code JobConfig#tableApiServerId}.
 */
public class TableApiCdcJob {

    public static void main(String[] args) {
        JobConfig config = JobConfig.fromEnv();
        requireSafeDdl(config.mysqlHost,        "MYSQL_HOST");
        requireSafeDdl(config.mysqlUser,        "MYSQL_USER");
        requireSafeDdl(config.mysqlPassword,    "MYSQL_PASSWORD");
        requireSafeDdl(config.mysqlDatabase,    "MYSQL_DATABASE");
        requireSafeDdl(config.kafkaBootstrap,   "KAFKA_BOOTSTRAP");
        requireSafeDdl(config.kafkaTopicPrefix, "KAFKA_TOPIC_PREFIX");
        requireSafeDdl(config.tableApiServerId, "tableApiServerId");

        StreamExecutionEnvironment    env      = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment        tableEnv = StreamTableEnvironment.create(env);

        // Checkpoint configuration for exactly-once CDC semantics
        CheckpointConfigurer.applyExactlyOnce(env);

        tableEnv.getConfig().getConfiguration().setString("pipeline.name", "Flink Table API CDC Job");

        // Source DDL — mysql-cdc connector reads the binlog directly
        tableEnv.executeSql(String.format("""
            CREATE TABLE mysql_orders (
                id          BIGINT,
                customer_id BIGINT,
                amount      DECIMAL(10,2),
                status      STRING,
                created_at  TIMESTAMP(3),
                PRIMARY KEY (id) NOT ENFORCED
            ) WITH (
                'connector'         = 'mysql-cdc',
                'hostname'          = '%s',
                'port'              = '%d',
                'username'          = '%s',
                'password'          = '%s',
                'database-name'     = '%s',
                'table-name'        = 'orders',
                'server-id'         = '%s',
                'server-time-zone'  = 'UTC'
            )
            """,
            config.mysqlHost, config.mysqlPort,
            config.mysqlUser, config.mysqlPassword,
            config.mysqlDatabase, config.tableApiServerId
        ));

        // Sink DDL — upsert-kafka writes the full row as JSON
        tableEnv.executeSql(String.format("""
            CREATE TABLE kafka_orders (
                id          BIGINT,
                customer_id BIGINT,
                amount      DECIMAL(10,2),
                status      STRING,
                created_at  TIMESTAMP(3),
                job_variant STRING,
                PRIMARY KEY (id) NOT ENFORCED
            ) WITH (
                'connector'                   = 'upsert-kafka',
                'topic'                       = '%s.table-api',
                'properties.bootstrap.servers'= '%s',
                'key.format'                  = 'json',
                'value.format'                = 'json'
            )
            """,
            config.kafkaTopicPrefix, config.kafkaBootstrap
        ));

        // Project + enrich, then insert into Kafka
        tableEnv.executeSql("""
            INSERT INTO kafka_orders
            SELECT id, customer_id, amount, status, created_at, 'table-api' AS job_variant
            FROM mysql_orders
            """);
    }
}
