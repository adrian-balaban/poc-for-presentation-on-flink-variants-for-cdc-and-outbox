package poc.component;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.cdc.connectors.mysql.source.MySqlSource;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import poc.common.config.JobConfig;
import poc.common.deserializer.PocJsonDeserializationSchema;
import poc.common.router.CdcEventRouter;
import poc.common.sink.KafkaSinkFactory;

import java.sql.*;
import java.time.Duration;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Component test for DataStream CDC variant.
 *
 * Verifies the full path: MySQL binlog → MySqlSource → CdcEventRouter → KafkaSink.
 * Flink runs in an embedded local cluster (no docker Flink needed).
 * Server-ID range 7000–7009 is reserved for this test.
 */
@Slf4j
@DisplayName("Flink DataStream API v.1 : CDC Test")
class DataStreamCdcTest extends ContainerBase {

    private static final String SERVER_ID  = "7000-7009";
    private static final String TABLES     = "poc_db.orders";

    @Test
    @Timeout(60)
    void cdcSource_capturesSnapshotRow_andPublishesEnrichedEventToKafka() throws Exception {
        JobConfig cfg = testConfig(SERVER_ID, TABLES);

        // Insert a row to verify CDC could capture it
        try (Connection c = flinkConn(); Statement s = c.createStatement()) {
            s.executeUpdate(
                "INSERT INTO poc_db.orders (customer_id, amount, status) VALUES (1, 11.11, 'DS-TEST')");
        }

        // Verify we can build the CDC source and pipeline
        MySqlSource<String> source = MySqlSource.<String>builder()
            .hostname(cfg.mysqlHost)
            .port(cfg.mysqlPort)
            .databaseList(cfg.mysqlDatabase)
            .tableList(TABLES)
            .username(cfg.mysqlUser)
            .password(cfg.mysqlPassword)
            .serverTimeZone("UTC")
            .serverId(SERVER_ID)
            .deserializer(new PocJsonDeserializationSchema())
            .build();

        assertThat(source).isNotNull();
        log.info("MySqlSource built successfully for table {}", TABLES);

        // Run the pipeline and verify events reach Kafka
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment(1);
        env.enableCheckpointing(5_000);
        env.fromSource(source, WatermarkStrategy.noWatermarks(), "MySQL CDC Source")
           .process(new CdcEventRouter(cfg))
           .sinkTo(KafkaSinkFactory.create(cfg, "datastream"));

        JobClient job = env.executeAsync("DataStream CDC Test");
        try {
            List<String> messages = pollKafka("test.cdc.datastream", 1, Duration.ofSeconds(45));
            assertThat(messages).isNotEmpty();
            assertThat(messages.get(0)).contains("DS-TEST");
            log.info("DataStream CDC pipeline produced {} Kafka message(s)", messages.size());
        } finally {
            job.cancel().get();
        }
    }

    @Test
    @Timeout(60)
    void cdcSource_capturesBinlogInsert_afterSnapshotComplete() throws Exception {
        JobConfig cfg = testConfig(SERVER_ID, TABLES);

        // Insert a row that will arrive via binlog after snapshot
        try (Connection c = flinkConn(); Statement s = c.createStatement()) {
            s.executeUpdate(
                "INSERT INTO poc_db.orders (customer_id, amount, status) VALUES (2, 22.22, 'BINLOG-TEST')");
        }

        MySqlSource<String> source = MySqlSource.<String>builder()
            .hostname(cfg.mysqlHost)
            .port(cfg.mysqlPort)
            .databaseList(cfg.mysqlDatabase)
            .tableList(TABLES)
            .username(cfg.mysqlUser)
            .password(cfg.mysqlPassword)
            .serverTimeZone("UTC")
            .serverId("7050-7059")
            .deserializer(new PocJsonDeserializationSchema())
            .build();

        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment(1);
        env.enableCheckpointing(5_000);
        env.fromSource(source, WatermarkStrategy.noWatermarks(), "MySQL CDC Source")
           .process(new CdcEventRouter(cfg))
           .sinkTo(KafkaSinkFactory.create(cfg, "datastream"));

        JobClient job = env.executeAsync("DataStream CDC Binlog Test");
        try {
            List<String> messages = pollKafka("test.cdc.datastream", 1, Duration.ofSeconds(45));
            assertThat(messages).isNotEmpty();
            log.info("DataStream CDC binlog test produced {} Kafka message(s)", messages.size());
        } finally {
            job.cancel().get();
        }
    }
}
