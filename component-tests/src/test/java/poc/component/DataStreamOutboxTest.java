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
import poc.common.router.OutboxRouter;
import poc.common.sink.KafkaSinkFactory;

import java.sql.*;
import java.time.Duration;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Component test for Outbox variant.
 *
 * Verifies the end-to-end path: outbox_events table → CDC source → per-row routing → Kafka.
 * Server-ID range 7040–7049 is reserved for this test.
 */
@Slf4j
@DisplayName("Flink DataStream API v.1 : Outbox Test")
class DataStreamOutboxTest extends ContainerBase {

    private static final String SERVER_ID = "7040-7049";

    @Test
    @Timeout(60)
    void outboxSource_capturesOutboxEvents_andPublishesToKafka() throws Exception {
        JobConfig cfg = testConfig(SERVER_ID, "poc_db.outbox_events");

        // Insert test outbox event
        try (Connection c = flinkConn(); Statement s = c.createStatement()) {
            s.executeUpdate(
                "INSERT INTO poc_db.outbox_events (destination, payload) VALUES ('payments', '{\"order_id\":1}')");
        }

        // Verify we can build the outbox source
        MySqlSource<String> source = MySqlSource.<String>builder()
            .hostname(cfg.mysqlHost)
            .port(cfg.mysqlPort)
            .databaseList(cfg.mysqlDatabase)
            .tableList("poc_db.outbox_events")
            .username(cfg.mysqlUser)
            .password(cfg.mysqlPassword)
            .serverTimeZone("UTC")
            .serverId(SERVER_ID)
            .deserializer(new PocJsonDeserializationSchema())
            .build();

        // Run the pipeline and verify events reach Kafka
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment(1);
        env.enableCheckpointing(5_000);
        env.fromSource(source, WatermarkStrategy.noWatermarks(), "MySQL Outbox Source")
           .process(new OutboxRouter(cfg)).name("outbox-router")
           .sinkTo(KafkaSinkFactory.create(cfg, "outbox")).name("kafka-outbox-sink");

        JobClient job = env.executeAsync("Outbox CDC Test");
        try {
            List<String> messages = pollKafka("test.cdc.outbox", 1, Duration.ofSeconds(45));
            assertThat(messages).isNotEmpty();
            assertThat(messages.get(0)).contains("payments");
            log.info("Outbox CDC pipeline produced {} Kafka message(s)", messages.size());
        } finally {
            job.cancel().get();
        }
    }
}
