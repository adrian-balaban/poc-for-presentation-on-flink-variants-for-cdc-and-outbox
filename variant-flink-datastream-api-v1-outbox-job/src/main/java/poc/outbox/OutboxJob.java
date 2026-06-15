package poc.outbox;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.cdc.connectors.mysql.source.MySqlSource;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import poc.common.config.JobConfig;
import poc.common.deserializer.PocJsonDeserializationSchema;
import poc.common.router.OutboxRouter;
import poc.common.sink.KafkaSinkFactory;

/**
 * Variant 4 — Outbox (DataStream)
 *
 * Reads from a transactional outbox table. Each row carries a destination topic
 * in the payload; OutboxRouter fans each event out to its correct Kafka topic.
 * Only DataStream supports per-row dynamic topic routing — Table/SQL API cannot.
 *
 * Server-ID range 5600-5699 is reserved for this variant.
 */
public class OutboxJob {

    public static void main(String[] args) throws Exception {
        JobConfig config = JobConfig.fromEnv();

        MySqlSource<String> source = MySqlSource.<String>builder()
            .hostname(config.mysqlHost)
            .port(config.mysqlPort)
            .databaseList(config.mysqlDatabase)
            .tableList(config.mysqlDatabase + ".outbox_events")
            .username(config.mysqlUser)
            .password(config.mysqlPassword)
            .serverTimeZone("UTC")
            .serverId(config.outboxServerId)
            .deserializer(new PocJsonDeserializationSchema())
            .build();

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Checkpoint configuration for exactly-once CDC semantics
        env.enableCheckpointing(30_000);
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        env.getCheckpointConfig().setCheckpointTimeout(60_000);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(5_000);

        DataStream<String> outboxStream =
            env.fromSource(source, WatermarkStrategy.noWatermarks(), "MySQL Outbox Source");

        outboxStream
            .process(new OutboxRouter(config)).name("outbox-router")
            .sinkTo(KafkaSinkFactory.create(config, "outbox")).name("kafka-outbox-sink");

        env.execute("Flink DataStream API v.1 Outbox Job");
    }
}
