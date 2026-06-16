package poc.datastream;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.cdc.connectors.mysql.source.MySqlSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import poc.common.checkpoint.CheckpointConfigurer;
import poc.common.config.JobConfig;
import poc.common.deserializer.PocJsonDeserializationSchema;
import poc.common.router.CdcEventRouter;
import poc.common.sink.KafkaSinkFactory;

/**
 * Variant 1 — DataStream CDC
 *
 * Reads MySQL binlog via Flink CDC MySqlSource, applies per-row routing through
 * CdcEventRouter, and writes to Kafka. Gives full Java control over enrichment
 * and topic routing — the most flexible of the five variants.
 *
 * Server-ID range 5900-5999 is reserved for this variant (see docker-compose.yml).
 */
public class DataStreamCdcJob {

    public static void main(String[] args) throws Exception {
        JobConfig config = JobConfig.fromEnv();

        MySqlSource<String> source = MySqlSource.<String>builder()
            .hostname(config.mysqlHost)
            .port(config.mysqlPort)
            .databaseList(config.mysqlDatabase)
            .tableList(config.mysqlTables)
            .username(config.mysqlUser)
            .password(config.mysqlPassword)
            .serverTimeZone("UTC")
            .serverId(config.serverId)
            .deserializer(new PocJsonDeserializationSchema())
            .build();

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Checkpoint configuration for exactly-once CDC semantics
        CheckpointConfigurer.applyExactlyOnce(env);

        env.fromSource(source, WatermarkStrategy.noWatermarks(), "MySQL CDC Source")
           .process(new CdcEventRouter(config))
           .sinkTo(KafkaSinkFactory.create(config, "datastream"));

        env.execute("Flink DataStream API v.1 CDC Job");
    }
}
