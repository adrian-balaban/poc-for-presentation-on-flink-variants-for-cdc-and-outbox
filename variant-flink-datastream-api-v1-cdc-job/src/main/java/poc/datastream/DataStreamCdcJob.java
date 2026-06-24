package poc.datastream;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.source.Source;
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
 * <p>Reads MySQL binlog via Flink CDC MySqlSource, applies per-row routing through CdcEventRouter,
 * and writes to Kafka. Gives full Java control over enrichment and topic routing — the most
 * flexible of the five variants.
 *
 * <p>Server-ID range 5900-5999 is reserved for this variant (see podman-compose.yml).
 */
public class DataStreamCdcJob {

  public static void main(String[] args) throws Exception {
    JobConfig config = JobConfig.fromEnv();

    MySqlSource<String> source =
        MySqlSource.<String>builder()
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
    CheckpointConfigurer.applyExactlyOnce(env);

    buildPipeline(env, source, KafkaSinkFactory.create(config, "datastream.orders"), config);
    env.execute("Flink DataStream API v.1 CDC Job");
  }

  /**
   * Wires source → CdcEventRouter → sink on the given environment.
   *
   * <p>Extracted so tests can inject a bounded in-memory source and a collecting sink instead of
   * the real MySqlSource + KafkaSink, exercising the full operator graph in a local MiniCluster.
   */
  public static void buildPipeline(
      StreamExecutionEnvironment env,
      Source<String, ?, ?> source,
      Sink<String> sink,
      JobConfig config) {
    env.fromSource(source, WatermarkStrategy.noWatermarks(), "MySQL CDC Source")
        .process(new CdcEventRouter(config))
        .sinkTo(sink);
  }
}
