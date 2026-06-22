package poc.component;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.util.Collector;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import poc.common.config.JobConfig;
import poc.common.router.CdcEventRouter;

/**
 * Local unit tests for DataStream CDC variant routing logic using MiniCluster.
 *
 * <p>Fast feedback during development without needing external services. Tests core CDC
 * transformation: event enrichment and topic routing.
 *
 * <p>For full end-to-end testing with MySQL CDC source and Kafka sink, use DataStreamCdcTest.
 */
@Slf4j
@DisplayName("Flink DataStream API v.1 : CDC Routing Tests")
class DataStreamCdcMiniClusterTest extends MiniClusterTestBase {

  static class ListCollector<T> implements Collector<T> {
    final List<T> out = new ArrayList<>();

    @Override
    public void collect(T record) {
      out.add(record);
    }

    @Override
    public void close() {}
  }

  @Test
  @Timeout(10)
  void cdcRouter_enrichesEventWithVariantAndTopic() throws Exception {
    JobConfig config =
        new JobConfig.Builder()
            .mysqlHost("localhost")
            .mysqlPort(3306)
            .mysqlUser("test")
            .mysqlPassword("test")
            .mysqlDatabase("test")
            .mysqlTables("test.t")
            .kafkaBootstrap("localhost:9092")
            .kafkaTopicPrefix("poc.cdc")
            .serverId("1-100")
            .build();

    CdcEventRouter router = new CdcEventRouter(config);
    ListCollector<String> collector = new ListCollector<>();

    router.processElement("{\"id\":1,\"name\":\"Alice\"}", null, collector);
    router.processElement("{\"id\":2,\"name\":\"Bob\"}", null, collector);

    assertThat(collector.out).hasSize(2);
    assertThat(collector.out).allMatch(msg -> msg.contains("\"variant\":\"datastream-cdc\""));
    assertThat(collector.out).allMatch(msg -> msg.contains("\"topic\":\"poc.cdc.datastream.flink\""));
    log.info("Enriched {} events successfully", collector.out.size());
  }

  @Test
  @Timeout(10)
  void cdcRouter_handlesEdgeCases() throws Exception {
    JobConfig config =
        new JobConfig.Builder()
            .mysqlHost("localhost")
            .mysqlPort(3306)
            .mysqlUser("test")
            .mysqlPassword("test")
            .mysqlDatabase("test")
            .mysqlTables("test.t")
            .kafkaBootstrap("localhost:9092")
            .kafkaTopicPrefix("test")
            .serverId("1-100")
            .build();

    CdcEventRouter router = new CdcEventRouter(config);
    ListCollector<String> collector = new ListCollector<>();

    // Valid empty object
    router.processElement("{}", null, collector);
    // Edge case: brace at start (lastIndexOf finds it)
    router.processElement("}", null, collector);
    // Malformed: no closing brace (passed through unchanged)
    router.processElement("{\"x\":1", null, collector);

    assertThat(collector.out).hasSize(3);
    // First two should be enriched (have closing brace)
    assertThat(collector.out.get(0)).contains("\"variant\":\"datastream-cdc\"");
    assertThat(collector.out.get(1)).contains("\"variant\":\"datastream-cdc\"");
    // Last one is malformed, passed through unchanged
    assertThat(collector.out.get(2)).isEqualTo("{\"x\":1");
    log.info("Processed {} messages with edge cases", collector.out.size());
  }

  @Test
  @Timeout(10)
  void cdcRouter_usesTopicPrefixFromConfig() throws Exception {
    JobConfig config =
        new JobConfig.Builder()
            .mysqlHost("localhost")
            .mysqlPort(3306)
            .mysqlUser("test")
            .mysqlPassword("test")
            .mysqlDatabase("test")
            .mysqlTables("test.t")
            .kafkaBootstrap("localhost:9092")
            .kafkaTopicPrefix("custom.prefix")
            .serverId("1-100")
            .build();

    CdcEventRouter router = new CdcEventRouter(config);
    ListCollector<String> collector = new ListCollector<>();

    router.processElement("{\"data\":\"test\"}", null, collector);

    assertThat(collector.out).hasSize(1);
    assertThat(collector.out.get(0)).contains("custom.prefix.datastream.flink");
    log.info("Topic routing verified");
  }
}
