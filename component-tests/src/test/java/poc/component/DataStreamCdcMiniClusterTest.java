package poc.component;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import poc.common.config.JobConfig;
import poc.common.router.CdcEventRouter;

/**
 * Tests for DataStream CDC variant at two levels:
 *
 * <ul>
 *   <li><b>Unit tests</b> — call {@link CdcEventRouter#processElement} directly; no Flink runtime.
 *   <li><b>MiniCluster pipeline test</b> — runs the full source → CdcEventRouter → sink operator
 *       graph inside a local Flink environment ({@code createLocalEnvironment}), with a bounded
 *       {@code fromData} source and a {@link CollectingSink} — no MySQL, no Kafka required.
 * </ul>
 *
 * <p>For full end-to-end testing with MySQL CDC source and Kafka sink, use DataStreamCdcTest.
 */
@Slf4j
@DisplayName("Flink DataStream API v.1 : CDC Routing Tests")
class DataStreamCdcMiniClusterTest extends MiniClusterTestBase {

  private static JobConfig testJobConfig(String topicPrefix) {
    return new JobConfig.Builder()
        .mysqlHost("localhost")
        .mysqlPort(3306)
        .mysqlUser("test")
        .mysqlPassword("test")
        .mysqlDatabase("test")
        .mysqlTables("test.t")
        .kafkaBootstrap("localhost:9092")
        .kafkaTopicPrefix(topicPrefix)
        .serverId("1-100")
        .build();
  }

  // ── MiniCluster pipeline test ────────────────────────────────────────────────

  @BeforeEach
  void resetSink() {
    CollectingSink.reset();
  }

  /**
   * Exercises the full DataStream CDC operator graph in a local Flink environment.
   *
   * <p>This is the pattern from the Flink docs "Testing Flink Jobs" section: a bounded {@code
   * fromData} source replaces MySqlSource and {@link CollectingSink} replaces KafkaSink, so the
   * entire source → CdcEventRouter → sink graph runs in-process without MySQL or Kafka.
   */
  @Test
  @Timeout(30)
  void pipeline_enrichesAllEventsEndToEnd() throws Exception {
    JobConfig config = testJobConfig("poc.flink");

    env.fromData(
            "{\"id\":1,\"name\":\"Alice\"}",
            "{\"id\":2,\"name\":\"Bob\"}",
            "{\"id\":3,\"name\":\"Carol\"}")
        .process(new CdcEventRouter(config))
        .sinkTo(new CollectingSink());

    env.execute("MiniCluster pipeline test");

    List<String> results = CollectingSink.values();
    log.info("MiniCluster collected {} events: {}", results.size(), results);

    assertThat(results).hasSize(3);
    assertThat(results).allMatch(msg -> msg.contains("\"variant\":\"datastream-cdc\""));
    assertThat(results).allMatch(msg -> msg.contains("\"topic\":\"poc.flink.datastream.orders\""));
    assertThat(results).anyMatch(msg -> msg.contains("\"name\":\"Alice\""));
    assertThat(results).anyMatch(msg -> msg.contains("\"name\":\"Bob\""));
    assertThat(results).anyMatch(msg -> msg.contains("\"name\":\"Carol\""));
  }

  // ── Unit tests (CdcEventRouter in isolation, no Flink runtime) ──────────────

  @Test
  @Timeout(10)
  void cdcRouter_enrichesEventWithVariantAndTopic() throws Exception {
    CdcEventRouter router = new CdcEventRouter(testJobConfig("poc.flink"));
    ListCollector<String> collector = new ListCollector<>();

    router.processElement("{\"id\":1,\"name\":\"Alice\"}", null, collector);
    router.processElement("{\"id\":2,\"name\":\"Bob\"}", null, collector);

    assertThat(collector.out).hasSize(2);
    assertThat(collector.out).allMatch(msg -> msg.contains("\"variant\":\"datastream-cdc\""));
    assertThat(collector.out)
        .allMatch(msg -> msg.contains("\"topic\":\"poc.flink.datastream.orders\""));
    log.info("Enriched {} events successfully", collector.out.size());
  }

  @Test
  @Timeout(10)
  void cdcRouter_handlesEdgeCases() throws Exception {
    CdcEventRouter router = new CdcEventRouter(testJobConfig("test"));
    ListCollector<String> collector = new ListCollector<>();

    // Valid empty object — org.json accepts and emits a fully enriched event
    router.processElement("{}", null, collector);
    // Malformed lone "}" — org.json throws, router passes through unchanged
    // (previous StringBuilder hack happened to produce a substring-match for
    // "variant":… because it appended text after the lone brace, but the
    // resulting payload was not valid JSON. Pass-through is the safer contract.)
    router.processElement("}", null, collector);
    // Malformed: no closing brace (passed through unchanged)
    router.processElement("{\"x\":1", null, collector);

    assertThat(collector.out).hasSize(3);
    assertThat(collector.out.get(0)).contains("\"variant\":\"datastream-cdc\"");
    assertThat(collector.out.get(1)).isEqualTo("}");
    assertThat(collector.out.get(2)).isEqualTo("{\"x\":1");
    log.info("Processed {} messages with edge cases", collector.out.size());
  }

  @Test
  @Timeout(10)
  void cdcRouter_usesTopicPrefixFromConfig() throws Exception {
    CdcEventRouter router = new CdcEventRouter(testJobConfig("custom.prefix"));
    ListCollector<String> collector = new ListCollector<>();

    router.processElement("{\"data\":\"test\"}", null, collector);

    assertThat(collector.out).hasSize(1);
    assertThat(collector.out.get(0)).contains("custom.prefix.datastream.orders");
    log.info("Topic routing verified");
  }
}
