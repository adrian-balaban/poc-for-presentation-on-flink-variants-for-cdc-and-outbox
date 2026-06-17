package poc.common.router;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.List;
import org.apache.flink.util.Collector;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import poc.common.config.JobConfig;

class CdcEventRouterTest {

  private CdcEventRouter router;

  @BeforeEach
  void setup() {
    JobConfig config =
        new JobConfig.Builder()
            .mysqlHost("localhost")
            .mysqlPort(3306)
            .mysqlUser("test")
            .mysqlPassword("test")
            .mysqlDatabase("testdb")
            .mysqlTables("testdb.orders")
            .kafkaBootstrap("localhost:9092")
            .kafkaTopicPrefix("test.cdc")
            .serverId("5900-5999")
            .build();
    router = new CdcEventRouter(config);
  }

  @Test
  void enrichesEventWithVariantAnnotation() throws Exception {
    String input = "{\"id\":1,\"customer_id\":100,\"amount\":50.00}";
    List<String> output = processElement(input);

    assertThat(output).hasSize(1);
    assertThat(output.get(0)).contains("\"variant\":\"datastream-cdc\"");
    assertThat(output.get(0)).contains("\"topic\":\"test.cdc.datastream\"");
  }

  @Test
  void preservesOriginalJsonFields() throws Exception {
    String input = "{\"id\":1,\"customer_id\":100,\"amount\":50.00,\"status\":\"active\"}";
    List<String> output = processElement(input);

    assertThat(output.get(0)).contains("\"id\":1");
    assertThat(output.get(0)).contains("\"customer_id\":100");
    assertThat(output.get(0)).contains("\"amount\":50.00");
    assertThat(output.get(0)).contains("\"status\":\"active\"");
  }

  @Test
  void handlesEmptyJson() throws Exception {
    String input = "{}";
    List<String> output = processElement(input);

    assertThat(output).hasSize(1);
    assertThat(output.get(0)).contains("\"variant\":\"datastream-cdc\"");
  }

  @Test
  void handlesMalformedJsonWithoutClosingBrace() throws Exception {
    String input = "{\"id\":1,\"status\":\"incomplete\"";
    List<String> output = processElement(input);

    assertThat(output).hasSize(1);
    assertThat(output.get(0)).isEqualTo(input);
  }

  @Test
  void topicNameUsesConfigPrefix() throws Exception {
    JobConfig customConfig =
        new JobConfig.Builder()
            .mysqlHost("localhost")
            .mysqlPort(3306)
            .mysqlUser("test")
            .mysqlPassword("test")
            .mysqlDatabase("testdb")
            .mysqlTables("testdb.orders")
            .kafkaBootstrap("localhost:9092")
            .kafkaTopicPrefix("custom.prefix")
            .serverId("5900-5999")
            .build();
    CdcEventRouter customRouter = new CdcEventRouter(customConfig);

    String input = "{\"id\":1}";
    List<String> output = processElement(customRouter, input);

    assertThat(output.get(0)).contains("\"topic\":\"custom.prefix.datastream\"");
  }

  // ── Helpers ───────────────────────────────────────────────────────────────

  private List<String> processElement(String input) throws Exception {
    return processElement(router, input);
  }

  private List<String> processElement(CdcEventRouter r, String input) throws Exception {
    List<String> output = new ArrayList<>();
    Collector<String> collector =
        new Collector<>() {
          @Override
          public void collect(String record) {
            output.add(record);
          }

          @Override
          public void close() {}
        };
    r.processElement(input, null, collector);
    return output;
  }
}
