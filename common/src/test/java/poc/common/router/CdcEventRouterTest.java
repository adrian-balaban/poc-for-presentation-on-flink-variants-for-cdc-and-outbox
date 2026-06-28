package poc.common.router;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.ArrayList;
import java.util.List;
import org.apache.flink.util.Collector;
import org.json.JSONObject;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import poc.common.config.JobConfig;

/**
 * Unit tests for {@link CdcEventRouter}.
 *
 * <p>The router re-serialises valid JSON through org.json (appending {@code variant}/{@code topic}
 * tags), so assertions use {@link JSONObject} exact-field access rather than substring matching on
 * the raw string — org.json normalises number formatting (e.g. {@code 50.00} → {@code 50.0}), which
 * would break any {@code contains("50.00")} assertion. Malformed JSON is forwarded unchanged, so
 * for those cases the raw input is compared verbatim.
 */
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

    assertEquals(1, output.size());
    JSONObject enriched = new JSONObject(output.get(0));
    assertEquals("datastream-cdc", enriched.getString("variant"));
    assertEquals("test.cdc.datastream.orders", enriched.getString("topic"));
  }

  @Test
  void preservesOriginalJsonFields() throws Exception {
    String input = "{\"id\":1,\"customer_id\":100,\"amount\":50.00,\"status\":\"active\"}";
    List<String> output = processElement(input);

    JSONObject enriched = new JSONObject(output.get(0));
    assertEquals(1, enriched.getInt("id"));
    assertEquals(100, enriched.getInt("customer_id"));
    assertEquals(50.0, enriched.getDouble("amount"), 0.001);
    assertEquals("active", enriched.getString("status"));
  }

  @Test
  void handlesEmptyJson() throws Exception {
    String input = "{}";
    List<String> output = processElement(input);

    assertEquals(1, output.size());
    JSONObject enriched = new JSONObject(output.get(0));
    assertEquals("datastream-cdc", enriched.getString("variant"));
  }

  @Test
  void handlesMalformedJsonWithoutClosingBrace() throws Exception {
    String input = "{\"id\":1,\"status\":\"incomplete\"";
    List<String> output = processElement(input);

    assertEquals(1, output.size());
    // Malformed JSON is forwarded unchanged (org.json throws → catch → pass-through).
    assertEquals(input, output.get(0));
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

    JSONObject enriched = new JSONObject(output.get(0));
    assertEquals("custom.prefix.datastream.orders", enriched.getString("topic"));
  }

  @Test
  void forwardsMalformedJsonUnchanged() throws Exception {
    // A lone "}" is malformed JSON (org.json throws JSONException), so the router forwards it
    // unchanged rather than crashing the job.
    List<String> output = processElement("}");

    assertEquals(1, output.size());
    assertEquals("}", output.get(0));
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
