package poc.datastream;

import static org.junit.jupiter.api.Assertions.*;

import java.util.ArrayList;
import java.util.List;
import org.apache.flink.util.Collector;
import org.json.JSONObject;
import org.junit.jupiter.api.Test;
import poc.common.config.JobConfig;
import poc.common.router.CdcEventRouter;

class CdcEventRouterTest {

  private static JobConfig config(String topicPrefix) {
    return new JobConfig.Builder()
        .mysqlHost("h")
        .mysqlPort(3306)
        .mysqlUser("u")
        .mysqlPassword("p")
        .mysqlDatabase("db")
        .mysqlTables("db.t")
        .kafkaBootstrap("k:9092")
        .kafkaTopicPrefix(topicPrefix)
        .serverId("1-9")
        .build();
  }

  private static class ListCollector<T> implements Collector<T> {
    final List<T> out = new ArrayList<>();

    @Override
    public void collect(T record) {
      out.add(record);
    }

    @Override
    public void close() {}
  }

  @Test
  void processElement_enrichesEventWithVariantAndTopic() throws Exception {
    CdcEventRouter router = new CdcEventRouter(config("poc.flink"));
    ListCollector<String> collector = new ListCollector<>();

    router.processElement("{\"id\":1}", null, collector);

    assertEquals(1, collector.out.size());
    JSONObject enriched = new JSONObject(collector.out.get(0));
    assertEquals("datastream-cdc", enriched.getString("variant"));
    assertEquals("poc.flink.datastream.orders", enriched.getString("topic"));
    assertEquals(1, enriched.getInt("id"));
  }

  @Test
  void processElement_roundTripsValidJson() throws Exception {
    // The router re-serialises valid JSON via org.json, preserving original fields
    // and appending the variant/topic tags. The output must be valid JSON (not a
    // hand-spliced string), so a JSONObject round-trip must succeed and keep the
    // source field intact.
    CdcEventRouter router = new CdcEventRouter(config("p"));
    ListCollector<String> collector = new ListCollector<>();

    router.processElement("{\"x\":\"y\"}", null, collector);

    JSONObject enriched = new JSONObject(collector.out.get(0));
    assertEquals("y", enriched.getString("x"));
    assertEquals("datastream-cdc", enriched.getString("variant"));
    assertEquals("p.datastream.orders", enriched.getString("topic"));
  }

  @Test
  void processElement_usesTopicPrefixFromConfig() throws Exception {
    CdcEventRouter router = new CdcEventRouter(config("custom.prefix"));
    ListCollector<String> collector = new ListCollector<>();

    router.processElement("{}", null, collector);

    JSONObject enriched = new JSONObject(collector.out.get(0));
    assertEquals("custom.prefix.datastream.orders", enriched.getString("topic"));
  }

  @Test
  void processElement_forwardsMalformedJsonUnchanged() throws Exception {
    // A lone "}" is malformed JSON (org.json throws JSONException), so the router
    // forwards it unchanged rather than crashing the job. This also covers the
    // lastIndexOf('}') == 0 boundary that used to be exercised by the old
    // string-splicing implementation.
    CdcEventRouter router = new CdcEventRouter(config("p"));
    ListCollector<String> collector = new ListCollector<>();

    router.processElement("}", null, collector);

    assertEquals(1, collector.out.size());
    assertEquals("}", collector.out.get(0));
  }

  @Test
  void processElement_emitsExactlyOneRecord() throws Exception {
    CdcEventRouter router = new CdcEventRouter(config("p"));
    ListCollector<String> collector = new ListCollector<>();

    router.processElement("{\"a\":1}", null, collector);

    assertEquals(1, collector.out.size());
  }
}
