package poc.datastream;

import static org.junit.jupiter.api.Assertions.*;

import java.util.ArrayList;
import java.util.List;
import org.apache.flink.util.Collector;
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
    CdcEventRouter router = new CdcEventRouter(config("poc.cdc"));
    ListCollector<String> collector = new ListCollector<>();

    router.processElement("{\"id\":1}", null, collector);

    assertEquals(1, collector.out.size());
    String enriched = collector.out.get(0);
    assertTrue(enriched.contains("\"variant\":\"datastream-cdc\""), "missing variant tag");
    assertTrue(enriched.contains("\"topic\":\"poc.cdc.datastream\""), "missing topic tag");
  }

  @Test
  void processElement_replacesClosingBrace() throws Exception {
    CdcEventRouter router = new CdcEventRouter(config("p"));
    ListCollector<String> collector = new ListCollector<>();

    router.processElement("{\"x\":\"y\"}", null, collector);

    String enriched = collector.out.get(0);
    // Original closing brace replaced; result must still be valid-looking JSON object
    assertTrue(enriched.endsWith("}"), "must end with closing brace");
    assertFalse(enriched.contains("\"x\":\"y\"}\"variant\""), "brace not replaced cleanly");
  }

  @Test
  void processElement_usesTopicPrefixFromConfig() throws Exception {
    CdcEventRouter router = new CdcEventRouter(config("custom.prefix"));
    ListCollector<String> collector = new ListCollector<>();

    router.processElement("{}", null, collector);

    assertTrue(collector.out.get(0).contains("custom.prefix.datastream"));
  }

  @Test
  void processElement_handlesBraceAtStartOfString() throws Exception {
    // lastIndexOf('}') == 0 — validates CONDITIONALS_BOUNDARY mutation on lastBrace < 0
    CdcEventRouter router = new CdcEventRouter(config("p"));
    ListCollector<String> collector = new ListCollector<>();

    router.processElement("}", null, collector);

    String enriched = collector.out.get(0);
    assertTrue(
        enriched.contains("\"variant\":\"datastream-cdc\""),
        "brace at index 0 must still be replaced");
  }

  @Test
  void processElement_emitsExactlyOneRecord() throws Exception {
    CdcEventRouter router = new CdcEventRouter(config("p"));
    ListCollector<String> collector = new ListCollector<>();

    router.processElement("{\"a\":1}", null, collector);

    assertEquals(1, collector.out.size());
  }
}
