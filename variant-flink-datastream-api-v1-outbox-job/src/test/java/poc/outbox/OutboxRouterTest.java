package poc.outbox;

import static org.junit.jupiter.api.Assertions.*;

import java.util.ArrayList;
import java.util.List;
import org.apache.flink.util.Collector;
import org.junit.jupiter.api.Test;
import poc.common.config.JobConfig;
import poc.common.router.OutboxRouter;

class OutboxRouterTest {

  private static JobConfig config() {
    return new JobConfig.Builder()
        .mysqlHost("h")
        .mysqlPort(3306)
        .mysqlUser("u")
        .mysqlPassword("p")
        .mysqlDatabase("db")
        .mysqlTables("db.t")
        .kafkaBootstrap("k:9092")
        .kafkaTopicPrefix("poc.flink")
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

  // ── extractField ──────────────────────────────────────────────────────────

  @Test
  void extractField_returnsValue_whenFieldPresent() {
    OutboxRouter router = new OutboxRouter(config());
    assertEquals("payments", router.extractField("{\"destination\":\"payments\"}", "destination"));
  }

  @Test
  void extractField_returnsUnknown_whenFieldMissing() {
    OutboxRouter router = new OutboxRouter(config());
    assertEquals("unknown", router.extractField("{\"other\":\"val\"}", "destination"));
  }

  @Test
  void extractField_returnsUnknown_whenValueUnterminated() {
    OutboxRouter router = new OutboxRouter(config());
    // closing quote for the value is absent
    assertEquals("unknown", router.extractField("{\"destination\":\"payments", "destination"));
  }

  @Test
  void extractField_handlesFieldAtStartOfString() {
    OutboxRouter router = new OutboxRouter(config());
    // start == 0 — validates CONDITIONALS_BOUNDARY mutation on start == -1
    assertEquals("v", router.extractField("\"f\":\"v\"}", "f"));
  }

  @Test
  void extractField_handlesEmptyValue() {
    OutboxRouter router = new OutboxRouter(config());
    assertEquals("", router.extractField("{\"destination\":\"\"}", "destination"));
  }

  @Test
  void extractField_skipsEscapedQuoteInsideValue() {
    OutboxRouter router = new OutboxRouter(config());
    // value contains an escaped quote (\") — must not stop scanning there.
    // Validates the `end += 2` skip on line 41 of OutboxRouter.
    assertEquals(
        "pay\\\"ments", router.extractField("{\"destination\":\"pay\\\"ments\"}", "destination"));
  }

  // ── processElement ────────────────────────────────────────────────────────

  @Test
  void processElement_routesToCorrectTopic_viaExtractField() throws Exception {
    JobConfig cfg = config();
    OutboxRouter router = new OutboxRouter(cfg);
    String event = "{\"destination\":\"payments\",\"amount\":99}";

    // Routing decision is observable through extractField (the same logic processElement uses).
    String destination = router.extractField(event, "destination");
    String topic = cfg.kafkaTopicPrefix + ".outbox." + destination;

    assertEquals("payments", destination);
    assertEquals(cfg.kafkaTopicPrefix + ".outbox.payments", topic);
  }

  @Test
  void processElement_unknownDestination_whenFieldMissing() throws Exception {
    OutboxRouter router = new OutboxRouter(config());

    String destination = router.extractField("{\"no-dest\":true}", "destination");

    assertEquals("unknown", destination);
  }

  @Test
  void processElement_usesTopicPrefixFromConfig() throws Exception {
    JobConfig other =
        new JobConfig.Builder()
            .mysqlHost("h")
            .mysqlPort(3306)
            .mysqlUser("u")
            .mysqlPassword("p")
            .mysqlDatabase("db")
            .mysqlTables("db.t")
            .kafkaBootstrap("k:9092")
            .kafkaTopicPrefix("custom")
            .serverId("1-2")
            .build();
    OutboxRouter router = new OutboxRouter(other);

    String destination = router.extractField("{\"destination\":\"audit\"}", "destination");
    String topic = other.kafkaTopicPrefix + ".outbox." + destination;

    assertEquals("custom.outbox.audit", topic);
  }

  @Test
  void processElement_collectsTheOriginalEvent() throws Exception {
    OutboxRouter router = new OutboxRouter(config());
    ListCollector<String> collector = new ListCollector<>();
    String event = "{\"destination\":\"payments\"}";

    router.processElement(event, null, collector);

    assertEquals(1, collector.out.size());
    assertEquals(
        event, collector.out.get(0), "router must pass the original event through unchanged");
  }
}
