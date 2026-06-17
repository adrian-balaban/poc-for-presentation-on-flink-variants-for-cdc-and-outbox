package poc.kafka.connect;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.connect.source.SourceRecord;
import org.json.JSONObject;
import org.junit.Before;
import org.junit.Test;

public class OutboxRoutingTransformTest {

  private OutboxRoutingTransform<SourceRecord> transform;

  @Before
  public void setup() {
    transform = new OutboxRoutingTransform<>();

    Map<String, Object> config = new HashMap<>();
    config.put("topic.prefix", "poc.outbox");
    config.put("destination.field", "destination");
    transform.configure(config);
  }

  @Test
  public void testRouteByDestination() {
    String json = "{\"after\":{\"id\":1,\"destination\":\"orders-svc\",\"payload\":\"test\"}}";
    SourceRecord record =
        new SourceRecord(null, null, "outbox_events", 0, null, null, null, json, null);

    SourceRecord result = transform.apply(record);

    // Topic should be routed
    assertEquals("poc.outbox.orders-svc", result.topic());

    // Value should have routing metadata
    String enriched = (String) result.value();
    JSONObject obj = new JSONObject(enriched);
    assertEquals("orders-svc", obj.getString("_route_destination"));
    assertEquals("poc.outbox.orders-svc", obj.getString("_route_topic"));
    assertTrue(obj.has("_routed_at"));
  }

  @Test
  public void testUnknownDestination() {
    String json = "{\"after\":{\"id\":1,\"payload\":\"test\"}}";
    SourceRecord record =
        new SourceRecord(null, null, "outbox_events", 0, null, null, null, json, null);

    SourceRecord result = transform.apply(record);

    // Should route to .unknown for missing destination
    assertEquals("poc.outbox.unknown", result.topic());
  }
}
