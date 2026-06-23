package poc.kafka.connect;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.connect.source.SourceRecord;
import org.json.JSONObject;
import org.junit.Before;
import org.junit.Test;

public class EnrichmentTransformTest {

  private EnrichmentTransform<SourceRecord> transform;

  @Before
  public void setup() {
    transform = new EnrichmentTransform<>();

    Map<String, Object> config = new HashMap<>();
    config.put("variant.name", "test-variant");
    config.put("topic.prefix", "poc.test");
    transform.configure(config);
  }

  @Test
  public void testEnrichJsonString() {
    String json = "{\"before\":null,\"after\":{\"id\":1,\"name\":\"test\"},\"table\":\"orders\"}";
    SourceRecord record =
        new SourceRecord(null, null, "input-topic", 0, null, null, null, json, null);

    SourceRecord result = transform.apply(record);
    String enriched = (String) result.value();

    JSONObject obj = new JSONObject(enriched);
    assertEquals("test-variant", obj.getString("variant"));
    assertTrue(obj.getString("topic").contains("poc.test"));
    assertTrue(obj.has("transformed_at"));
  }

  @Test
  public void testEnrichMapValue() {
    Map<String, Object> value = new HashMap<>();
    value.put("id", 1);
    value.put("name", "test");

    SourceRecord record =
        new SourceRecord(null, null, "input-topic", 0, null, null, null, value, null);

    SourceRecord result = transform.apply(record);
    Map<String, Object> enriched = (Map<String, Object>) result.value();

    assertEquals("test-variant", enriched.get("variant"));
    assertTrue(enriched.get("topic").toString().contains("poc.test"));
    assertTrue(enriched.containsKey("transformed_at"));
  }
}
