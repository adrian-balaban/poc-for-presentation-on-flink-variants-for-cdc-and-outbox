package poc.kafka.connect;

import java.util.Map;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.transforms.Transformation;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Routes outbox events to topic based on the 'destination' field.
 *
 * <p>For outbox pattern: reads from outbox_events table, extracts the 'destination' field from the
 * payload, and routes each event to a topic like "poc.kc.outbox.<destination>".
 *
 * <p>Configuration: - topic.prefix: Base topic prefix (e.g., "poc.kc.outbox") - destination.field:
 * Field name containing the destination (default: "destination")
 */
public class OutboxRoutingTransform<R extends ConnectRecord<R>> implements Transformation<R> {

  private static final Logger log = LoggerFactory.getLogger(OutboxRoutingTransform.class);

  private static final String TOPIC_PREFIX = "topic.prefix";
  private static final String DESTINATION_FIELD = "destination.field";
  private static final String DEFAULT_DESTINATION = "unknown";

  private String topicPrefix;
  private String destinationField;

  @Override
  public void configure(Map<String, ?> configs) {
    Object prefix = configs.get(TOPIC_PREFIX);
    topicPrefix = prefix != null ? prefix.toString() : "poc.kc.outbox";
    Object field = configs.get(DESTINATION_FIELD);
    destinationField = field != null ? field.toString() : "destination";
  }

  @Override
  public R apply(R record) {
    String destination = extractDestination(record.value());
    String targetTopic = topicPrefix + "." + destination;
    Object enrichedValue = addRoutingMetadata(record.value(), destination, targetTopic);

    Schema newSchema =
        (enrichedValue instanceof Struct)
            ? ((Struct) enrichedValue).schema()
            : record.valueSchema();

    return record.newRecord(
        targetTopic,
        null,
        record.keySchema(),
        record.key(),
        newSchema,
        enrichedValue,
        record.timestamp());
  }

  private String extractDestination(Object value) {
    if (value instanceof String) {
      try {
        JSONObject obj = new JSONObject((String) value);
        if (obj.has("after")) {
          return obj.getJSONObject("after").optString(destinationField, DEFAULT_DESTINATION);
        }
        return obj.optString(destinationField, DEFAULT_DESTINATION);
      } catch (Exception e) {
        return DEFAULT_DESTINATION;
      }
    } else if (value instanceof Struct) {
      try {
        Struct struct = (Struct) value;
        Struct after = (Struct) struct.get("after");
        if (after != null) {
          Object dest = after.get(destinationField);
          return dest != null ? dest.toString() : DEFAULT_DESTINATION;
        }
        Object dest = struct.get(destinationField);
        return dest != null ? dest.toString() : DEFAULT_DESTINATION;
      } catch (Exception e) {
        return DEFAULT_DESTINATION;
      }
    } else if (value instanceof Map) {
      Map<String, Object> map = (Map<String, Object>) value;
      Object afterObj = map.get("after");
      if (afterObj instanceof Map) {
        return ((Map<String, Object>) afterObj)
            .getOrDefault(destinationField, DEFAULT_DESTINATION)
            .toString();
      }
      return map.getOrDefault(destinationField, DEFAULT_DESTINATION).toString();
    }
    return DEFAULT_DESTINATION;
  }

  private Object addRoutingMetadata(Object value, String destination, String topic) {
    if (value instanceof String) {
      try {
        JSONObject obj = new JSONObject((String) value);
        obj.put("_route_destination", destination);
        obj.put("_route_topic", topic);
        obj.put("_routed_at", System.currentTimeMillis());
        return obj.toString();
      } catch (Exception e) {
        log.error(
            "Failed to add routing metadata for topic {}: {} — passing record through un-routed",
            topic,
            e.getMessage());
        return value;
      }
    } else if (value instanceof Struct) {
      Struct original = (Struct) value;
      Schema originalSchema = original.schema();
      SchemaBuilder builder = SchemaBuilder.struct();
      for (org.apache.kafka.connect.data.Field field : originalSchema.fields()) {
        builder.field(field.name(), field.schema());
      }
      builder.field("_route_destination", Schema.OPTIONAL_STRING_SCHEMA);
      builder.field("_route_topic", Schema.OPTIONAL_STRING_SCHEMA);
      builder.field("_routed_at", Schema.OPTIONAL_INT64_SCHEMA);
      Schema newSchema = builder.build();

      Struct enriched = new Struct(newSchema);
      for (org.apache.kafka.connect.data.Field field : originalSchema.fields()) {
        enriched.put(field.name(), original.get(field));
      }
      enriched.put("_route_destination", destination);
      enriched.put("_route_topic", topic);
      enriched.put("_routed_at", System.currentTimeMillis());
      return enriched;
    } else if (value instanceof Map) {
      Map<String, Object> map = (Map<String, Object>) value;
      map.put("_route_destination", destination);
      map.put("_route_topic", topic);
      map.put("_routed_at", System.currentTimeMillis());
    }
    return value;
  }

  @Override
  public ConfigDef config() {
    return new ConfigDef()
        .define(
            TOPIC_PREFIX,
            ConfigDef.Type.STRING,
            "poc.kc.outbox",
            ConfigDef.Importance.HIGH,
            "Topic prefix for routed outbox events")
        .define(
            DESTINATION_FIELD,
            ConfigDef.Type.STRING,
            "destination",
            ConfigDef.Importance.MEDIUM,
            "Field name containing the destination in the payload");
  }

  @Override
  public void close() {}
}
