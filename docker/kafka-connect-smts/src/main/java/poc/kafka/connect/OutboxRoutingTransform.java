package poc.kafka.connect;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.transforms.Transformation;
import org.json.JSONObject;

import java.util.Map;

/**
 * Routes outbox events to topic based on the 'destination' field.
 *
 * For outbox pattern: reads from outbox_events table, extracts the
 * 'destination' field from the payload, and routes each event to
 * a topic like "poc.cdc.outbox.<destination>".
 *
 * Configuration:
 *   - topic.prefix: Base topic prefix (e.g., "poc.cdc.outbox")
 *   - destination.field: Field name containing the destination (default: "destination")
 */
public class OutboxRoutingTransform<R extends ConnectRecord<R>> implements Transformation<R> {

    private static final String TOPIC_PREFIX = "topic.prefix";
    private static final String DESTINATION_FIELD = "destination.field";
    private static final String DEFAULT_DESTINATION = "unknown";

    private String topicPrefix;
    private String destinationField;

    @Override
    public void configure(Map<String, ?> configs) {
        topicPrefix = (String) configs.getOrDefault(TOPIC_PREFIX, "poc.cdc.outbox");
        destinationField = (String) configs.getOrDefault(DESTINATION_FIELD, "destination");
    }

    @Override
    public R apply(R record) {
        String destination = extractDestination(record.value());
        String targetTopic = topicPrefix + "." + destination;

        return record.newRecord(
            targetTopic,  // Route to destination-specific topic
            null,         // Partition: let Kafka decide
            record.keySchema(),
            record.key(),
            record.valueSchema(),
            addRoutingMetadata(record.value(), destination, targetTopic),
            record.timestamp()
        );
    }

    private String extractDestination(Object value) {
        if (value instanceof String) {
            try {
                JSONObject obj = new JSONObject((String) value);
                // For CDC events, payload is nested under 'after'
                if (obj.has("after")) {
                    JSONObject after = obj.getJSONObject("after");
                    return after.optString(destinationField, DEFAULT_DESTINATION);
                }
                // Fallback to top-level field
                return obj.optString(destinationField, DEFAULT_DESTINATION);
            } catch (Exception e) {
                return DEFAULT_DESTINATION;
            }
        } else if (value instanceof Map) {
            Map<String, Object> map = (Map<String, Object>) value;
            Object afterObj = map.get("after");
            if (afterObj instanceof Map) {
                return ((Map<String, Object>) afterObj).getOrDefault(destinationField, DEFAULT_DESTINATION).toString();
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
                return value;
            }
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
            .define(TOPIC_PREFIX, ConfigDef.Type.STRING, "poc.cdc.outbox", ConfigDef.Importance.HIGH,
                "Topic prefix for routed outbox events")
            .define(DESTINATION_FIELD, ConfigDef.Type.STRING, "destination", ConfigDef.Importance.MEDIUM,
                "Field name containing the destination in the payload");
    }

    @Override
    public void close() {
    }
}
