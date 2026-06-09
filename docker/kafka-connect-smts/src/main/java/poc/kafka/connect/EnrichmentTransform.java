package poc.kafka.connect;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.transforms.Transformation;
import org.json.JSONObject;

import java.util.Map;

/**
 * Enriches each CDC event with variant metadata (variant name, topic, timestamp).
 *
 * Configuration:
 *   - variant.name: Name of the variant (e.g., "datastream-cdc", "outbox-cdc")
 *   - topic.prefix: Kafka topic prefix (e.g., "poc.cdc.datastream")
 */
public class EnrichmentTransform<R extends ConnectRecord<R>> implements Transformation<R> {

    private static final String VARIANT_NAME = "variant.name";
    private static final String TOPIC_PREFIX = "topic.prefix";

    private String variantName;
    private String topicPrefix;

    @Override
    public void configure(Map<String, ?> configs) {
        variantName = (String) configs.get(VARIANT_NAME);
        topicPrefix = (String) configs.get(TOPIC_PREFIX);
    }

    @Override
    public R apply(R record) {
        if (record.value() instanceof String) {
            // JSON string value (Debezium default)
            String jsonValue = (String) record.value();
            String enriched = enrichJson(jsonValue);
            return record.newRecord(
                record.topic(),
                record.kafkaPartition(),
                record.keySchema(),
                record.key(),
                record.valueSchema(),
                enriched,
                record.timestamp()
            );
        } else if (record.value() instanceof Map) {
            // Map value
            Map<String, Object> value = (Map<String, Object>) record.value();
            value.put("variant", variantName);
            value.put("topic", topicPrefix + "." + record.topic());
            value.put("transformed_at", System.currentTimeMillis());
            return record;
        }
        return record;
    }

    private String enrichJson(String json) {
        try {
            JSONObject obj = new JSONObject(json);
            obj.put("variant", variantName);
            obj.put("topic", topicPrefix + "." + obj.optString("table", "unknown"));
            obj.put("transformed_at", System.currentTimeMillis());
            return obj.toString();
        } catch (Exception e) {
            // Return original if JSON parsing fails
            return json;
        }
    }

    @Override
    public ConfigDef config() {
        return new ConfigDef()
            .define(VARIANT_NAME, ConfigDef.Type.STRING, null, ConfigDef.Importance.HIGH,
                "Name of the CDC variant (e.g., datastream-cdc, outbox-cdc)")
            .define(TOPIC_PREFIX, ConfigDef.Type.STRING, null, ConfigDef.Importance.HIGH,
                "Kafka topic prefix for this variant");
    }

    @Override
    public void close() {
    }
}
