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
 * Enriches each CDC event with variant metadata (variant name, topic, timestamp) and renames the
 * Kafka topic to {topicPrefix}.{tableName} (stripping the database prefix that Debezium adds by
 * default: {prefix}.{db}.{table} → {prefix}.{table}).
 *
 * <p>Configuration: - variant.name: Name of the variant (e.g., "datastream-cdc", "outbox-cdc") -
 * topic.prefix: Kafka topic prefix (e.g., "poc.kc.datastream")
 */
public class EnrichmentTransform<R extends ConnectRecord<R>> implements Transformation<R> {

  private static final Logger log = LoggerFactory.getLogger(EnrichmentTransform.class);

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
      String jsonValue = (String) record.value();
      String table = extractTableFromJson(jsonValue);
      String newTopic = topicPrefix + "." + table;
      String enriched = enrichJson(jsonValue, newTopic);
      return record.newRecord(
          newTopic,
          record.kafkaPartition(),
          record.keySchema(),
          record.key(),
          record.valueSchema(),
          enriched,
          record.timestamp());

    } else if (record.value() instanceof Struct) {
      Struct struct = (Struct) record.value();
      String table = extractTableFromStruct(struct);
      String newTopic = topicPrefix + "." + table;
      Struct enriched = enrichStruct(struct, newTopic);
      return record.newRecord(
          newTopic,
          record.kafkaPartition(),
          record.keySchema(),
          record.key(),
          enriched.schema(),
          enriched,
          record.timestamp());

    } else if (record.value() instanceof Map) {
      Map<String, Object> value = (Map<String, Object>) record.value();
      String table = extractTableFromMap(value);
      String newTopic = topicPrefix + "." + table;
      value.put("variant", variantName);
      value.put("topic", newTopic);
      value.put("transformed_at", System.currentTimeMillis());
      return record.newRecord(
          newTopic,
          record.kafkaPartition(),
          record.keySchema(),
          record.key(),
          record.valueSchema(),
          value,
          record.timestamp());
    }
    return record;
  }

  private String extractTableFromJson(String json) {
    try {
      JSONObject obj = new JSONObject(json);
      JSONObject source = obj.optJSONObject("source");
      if (source != null) return source.optString("table", "unknown");
      return obj.optString("table", "unknown");
    } catch (Exception e) {
      return "unknown";
    }
  }

  private String extractTableFromStruct(Struct struct) {
    try {
      Struct source = (Struct) struct.get("source");
      if (source != null) return (String) source.get("table");
    } catch (Exception e) {
      log.warn("Failed to extract table from Struct: {}", e.getMessage());
    }
    return "unknown";
  }

  private String extractTableFromMap(Map<String, Object> map) {
    Object source = map.get("source");
    if (source instanceof Map) {
      Object table = ((Map<?, ?>) source).get("table");
      if (table != null) return table.toString();
    }
    Object table = map.get("table");
    return table != null ? table.toString() : "unknown";
  }

  private String enrichJson(String json, String newTopic) {
    try {
      JSONObject obj = new JSONObject(json);
      obj.put("variant", variantName);
      obj.put("topic", newTopic);
      obj.put("transformed_at", System.currentTimeMillis());
      return obj.toString();
    } catch (Exception e) {
      log.error(
          "Failed to enrich JSON for topic {}: {} — passing record through un-enriched",
          newTopic,
          e.getMessage());
      return json;
    }
  }

  private Struct enrichStruct(Struct original, String newTopic) {
    Schema originalSchema = original.schema();
    SchemaBuilder builder = SchemaBuilder.struct();
    for (org.apache.kafka.connect.data.Field field : originalSchema.fields()) {
      builder.field(field.name(), field.schema());
    }
    builder.field("variant", Schema.OPTIONAL_STRING_SCHEMA);
    builder.field("topic", Schema.OPTIONAL_STRING_SCHEMA);
    builder.field("transformed_at", Schema.OPTIONAL_INT64_SCHEMA);
    Schema newSchema = builder.build();

    Struct enriched = new Struct(newSchema);
    for (org.apache.kafka.connect.data.Field field : originalSchema.fields()) {
      enriched.put(field.name(), original.get(field));
    }
    enriched.put("variant", variantName);
    enriched.put("topic", newTopic);
    enriched.put("transformed_at", System.currentTimeMillis());
    return enriched;
  }

  @Override
  public ConfigDef config() {
    return new ConfigDef()
        .define(
            VARIANT_NAME,
            ConfigDef.Type.STRING,
            null,
            ConfigDef.Importance.HIGH,
            "Name of the CDC variant (e.g., datastream-cdc, outbox-cdc)")
        .define(
            TOPIC_PREFIX,
            ConfigDef.Type.STRING,
            null,
            ConfigDef.Importance.HIGH,
            "Kafka topic prefix for this variant");
  }

  @Override
  public void close() {}
}
