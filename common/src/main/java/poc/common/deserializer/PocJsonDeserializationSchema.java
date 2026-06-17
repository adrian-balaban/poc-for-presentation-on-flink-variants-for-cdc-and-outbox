package poc.common.deserializer;

import org.apache.flink.cdc.debezium.JsonDebeziumDeserializationSchema;

/**
 * Thin re-export so variant jobs can import from poc.common. Delegates entirely to the built-in
 * Flink CDC JSON deserializer.
 */
public class PocJsonDeserializationSchema extends JsonDebeziumDeserializationSchema {
  public PocJsonDeserializationSchema() {
    super();
  }
}
