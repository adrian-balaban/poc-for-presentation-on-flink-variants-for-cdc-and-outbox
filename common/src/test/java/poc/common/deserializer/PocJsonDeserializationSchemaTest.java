package poc.common.deserializer;

import static org.junit.jupiter.api.Assertions.assertNotNull;

import org.junit.jupiter.api.Test;

class PocJsonDeserializationSchemaTest {

  @Test
  void constructor_createsInstance() {
    assertNotNull(new PocJsonDeserializationSchema());
  }

  @Test
  void getProducedType_returnsNonNull() {
    // Confirms the Flink type system wiring inherited from JsonDebeziumDeserializationSchema
    // is intact — a broken super-class delegation would return null or throw.
    assertNotNull(new PocJsonDeserializationSchema().getProducedType());
  }
}
