package poc.common.deserializer;

import static org.junit.jupiter.api.Assertions.assertNotNull;

import org.junit.jupiter.api.Test;

class PocJsonDeserializationSchemaTest {

  @Test
  void constructor_createsInstance() {
    assertNotNull(new PocJsonDeserializationSchema());
  }
}
