package poc.common.deserializer;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertNotNull;

class PocJsonDeserializationSchemaTest {

    @Test
    void constructor_createsInstance() {
        assertNotNull(new PocJsonDeserializationSchema());
    }
}
