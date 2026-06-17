package poc.common.validation;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static poc.common.validation.DdlValidator.requireSafeDdl;

import org.junit.jupiter.api.Test;

class DdlValidatorTest {

  @Test
  void doesNotThrow_whenValueIsNull() {
    assertDoesNotThrow(() -> requireSafeDdl(null, "field"));
  }

  @Test
  void doesNotThrow_whenValueHasNoUnsafeCharacters() {
    assertDoesNotThrow(() -> requireSafeDdl("safe-value_123", "field"));
  }

  @Test
  void throws_whenValueContainsSingleQuote() {
    IllegalArgumentException ex =
        assertThrows(IllegalArgumentException.class, () -> requireSafeDdl("o'brien", "MYSQL_USER"));
    assertTrue(ex.getMessage().contains("MYSQL_USER"));
  }

  @Test
  void throws_whenValueContainsBackslash() {
    assertThrows(
        IllegalArgumentException.class, () -> requireSafeDdl("path\\to\\thing", "MYSQL_HOST"));
  }
}
