package poc.common.validation;

/**
 * Guards against embedding unsafe characters into Flink SQL DDL strings that are built via {@code
 * String.format}. Config values come from trusted env vars, but a stray {@code '} or {@code \}
 * would still break out of a quoted DDL literal — this is a cheap, fail-fast sanity check rather
 * than a defense against attackers.
 */
public final class DdlValidator {

  private DdlValidator() {}

  public static void requireSafeDdl(String value, String name) {
    if (value != null && (value.contains("'") || value.contains("\\"))) {
      throw new IllegalArgumentException(
          "Config value for " + name + " contains \"'\" or \"\\\" — unsafe to embed in Flink DDL");
    }
  }
}
