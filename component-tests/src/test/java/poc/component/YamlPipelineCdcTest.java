package poc.component;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

import java.io.InputStream;
import java.time.Duration;
import java.util.Objects;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * Tests for YAML Pipeline variant.
 *
 * <p>The YAML pipeline is submitted automatically at stack start by the flink-cdc-submitter
 * container. The e2e test verifies that CDC events reach the expected Kafka topic without any
 * in-test job submission.
 */
@Slf4j
@DisplayName("Flink CDC : YAML Pipeline CDC Test")
class YamlPipelineCdcTest extends ContainerBase {

  // ── Automated: YAML structural check ─────────────────────────────────────

  @Test
  void pipelineYaml_isOnClasspathAndReadable() {
    assertThatCode(
            () -> {
              try (InputStream is =
                  YamlPipelineCdcTest.class.getResourceAsStream("/pipeline.yaml")) {
                Objects.requireNonNull(
                    is,
                    "pipeline.yaml not found on classpath — "
                        + "add it as a test resource in component-tests/src/test/resources/");
                byte[] bytes = is.readAllBytes();
                if (bytes.length == 0) throw new IllegalStateException("pipeline.yaml is empty");
              }
            })
        .doesNotThrowAnyException();
  }

  // ── Automated: e2e verification via flink-cdc-submitter ──────────────────

  @Test
  @Timeout(120)
  void yamlPipeline_e2e_submitterRunsJobAndProducesKafkaEvents() throws Exception {
    // Insert a unique marker so this test is self-contained regardless of snapshot state.
    // The YAML pipeline job is already running (submitted by flink-cdc-submitter on stack start);
    // this binlog insert will be picked up and committed at the next checkpoint (≤30 s).
    String marker = "YAML-E2E-" + System.currentTimeMillis();
    // Use a parameterised PreparedStatement rather than string concatenation — models the
    // "near-production-quality" idiom this POC targets and avoids any quoting pitfalls.
    try (java.sql.Connection c = flinkConn();
        java.sql.PreparedStatement ps =
            c.prepareStatement(
                "INSERT INTO poc_db.orders (customer_id, amount, status) VALUES (?, ?, ?)")) {
      ps.setInt(1, 99);
      ps.setDouble(2, 99.99);
      ps.setString(3, marker);
      ps.executeUpdate();
    }

    // Poll up to 90 s to absorb checkpoint latency (30 s interval + processing time).
    String msg =
        waitForKafkaMessage(
            "poc.flink.yaml-pipeline.orders", Duration.ofSeconds(90), m -> m.contains(marker));
    assertThat(msg)
        .as(
            "Expected CDC event containing '"
                + marker
                + "' on poc.flink.yaml-pipeline.orders — "
                + "ensure flink-cdc-submitter started successfully")
        .isNotNull();
    log.info("YAML Pipeline CDC: received event with marker {}", marker);
  }
}
