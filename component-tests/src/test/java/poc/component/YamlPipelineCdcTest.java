package poc.component;

import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.InputStream;
import java.time.Duration;
import java.util.List;
import java.util.Objects;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

/**
 * Tests for YAML Pipeline variant.
 *
 * The YAML pipeline is submitted automatically at stack start by the
 * flink-cdc-submitter container. The e2e test verifies that CDC events
 * reach the expected Kafka topic without any in-test job submission.
 */
@Slf4j
@DisplayName("Flink CDC : YAML Pipeline CDC Test")
class YamlPipelineCdcTest extends ContainerBase {

    // ── Automated: YAML structural check ─────────────────────────────────────

    @Test
    void pipelineYaml_isOnClasspathAndReadable() {
        assertThatCode(() -> {
            try (InputStream is = YamlPipelineCdcTest.class
                    .getResourceAsStream("/pipeline.yaml")) {
                Objects.requireNonNull(is, "pipeline.yaml not found on classpath — " +
                    "add it as a test resource in component-tests/src/test/resources/");
                byte[] bytes = is.readAllBytes();
                if (bytes.length == 0) throw new IllegalStateException("pipeline.yaml is empty");
            }
        }).doesNotThrowAnyException();
    }

    // ── Automated: e2e verification via flink-cdc-submitter ──────────────────

    @Test
    @Timeout(60)
    void yamlPipeline_e2e_submitterRunsJobAndProducesKafkaEvents() {
        // The flink-cdc-submitter container submits the YAML pipeline on stack start.
        // This test verifies that CDC events are flowing into poc.cdc.yaml.orders.
        // If the job has not started yet (fresh stack), the poll timeout gives it time.
        List<String> messages = pollKafka("poc.cdc.yaml.orders", 1, Duration.ofSeconds(45));
        assertThat(messages)
            .as("Expected at least one CDC event on poc.cdc.yaml.orders — " +
                "ensure flink-cdc-submitter started successfully")
            .isNotEmpty();
        log.info("YAML Pipeline CDC: {} Kafka message(s) received", messages.size());
    }
}
