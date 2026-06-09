package poc.component;

import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.InputStream;
import java.util.Objects;

import static org.assertj.core.api.Assertions.assertThatCode;

/**
 * Tests for YAML Pipeline variant.
 *
 * The YAML pipeline has no Java entry point — it is submitted via the
 * flink-cdc.sh CLI tool, which is not embeddable in a JVM test. The automated
 * test validates the YAML is present and well-formed. The end-to-end test
 * documents the manual verification procedure.
 */
@Slf4j
@DisplayName("Flink CDC : YAML Pipeline CDC Test")
class YamlPipelineCdcTest extends ContainerBase {

    // ── Automated: YAML structural check ─────────────────────────────────────

    @Test
    void pipelineYaml_isOnClasspathAndReadable() {
        // The pipeline.yaml is packaged into the variant-yaml-pipeline zip.
        // For CI we include it as a test resource (symlinked or copied by Gradle).
        // This test verifies the file exists and is non-empty; full schema
        // validation would require the flink-cdc runtime YAML parser.
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

        /**
     * Manual verification procedure for Variant 5 (YAML Pipeline):
     *
     * Prerequisites:
     *   1. Docker Compose stack running:       cd docker && docker compose up -d
     *   2. flink-cdc-dist downloaded and on PATH:
     *        export FLINK_CDC_HOME=/opt/flink-cdc-3.x
     *        export PATH=$FLINK_CDC_HOME/bin:$PATH
     *   3. Flink CDC mysql + kafka JARs in $FLINK_CDC_HOME/lib/:
     *        flink-cdc-pipeline-connector-mysql-<version>.jar
     *        flink-cdc-pipeline-connector-kafka-<version>.jar
     *
     * Submit the pipeline:
     *   flink-cdc.sh variant-yaml-pipeline/src/main/resources/pipeline.yaml \
     *       --flink-home /opt/flink-2.2.0
     *
     * Verify events in Kafka:
     *   kafka-console-consumer \
     *       --bootstrap-server localhost:9092 \
     *       --topic poc.cdc.yaml-pipeline \
     *       --from-beginning
     *
     * Expected output: JSON CDC events for rows in poc_db.orders.
     *
     * Expected topic: poc.cdc.yaml-pipeline
     * Server-ID range: 5700–5709 (see CLAUDE.md)
     */
    @Test
    void yamlPipeline_e2e_manualOnly() {
        log.info("YAML pipeline manual test — see javadoc for procedure");
        // This test documents the manual verification procedure.
        // The YAML pipeline cannot be embedded in a JVM test since it requires
        // the flink-cdc.sh CLI tool. All steps are documented in the javadoc above.
    }
}
