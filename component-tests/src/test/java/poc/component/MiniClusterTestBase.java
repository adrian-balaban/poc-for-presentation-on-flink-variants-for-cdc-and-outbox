package poc.component;

import lombok.extern.slf4j.Slf4j;

/**
 * Base class for local unit tests of Flink job logic.
 *
 * Fast unit testing of CDC transformations without needing containers.
 * For full end-to-end testing with real CDC source + Kafka, use FlinkTestBase.
 *
 * Suitable for:
 * - Fast feedback during development
 * - Testing transformation logic in isolation
 * - CI/CD pipelines where Podman is unavailable
 */
@Slf4j
abstract class MiniClusterTestBase extends ContainerBase {
}
