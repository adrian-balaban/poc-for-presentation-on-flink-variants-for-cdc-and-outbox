package poc.component;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.api.connector.sink2.WriterInitContext;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.junit.jupiter.api.BeforeEach;

/**
 * Base class for in-process Flink job tests.
 *
 * <p>Provides a local {@link StreamExecutionEnvironment} (no external services needed) and a
 * thread-safe {@link CollectingSink} so subclasses can execute real operator graphs — source →
 * process → sink — without MySQL or Kafka. Fast feedback during development and usable in CI where
 * Podman is unavailable.
 *
 * <p>For full end-to-end testing against a live CDC source and Kafka broker, use FlinkTestBase.
 */
@Slf4j
abstract class MiniClusterTestBase extends ContainerBase {

  /** Env is recreated fresh before each test so parallelism settings don't bleed across tests. */
  protected StreamExecutionEnvironment env;

  @BeforeEach
  void setUpMiniClusterEnv() {
    env = StreamExecutionEnvironment.createLocalEnvironment();
    env.setParallelism(2);
  }

  /**
   * A {@link Sink} that accumulates records in a synchronized list.
   *
   * <p>Static so it is reachable across Flink's serialization boundary — the SinkWriter lambda runs
   * in the operator thread, while assertions run in the test thread.
   */
  static final class CollectingSink implements Sink<String> {

    private static final List<String> COLLECTED = Collections.synchronizedList(new ArrayList<>());

    /** Clear before each test that uses this sink. */
    static void reset() {
      COLLECTED.clear();
    }

    static List<String> values() {
      return Collections.unmodifiableList(COLLECTED);
    }

    @Override
    public SinkWriter<String> createWriter(WriterInitContext context) {
      return new SinkWriter<>() {
        @Override
        public void write(String element, Context ctx) {
          COLLECTED.add(element);
        }

        @Override
        public void flush(boolean endOfInput) {}

        @Override
        public void close() {}
      };
    }
  }
}
