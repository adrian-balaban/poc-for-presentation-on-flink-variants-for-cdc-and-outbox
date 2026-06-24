package poc.component;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.api.connector.sink2.WriterInitContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.SecurityOptions;
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
    Configuration config = new Configuration();
    config.set(SecurityOptions.KERBEROS_LOGIN_USETICKETCACHE, false);
    env = StreamExecutionEnvironment.createLocalEnvironment(config);
    env.setParallelism(2);
  }

  /**
   * Collector that accumulates emitted records into a list — for asserting on a process function's
   * output without a Flink runtime. Hoisted here so both MiniCluster test classes share one
   * definition.
   */
  static final class ListCollector<T> implements org.apache.flink.util.Collector<T> {
    final List<T> out = new ArrayList<>();

    @Override
    public void collect(T record) {
      out.add(record);
    }

    @Override
    public void close() {}
  }

  /**
   * A {@link Sink} that accumulates records in a synchronized list.
   *
   * <p>Static so it is reachable across Flink's serialization boundary — the SinkWriter lambda runs
   * in the operator thread, while assertions run in the test thread.
   *
   * <p><b>Concurrency caveat:</b> because {@code COLLECTED} is a single static list, only one test
   * may use this sink at a time. That holds under JUnit's default sequential execution; if parallel
   * test execution is ever enabled, two MiniCluster tests sharing this sink would interleave writes
   * and corrupt each other's assertions. Keep these tests sequential (or give the sink an
   * instance-scoped buffer) before enabling parallelism.
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
