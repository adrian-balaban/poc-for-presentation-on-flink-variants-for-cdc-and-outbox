package poc.common.checkpoint;

import org.apache.flink.configuration.ExternalizedCheckpointRetention;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * Applies the shared exactly-once checkpoint configuration used by every Flink variant in this POC.
 * Centralising it keeps all variants in lock-step — change the interval/timeout once here rather
 * than in four entry classes.
 */
public final class CheckpointConfigurer {

  /** Checkpoint interval in milliseconds. */
  public static final long CHECKPOINT_INTERVAL_MS = 30_000;

  /** Maximum time a single checkpoint may take before it is aborted. */
  public static final long CHECKPOINT_TIMEOUT_MS = 60_000;

  /** Minimum idle time between the end of one checkpoint and the start of the next. */
  public static final long MIN_PAUSE_BETWEEN_CHECKPOINTS_MS = 5_000;

  /** Only one checkpoint may be in flight at a time. */
  public static final int MAX_CONCURRENT_CHECKPOINTS = 1;

  private CheckpointConfigurer() {}

  /**
   * Enables exactly-once checkpointing on the given environment with the POC-standard interval,
   * timeout, pause, and concurrency settings. Checkpoints are retained on cancellation so that safe
   * stateful upgrades (savepoint-less recovery) remain possible.
   */
  public static void applyExactlyOnce(StreamExecutionEnvironment env) {
    env.enableCheckpointing(CHECKPOINT_INTERVAL_MS);
    env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
    env.getCheckpointConfig().setMaxConcurrentCheckpoints(MAX_CONCURRENT_CHECKPOINTS);
    env.getCheckpointConfig().setCheckpointTimeout(CHECKPOINT_TIMEOUT_MS);
    env.getCheckpointConfig().setMinPauseBetweenCheckpoints(MIN_PAUSE_BETWEEN_CHECKPOINTS_MS);
    env.getCheckpointConfig()
        .setExternalizedCheckpointRetention(ExternalizedCheckpointRetention.RETAIN_ON_CANCELLATION);
  }
}
