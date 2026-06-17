package poc.common.checkpoint;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.junit.jupiter.api.Test;

class CheckpointConfigurerTest {

  @Test
  void applyExactlyOnce_setsAllCheckpointParameters() {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

    CheckpointConfigurer.applyExactlyOnce(env);

    CheckpointConfig cfg = env.getCheckpointConfig();
    assertEquals(CheckpointConfigurer.CHECKPOINT_INTERVAL_MS, cfg.getCheckpointInterval());
    assertEquals(CheckpointingMode.EXACTLY_ONCE, cfg.getCheckpointingMode());
    assertEquals(
        CheckpointConfigurer.MAX_CONCURRENT_CHECKPOINTS, cfg.getMaxConcurrentCheckpoints());
    assertEquals(CheckpointConfigurer.CHECKPOINT_TIMEOUT_MS, cfg.getCheckpointTimeout());
    assertEquals(
        CheckpointConfigurer.MIN_PAUSE_BETWEEN_CHECKPOINTS_MS, cfg.getMinPauseBetweenCheckpoints());
  }
}
