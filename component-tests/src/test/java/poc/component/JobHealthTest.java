package poc.component;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Path;
import java.time.Duration;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * Job-health assertions via Flink JobManager metrics — complements the "job reaches RUNNING" check
 * in every variant test with proof that the job is actually <em>making progress</em> (completing
 * checkpoints) and not silently restart-looping.
 *
 * <p>Uses {@link FlinkRestClient#getJobMetric(String, String)} to read JobManager-scoped metrics:
 *
 * <ul>
 *   <li>{@code numberOfCompletedCheckpoints} — must reach ≥ 1, proving checkpointing works
 *       end-to-end (the checkpoint interval is 30 s, so the test polls up to 120 s).
 *   <li>{@code numRestarts} — must be 0; a non-zero value means the job has restarted, indicating a
 *       fault that RUNNING-state-only assertions would miss.
 * </ul>
 *
 * <p>Targets the DataStream CDC job as a representative exactly-once job. The metrics endpoint is
 * generic; the same assertions apply to any variant's jobId.
 */
@Slf4j
@DisplayName("Flink Job Health (metrics)")
class JobHealthTest extends FlinkTestBase {

  private static final Path JAR = jarPath("variant-flink-datastream-api-v1-cdc-job");
  private static final String JOB_NAME = "Flink DataStream API v.1 CDC Job";

  @Test
  @Timeout(180)
  void dataStreamJob_completesCheckpointsAndDoesNotRestart() throws Exception {
    String jobId =
        ensureJobRunning(JAR, "poc.datastream.DataStreamCdcJob", JOB_NAME, Duration.ofSeconds(120));

    // Checkpoint interval is 30 s — wait for at least one completed checkpoint.
    long deadline = System.currentTimeMillis() + Duration.ofSeconds(120).toMillis();
    long completed = -1;
    while (System.currentTimeMillis() < deadline) {
      String raw = flink.getJobMetric(jobId, "numberOfCompletedCheckpoints");
      if (raw != null) {
        try {
          completed = Long.parseLong(raw);
        } catch (NumberFormatException ignore) {
          completed = -1;
        }
      }
      if (completed >= 1) break;
      Thread.sleep(3_000);
    }
    assertThat(completed)
        .as("numberOfCompletedCheckpoints >= 1 (job is making checkpoint progress)")
        .isGreaterThanOrEqualTo(1);

    String restartsRaw = flink.getJobMetric(jobId, "numRestarts");
    long restarts = restartsRaw == null ? -1 : Long.parseLong(restartsRaw);
    assertThat(restarts).as("numRestarts is 0 (job has not restart-looped)").isEqualTo(0);

    log.info(
        "Job health OK for '{}': checkpointsCompleted={}, restarts={}",
        JOB_NAME,
        completed,
        restarts);
  }
}
