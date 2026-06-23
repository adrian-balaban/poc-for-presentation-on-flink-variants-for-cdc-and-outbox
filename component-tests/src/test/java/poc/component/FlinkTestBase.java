package poc.component;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeEach;

/**
 * Base class for Flink component tests that submit jobs to the Flink JobManager REST API.
 *
 * <p>The target URL is read from the {@code FLINK_REST_URL} environment variable (default {@code
 * http://localhost:8081}). Set this variable to target a different JM — e.g. when running against
 * a k8s variant via {@code kubectl port-forward}.
 *
 * <p>Tests are skipped gracefully if the Flink JM is not reachable. Submitted jobs remain running
 * after each test; resubmitting would collide on the MySQL server-id of the already-running job.
 *
 * <p>Jar paths are resolved from the root project dir (passed as system property 'rootProjectDir'
 * by component-tests/build.gradle).
 */
@Slf4j
abstract class FlinkTestBase extends ContainerBase {

  static final FlinkRestClient flink = new FlinkRestClient();

  private static volatile Boolean flinkAvailable = null;
  private static final Object flinkCheckLock = new Object();

  @BeforeEach
  void verifyFlinkAvailable() {
    if (flinkAvailable == null) {
      synchronized (flinkCheckLock) {
        if (flinkAvailable == null) {
          flinkAvailable = flink.isAvailable();
          String url = System.getenv().getOrDefault("FLINK_REST_URL", "http://localhost:8081");
          if (flinkAvailable) {
            log.info("Flink JobManager is available at {}", url);
          } else {
            log.warn("Flink JobManager not available at {}", url);
          }
        }
      }
    }
    Assumptions.assumeTrue(
        flinkAvailable,
        "Flink JobManager not available — skipping test. "
            + "Set FLINK_REST_URL or run: cd local-development-podman && podman-compose -f podman-compose.yml up -d");
  }

  /**
   * Ensure exactly one RUNNING instance of the job: reuse it if already running, otherwise upload
   * the fat-jar, submit, and wait until RUNNING. Jobs are not cancelled after tests, so
   * resubmitting would start a second instance whose duplicate MySQL server-id collides with the
   * first.
   */
  protected static String ensureJobRunning(
      Path jarPath, String entryClass, String jobName, Duration waitTimeout) throws Exception {
    String existing = flink.findRunningJob(jobName);
    if (existing != null) {
      log.info("Job '{}' already RUNNING (jobId={}) — reusing it", jobName, existing);
      return existing;
    }
    log.info("Submitting {} to Flink JM", jarPath.getFileName());
    String jarId = flink.uploadJar(jarPath);
    String jobId = flink.submitJob(jarId, entryClass);
    flink.waitForJobRunning(jobId, waitTimeout);
    return jobId;
  }

  /** Resolve the fat-jar path for a variant module relative to the root project dir. */
  protected static Path jarPath(String moduleName) {
    String rootDir = System.getProperty("rootProjectDir", ".");
    return Paths.get(rootDir, moduleName, "build", "libs", moduleName + "-all.jar");
  }
}
