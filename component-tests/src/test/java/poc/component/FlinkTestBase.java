package poc.component;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeEach;

/**
 * Base class for Flink component tests that submit jobs to the real Flink JobManager container
 * (localhost:8081) via the REST API.
 *
 * <p>Tests are skipped gracefully if the Flink JM is not reachable. Submitted jobs remain running
 * after each test — visible at localhost:8081/#/job/running.
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
          if (flinkAvailable) {
            log.info("Flink JobManager is available at localhost:8081");
          } else {
            log.warn("Flink JobManager not available at localhost:8081");
          }
        }
      }
    }
    Assumptions.assumeTrue(
        flinkAvailable,
        "Flink JobManager not available — skipping test. "
            + "Run: cd local-development && podman-compose -f podman-compose.yml up -d");
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
