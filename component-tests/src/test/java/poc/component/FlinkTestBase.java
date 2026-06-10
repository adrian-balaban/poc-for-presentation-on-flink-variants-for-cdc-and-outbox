package poc.component;

import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeEach;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;

/**
 * Base class for Flink component tests that submit jobs to the real Flink JobManager
 * container (localhost:8081) via the REST API.
 *
 * Tests are skipped gracefully if the Flink JM is not reachable.
 * Each test is responsible for cancelling its job in a finally block.
 *
 * Jar paths are resolved from the root project dir (passed as system property
 * 'rootProjectDir' by component-tests/build.gradle).
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
        Assumptions.assumeTrue(flinkAvailable,
            "Flink JobManager not available — skipping test. " +
            "Run: cd local-development && podman-compose -f podman-compose.yml up -d");
    }

    /**
     * Upload the fat-jar, submit the job, wait until RUNNING, and return the jobId.
     * The caller must cancel the job in a finally block via {@link #cancelSilently}.
     */
    protected static String submitAndWait(Path jarPath, String entryClass, Duration waitTimeout) throws Exception {
        log.info("Submitting {} to Flink JM", jarPath.getFileName());
        String jarId = flink.uploadJar(jarPath);
        String jobId = flink.submitJob(jarId, entryClass);
        flink.waitForJobRunning(jobId, waitTimeout);
        return jobId;
    }

    /** Cancel a job; swallows errors so it is safe to use in finally blocks. */
    protected static void cancelSilently(String jobId) {
        if (jobId == null) return;
        try {
            flink.cancelJob(jobId);
        } catch (Exception e) {
            log.warn("Could not cancel job {}: {}", jobId, e.getMessage());
        }
    }

    /** Resolve the fat-jar path for a variant module relative to the root project dir. */
    protected static Path jarPath(String moduleName) {
        String rootDir = System.getProperty("rootProjectDir", ".");
        return Paths.get(rootDir, moduleName, "build", "libs", moduleName + ".jar");
    }
}
