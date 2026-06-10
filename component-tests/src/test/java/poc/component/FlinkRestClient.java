package poc.component;

import lombok.extern.slf4j.Slf4j;
import org.json.JSONArray;
import org.json.JSONObject;

import java.io.ByteArrayOutputStream;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.UUID;

/**
 * Thin client for the Flink REST API (localhost:8081).
 * Supports jar upload, job submission, and status polling.
 */
@Slf4j
class FlinkRestClient {

    private static final String BASE_URL = "http://localhost:8081";
    private static final HttpClient http = HttpClient.newHttpClient();

    boolean isAvailable() {
        try {
            HttpResponse<String> r = http.send(
                HttpRequest.newBuilder()
                    .uri(URI.create(BASE_URL + "/overview"))
                    .timeout(Duration.ofSeconds(5))
                    .GET().build(),
                HttpResponse.BodyHandlers.ofString());
            return r.statusCode() == 200;
        } catch (Exception e) {
            return false;
        }
    }

    /**
     * Upload a fat-jar and return the jarId (last path segment of the response filename).
     */
    String uploadJar(Path jarPath) throws Exception {
        String boundary = UUID.randomUUID().toString();
        String CRLF = "\r\n";
        byte[] jarBytes = Files.readAllBytes(jarPath);

        ByteArrayOutputStream body = new ByteArrayOutputStream();
        String partHeader = "--" + boundary + CRLF
            + "Content-Disposition: form-data; name=\"jarfile\"; filename=\"" + jarPath.getFileName() + "\"" + CRLF
            + "Content-Type: application/java-archive" + CRLF
            + CRLF;
        body.write(partHeader.getBytes(StandardCharsets.UTF_8));
        body.write(jarBytes);
        body.write((CRLF + "--" + boundary + "--" + CRLF).getBytes(StandardCharsets.UTF_8));

        HttpResponse<String> r = http.send(
            HttpRequest.newBuilder()
                .uri(URI.create(BASE_URL + "/jars/upload"))
                .timeout(Duration.ofSeconds(60))
                .header("Content-Type", "multipart/form-data; boundary=" + boundary)
                .POST(HttpRequest.BodyPublishers.ofByteArray(body.toByteArray()))
                .build(),
            HttpResponse.BodyHandlers.ofString());

        if (r.statusCode() != 200) {
            throw new RuntimeException("Jar upload failed (" + r.statusCode() + "): " + r.body());
        }

        String filename = new JSONObject(r.body()).getString("filename");
        String jarId = filename.substring(filename.lastIndexOf('/') + 1);
        log.info("Uploaded {} → jarId={}", jarPath.getFileName(), jarId);
        return jarId;
    }

    /**
     * Submit a jar as a streaming job and return the Flink jobId.
     */
    String submitJob(String jarId, String entryClass) throws Exception {
        String payload = entryClass != null
            ? "{\"entryClass\":\"" + entryClass + "\",\"parallelism\":1}"
            : "{\"parallelism\":1}";

        HttpResponse<String> r = http.send(
            HttpRequest.newBuilder()
                .uri(URI.create(BASE_URL + "/jars/" + jarId + "/run"))
                .timeout(Duration.ofSeconds(30))
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(payload))
                .build(),
            HttpResponse.BodyHandlers.ofString());

        if (r.statusCode() != 200) {
            throw new RuntimeException("Job submission failed (" + r.statusCode() + "): " + r.body());
        }

        String jobId = new JSONObject(r.body()).getString("jobid");
        log.info("Submitted jarId={} → jobId={}", jarId, jobId);
        return jobId;
    }

    /**
     * Return the jobId of a RUNNING job with the given name, or null if none exists.
     */
    String findRunningJob(String jobName) throws Exception {
        HttpResponse<String> r = http.send(
            HttpRequest.newBuilder()
                .uri(URI.create(BASE_URL + "/jobs/overview"))
                .timeout(Duration.ofSeconds(5))
                .GET().build(),
            HttpResponse.BodyHandlers.ofString());
        JSONArray jobs = new JSONObject(r.body()).getJSONArray("jobs");
        for (int i = 0; i < jobs.length(); i++) {
            JSONObject job = jobs.getJSONObject(i);
            if (jobName.equals(job.getString("name")) && "RUNNING".equals(job.getString("state"))) {
                return job.getString("jid");
            }
        }
        return null;
    }

    /**
     * Poll until the job reaches RUNNING state or the timeout expires.
     */
    void waitForJobRunning(String jobId, Duration timeout) throws Exception {
        Instant deadline = Instant.now().plus(timeout);
        while (Instant.now().isBefore(deadline)) {
            String state = getJobState(jobId);
            log.debug("Job {} state: {}", jobId, state);
            if ("RUNNING".equals(state)) {
                log.info("Job {} is RUNNING", jobId);
                return;
            }
            if ("FAILED".equals(state) || "CANCELED".equals(state) || "FINISHED".equals(state)) {
                throw new RuntimeException("Job " + jobId + " reached terminal state: " + state);
            }
            Thread.sleep(1000);
        }
        throw new RuntimeException("Job " + jobId + " did not reach RUNNING within " + timeout);
    }

    private String getJobState(String jobId) throws Exception {
        HttpResponse<String> r = http.send(
            HttpRequest.newBuilder()
                .uri(URI.create(BASE_URL + "/jobs/" + jobId))
                .timeout(Duration.ofSeconds(5))
                .GET().build(),
            HttpResponse.BodyHandlers.ofString());
        return new JSONObject(r.body()).getString("state");
    }
}
