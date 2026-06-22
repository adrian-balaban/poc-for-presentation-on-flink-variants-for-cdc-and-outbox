package poc.common.router;

import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import poc.common.config.JobConfig;

/**
 * Routes each outbox event to its destination Kafka topic based on the "destination" field in the
 * JSON payload.
 *
 * <p>In production: use side outputs (one per destination) for exactly-once per-topic fan-out. For
 * the POC we log the routing decision.
 */
public class OutboxRouter extends ProcessFunction<String, String> {

  private static final Logger LOG = LoggerFactory.getLogger(OutboxRouter.class);

  private final JobConfig config;

  public OutboxRouter(JobConfig config) {
    this.config = config;
  }

  @Override
  public void processElement(String event, Context ctx, Collector<String> out) {
    String destination = extractField(event, "destination");
    String topic = config.kafkaTopicPrefix + ".outbox.flink." + destination;
    // In production: use side outputs (one per destination) for per-topic exactly-once routing.
    // ctx.output(sideOutputTag(destination), event)
    LOG.info("event → topic={}  payload={}", topic, event);
    out.collect(event);
  }

  public String extractField(String json, String field) {
    String key = "\"" + field + "\":\"";
    int start = json.indexOf(key);
    if (start == -1) return "unknown";
    start += key.length();
    // Skip backslash-escaped quotes to avoid stopping at \" inside the value.
    int end = start;
    while (end < json.length()) {
      char c = json.charAt(end);
      if (c == '\\') {
        end += 2;
        continue;
      }
      if (c == '"') {
        break;
      }
      end++;
    }
    if (end >= json.length()) return "unknown";
    return json.substring(start, end);
  }
}
