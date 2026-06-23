package poc.common.router;

import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import poc.common.config.JobConfig;

/**
 * Routes each CDC event to an enriched output. In a real connector this would fan out to per-table
 * Kafka topics; here it annotates each event with the variant name for demo purposes.
 */
public class CdcEventRouter extends ProcessFunction<String, String> {

  private final String topicPrefix;

  public CdcEventRouter(JobConfig config) {
    this.topicPrefix = config.kafkaTopicPrefix;
  }

  @Override
  public void processElement(String event, Context ctx, Collector<String> out) {
    // In production: parse table from event JSON, route to per-table topic.
    // For the POC: tag event with variant and pass through.
    int lastBrace = event.lastIndexOf('}');
    String enriched =
        lastBrace < 0
            ? event
            : new StringBuilder(event)
                .replace(
                    lastBrace,
                    lastBrace + 1,
                    ",\"variant\":\"datastream-cdc\",\"topic\":\""
                        + topicPrefix
                        + ".datastream.orders\"}")
                .toString();
    out.collect(enriched);
  }
}
