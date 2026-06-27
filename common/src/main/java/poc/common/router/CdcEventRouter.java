package poc.common.router;

import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.json.JSONObject;
import poc.common.config.JobConfig;

/**
 * Routes each CDC event to an enriched output. In a real connector this would fan out to per-table
 * Kafka topics; here it annotates each event with the variant name for demo purposes.
 */
public class CdcEventRouter extends ProcessFunction<String, String> {

  private static final long serialVersionUID = 1L;

  private final String topicPrefix;

  public CdcEventRouter(JobConfig config) {
    this.topicPrefix = config.kafkaTopicPrefix;
  }

  @Override
  public void processElement(String event, Context ctx, Collector<String> out) {
    // In production: parse table from event JSON, route to per-table topic.
    // For the POC: tag event with variant and pass through.
    //
    // org.json parses safely and reports a clean JSONException on bad input — we
    // forward the event unmodified in that case (better than crashing the job).
    // A lone "}" or "{...no close brace" is malformed JSON we deliberately do
    // not attempt to enrich.
    try {
      JSONObject obj = new JSONObject(event);
      obj.put("variant", "datastream-cdc");
      obj.put("topic", topicPrefix + ".datastream.orders");
      out.collect(obj.toString());
    } catch (Exception e) {
      out.collect(event);
    }
  }
}
