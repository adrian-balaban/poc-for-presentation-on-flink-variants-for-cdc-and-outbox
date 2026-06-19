package poc.common.sink;

import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import poc.common.config.JobConfig;

public class KafkaSinkFactory {

  static String topicFor(JobConfig config, String suffix) {
    return config.kafkaTopicPrefix + "." + suffix;
  }

  public static KafkaSink<String> create(JobConfig config, String topicSuffix) {
    return KafkaSink.<String>builder()
        .setBootstrapServers(config.kafkaBootstrap)
        .setDeliveryGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
        .setTransactionalIdPrefix(config.kafkaTopicPrefix + "-" + topicSuffix)
        .setRecordSerializer(
            KafkaRecordSerializationSchema.builder()
                .setTopic(topicFor(config, topicSuffix))
                .setValueSerializationSchema(
                    new org.apache.flink.api.common.serialization.SimpleStringSchema())
                .build())
        .build();
  }
}
