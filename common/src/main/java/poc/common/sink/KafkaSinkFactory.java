package poc.common.sink;

import java.util.Properties;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import poc.common.config.JobConfig;

public class KafkaSinkFactory {

  // Flink's default transaction.timeout.ms (1 hour) exceeds Kafka broker's
  // transaction.max.timeout.ms default (15 min), causing InitProducerId rejection.
  // 10 minutes is well above any checkpoint timeout while staying under the broker max.
  private static final String TRANSACTION_TIMEOUT_MS = "600000";

  static String topicFor(JobConfig config, String suffix) {
    return config.kafkaTopicPrefix + "." + suffix;
  }

  public static KafkaSink<String> create(JobConfig config, String topicSuffix) {
    Properties producerProps = new Properties();
    producerProps.setProperty("transaction.timeout.ms", TRANSACTION_TIMEOUT_MS);

    return KafkaSink.<String>builder()
        .setBootstrapServers(config.kafkaBootstrap)
        .setDeliveryGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
        .setTransactionalIdPrefix(config.kafkaTopicPrefix + "-" + topicSuffix)
        .setKafkaProducerConfig(producerProps)
        .setRecordSerializer(
            KafkaRecordSerializationSchema.builder()
                .setTopic(topicFor(config, topicSuffix))
                .setValueSerializationSchema(
                    new org.apache.flink.api.common.serialization.SimpleStringSchema())
                .build())
        .build();
  }
}
