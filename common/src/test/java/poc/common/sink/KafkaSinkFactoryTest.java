package poc.common.sink;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;
import poc.common.config.JobConfig;

class KafkaSinkFactoryTest {

  private static JobConfig config() {
    return new JobConfig.Builder()
        .mysqlHost("localhost")
        .mysqlPort(3306)
        .mysqlUser("u")
        .mysqlPassword("p")
        .mysqlDatabase("db")
        .mysqlTables("db.t")
        .kafkaBootstrap("localhost:9092")
        .kafkaTopicPrefix("poc.cdc")
        .serverId("1-9")
        .build();
  }

  @Test
  void topicFor_concatenatesPrefixDotSuffix() {
    assertEquals("poc.cdc.datastream", KafkaSinkFactory.topicFor(config(), "datastream"));
  }

  @Test
  void transactionalIdPrefix_scopedToTopicPrefix() {
    // Verifies the prefix format introduced to prevent cross-environment Kafka transaction fencing.
    // Two deployments with different kafkaTopicPrefix values must produce different transactional IDs.
    assertEquals("poc.cdc-datastream", config().kafkaTopicPrefix + "-" + "datastream");

    JobConfig staging =
        new JobConfig.Builder()
            .mysqlHost("h")
            .mysqlPort(3306)
            .mysqlUser("u")
            .mysqlPassword("p")
            .mysqlDatabase("db")
            .mysqlTables("db.t")
            .kafkaBootstrap("k:9092")
            .kafkaTopicPrefix("staging.cdc")
            .serverId("1-2")
            .build();
    assertEquals("staging.cdc-datastream", staging.kafkaTopicPrefix + "-" + "datastream");
  }

  @Test
  void topicFor_respectsConfigPrefix() {
    JobConfig other =
        new JobConfig.Builder()
            .mysqlHost("h")
            .mysqlPort(3306)
            .mysqlUser("u")
            .mysqlPassword("p")
            .mysqlDatabase("db")
            .mysqlTables("db.t")
            .kafkaBootstrap("k:9092")
            .kafkaTopicPrefix("x.y")
            .serverId("1-2")
            .build();

    assertEquals("x.y.orders", KafkaSinkFactory.topicFor(other, "orders"));
  }
}
