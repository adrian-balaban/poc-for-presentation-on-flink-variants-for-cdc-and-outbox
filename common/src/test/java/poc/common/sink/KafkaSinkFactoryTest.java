package poc.common.sink;

import org.junit.jupiter.api.Test;
import poc.common.config.JobConfig;

import static org.junit.jupiter.api.Assertions.assertEquals;

class KafkaSinkFactoryTest {

    private static JobConfig config() {
        return new JobConfig.Builder()
            .mysqlHost("localhost").mysqlPort(3306)
            .mysqlUser("u").mysqlPassword("p")
            .mysqlDatabase("db").mysqlTables("db.t")
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
    void topicFor_respectsConfigPrefix() {
        JobConfig other = new JobConfig.Builder()
            .mysqlHost("h").mysqlPort(3306).mysqlUser("u").mysqlPassword("p")
            .mysqlDatabase("db").mysqlTables("db.t")
            .kafkaBootstrap("k:9092")
            .kafkaTopicPrefix("x.y")
            .serverId("1-2")
            .build();

        assertEquals("x.y.orders", KafkaSinkFactory.topicFor(other, "orders"));
    }

}
