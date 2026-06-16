package poc.component;

import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import poc.common.router.OutboxRouter;
import poc.common.config.JobConfig;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Local unit tests for OutboxRouter using fast, non-containerized environment.
 *
 * Tests dynamic topic routing based on the `destination` field in outbox events.
 * For full end-to-end testing with real CDC source and Kafka, use DataStreamOutboxTest.
 */
@Slf4j
@DisplayName("Flink DataStream Outbox : Router Tests")
class OutboxRouterMiniClusterTest extends MiniClusterTestBase {

    static class ListCollector<T> implements Collector<T> {
        final List<T> out = new ArrayList<>();

        @Override
        public void collect(T record) {
            out.add(record);
        }

        @Override
        public void close() {
        }
    }

    @Test
    @Timeout(10)
    void outboxRouter_extractsDestinationField() throws Exception {
        JobConfig config = new JobConfig.Builder()
            .mysqlHost("localhost").mysqlPort(3306)
            .mysqlUser("test").mysqlPassword("test")
            .mysqlDatabase("test").mysqlTables("test.t")
            .kafkaBootstrap("localhost:9092")
            .kafkaTopicPrefix("poc.outbox")
            .serverId("1-100")
            .build();

        OutboxRouter router = new OutboxRouter(config);

        String dest1 = router.extractField("{\"destination\":\"payments\",\"amount\":100}", "destination");
        String dest2 = router.extractField("{\"destination\":\"audit\",\"event\":\"login\"}", "destination");
        String dest3 = router.extractField("{\"no-dest\":true}", "destination");

        assertThat(dest1).isEqualTo("payments");
        assertThat(dest2).isEqualTo("audit");
        assertThat(dest3).isEqualTo("unknown");
        log.info("Field extraction works correctly");
    }

    @Test
    @Timeout(10)
    void outboxRouter_routesToDynamicTopic() throws Exception {
        JobConfig config = new JobConfig.Builder()
            .mysqlHost("localhost").mysqlPort(3306)
            .mysqlUser("test").mysqlPassword("test")
            .mysqlDatabase("test").mysqlTables("test.t")
            .kafkaBootstrap("localhost:9092")
            .kafkaTopicPrefix("custom")
            .serverId("1-100")
            .build();

        OutboxRouter router = new OutboxRouter(config);
        ListCollector<String> collector = new ListCollector<>();

        router.processElement("{\"destination\":\"payments\",\"amount\":50}", null, collector);
        router.processElement("{\"destination\":\"audit\"}", null, collector);

        assertThat(collector.out).hasSize(2);
        // Router collects the original event unchanged (routing info is in logs)
        assertThat(collector.out.get(0)).contains("\"destination\":\"payments\"");
        assertThat(collector.out.get(1)).contains("\"destination\":\"audit\"");
        log.info("Dynamic routing verified");
    }

    @Test
    @Timeout(10)
    void outboxRouter_handlesEscapedQuotes() throws Exception {
        JobConfig config = new JobConfig.Builder()
            .mysqlHost("localhost").mysqlPort(3306)
            .mysqlUser("test").mysqlPassword("test")
            .mysqlDatabase("test").mysqlTables("test.t")
            .kafkaBootstrap("localhost:9092")
            .kafkaTopicPrefix("test")
            .serverId("1-100")
            .build();

        OutboxRouter router = new OutboxRouter(config);

        // Value contains escaped quote — must not stop scanning there
        String field = router.extractField(
            "{\"destination\":\"pay\\\"ments\"}",
            "destination"
        );

        assertThat(field).isEqualTo("pay\\\"ments");
        log.info("Escaped quote handling verified");
    }

    @Test
    @Timeout(10)
    void outboxRouter_handlesEmptyValue() throws Exception {
        JobConfig config = new JobConfig.Builder()
            .mysqlHost("localhost").mysqlPort(3306)
            .mysqlUser("test").mysqlPassword("test")
            .mysqlDatabase("test").mysqlTables("test.t")
            .kafkaBootstrap("localhost:9092")
            .kafkaTopicPrefix("test")
            .serverId("1-100")
            .build();

        OutboxRouter router = new OutboxRouter(config);

        String field = router.extractField("{\"destination\":\"\"}", "destination");

        assertThat(field).isEmpty();
        log.info("Empty value handling verified");
    }
}
