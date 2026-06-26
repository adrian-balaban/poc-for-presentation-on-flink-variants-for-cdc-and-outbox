package poc.component;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Path;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.time.Duration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.json.JSONObject;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * Exactly-once invariant for the DataStream CDC variant's append Kafka sink.
 *
 * <p>This test asserts the <em>observable steady-state invariant</em> that the exactly-once sink
 * delivers downstream in normal operation: for a batch of inserted rows, each row id appears
 * <strong>exactly once</strong> among the sink's emitted messages within a bounded poll window. A
 * normal-path double-emission (a sink/source regression) surfaces as a duplicate id; a dropped
 * record surfaces as a missing id. Either fails the per-id count assertion.
 *
 * <p>It does <em>not</em> induce a crash or restart, so it does <em>not</em> exercise the
 * checkpoint-replay recovery path. A duplicate caused by replaying a checkpoint after a TaskManager
 * restart can only occur under a failure this test deliberately does not trigger — killing a TM
 * mid-checkpoint is flaky and environment-dependent (see {@link JobHealthTest} for restart-loop
 * detection). The value here is a deterministic, hermetic check of the steady-state exactly-once
 * property that a sink/source regression would break.
 *
 * <p>The Kafka consumer uses the default {@code read_uncommitted} isolation, so records are visible
 * on production rather than at checkpoint commit. In normal operation no transaction aborts, so the
 * read_uncommitted stream equals the committed stream and the per-id count is a faithful
 * exactly-once observation. The aborted-transaction case is exactly the crash-recovery path not
 * covered here.
 *
 * <p>This targets the DataStream append sink ({@code poc.flink.datastream.orders}) because every
 * mutation is a distinct Debezium envelope keyed by row id — the cleanest place to count per-id
 * emissions. The upsert-kafka variants (Table/SQL) coalesce updates by key and emit tombstones on
 * delete, so a per-id count invariant does not apply there.
 */
@Slf4j
@DisplayName("Exactly-Once Invariant (DataStream append sink)")
class ExactlyOnceInvariantTest extends FlinkTestBase {

  private static final Path JAR = jarPath("variant-flink-datastream-api-v1-cdc-job");
  private static final String JOB_NAME = "Flink DataStream API v.1 CDC Job";
  private static final String TOPIC = "poc.flink.datastream.orders";
  private static final int ROW_COUNT = 5;

  private static long idFromAfter(String msg) {
    try {
      JSONObject a = new JSONObject(msg).optJSONObject("after");
      return a == null ? -1 : a.optLong("id");
    } catch (Exception e) {
      return -1;
    }
  }

  @Test
  @Timeout(180)
  void dataStreamAppendSink_emitsEachInsertedRowExactlyOnce() throws Exception {
    ensureJobRunning(JAR, "poc.datastream.DataStreamCdcJob", JOB_NAME, Duration.ofSeconds(120));

    long stamp = uniqueId();
    Set<Long> insertedIds = new HashSet<>();
    try (Connection c = flinkConn();
        Statement s = c.createStatement()) {
      for (int i = 0; i < ROW_COUNT; i++) {
        s.executeUpdate(
            String.format(
                "INSERT INTO poc_db.orders (customer_id, amount, status) "
                    + "VALUES (%d, 1.23, 'EO-%d-%d')",
                stamp + i, stamp, i),
            Statement.RETURN_GENERATED_KEYS);
        try (ResultSet keys = s.getGeneratedKeys()) {
          assertThat(keys.next()).as("got generated id for row " + i).isTrue();
          insertedIds.add(keys.getLong(1));
        }
      }
    }
    assertThat(insertedIds).as("inserted " + ROW_COUNT + " distinct rows").hasSize(ROW_COUNT);

    // Poll for a bounded window, counting how many messages carry each inserted id. The consumer
    // uses the default read_uncommitted isolation, so records are visible on production rather than
    // at checkpoint commit; the 90 s window therefore absorbs only production + processing time,
    // not
    // the 30 s checkpoint interval. We poll repeatedly, tallying per-id message counts, until every
    // inserted id has been seen at least once AND a short settle period elapses with no new
    // matching
    // messages — that settle period is what lets us assert "exactly once" rather than "at least
    // once".
    Map<Long, Integer> counts = new HashMap<>();
    pollUntilSettled(TOPIC, insertedIds, counts, Duration.ofSeconds(90));

    for (Long id : insertedIds) {
      int seen = counts.getOrDefault(id, 0);
      assertThat(seen)
          .as("row id=" + id + " emitted exactly once (saw " + seen + " message(s))")
          .isEqualTo(1);
    }
    log.info(
        "Exactly-once invariant verified: {} inserted rows each emitted exactly once", ROW_COUNT);
  }

  /**
   * Consumes {@code topic} from earliest, tallying per-id message counts for ids in {@code
   * targetIds}. Returns once every target id has been seen at least once and no new target-id
   * message has arrived for a 10-second settle window, or when {@code deadline} expires. The
   * 10-second settle is sufficient here because the read_uncommitted consumer sees a normal-path
   * duplicate as soon as it is produced; a crash-replay duplicate (which could arrive later, at the
   * next checkpoint) is outside this test's scope — see the class Javadoc.
   */
  private void pollUntilSettled(
      String topic, Set<Long> targetIds, Map<Long, Integer> counts, Duration deadline) {
    Properties props = new Properties();
    props.put("bootstrap.servers", KAFKA_BOOTSTRAP);
    props.put("group.id", "eo-" + System.nanoTime());
    props.put("auto.offset.reset", "earliest");
    props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
    props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

    long deadlineMs = System.currentTimeMillis() + deadline.toMillis();
    long lastMatchMs = 0;
    try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
      consumer.subscribe(List.of(topic));
      while (System.currentTimeMillis() < deadlineMs) {
        var records = consumer.poll(Duration.ofMillis(500));
        for (var record : records) {
          long id = idFromAfter(record.value());
          if (targetIds.contains(id)) {
            counts.merge(id, 1, Integer::sum);
            lastMatchMs = System.currentTimeMillis();
          }
        }
        boolean allSeen = targetIds.stream().allMatch(id -> counts.containsKey(id));
        if (allSeen && lastMatchMs != 0 && System.currentTimeMillis() - lastMatchMs > 10_000) {
          return; // settled — no new matching messages for 10 s
        }
      }
    }
  }
}
