# Checkpoint Configuration Guide — Flink CDC POC

## Flink 2.2 Checkpoint API (Simplified)

Flink 2.2 streamlined the checkpoint configuration. Here's the production-ready configuration for each variant.

---

## For Java Variants (DataStream, Table API, SQL API, Outbox)

Add this to each `main()` method after `StreamExecutionEnvironment.getExecutionEnvironment()`:

```java
StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

// Checkpoint configuration — production-ready
env.enableCheckpointing(30_000);              // 30-second interval
env.getCheckpointConfig().setCheckpointingMode(
    org.apache.flink.streaming.api.CheckpointingMode.EXACTLY_ONCE);
env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
env.getCheckpointConfig().setCheckpointTimeout(60_000);
env.getCheckpointConfig().setMinPauseBetweenCheckpoints(5_000);

// State backend — use in-memory for demo, RocksDB for production
// For Flink 2.2, state backend config is simplified:
// env.setStateBackend(new HashMapStateBackend());  // In-memory, for dev/demo
// For production: use cluster configuration or JobManager config
```

### Why This Configuration?

| Setting | Value | Reason |
|---------|-------|--------|
| `enableCheckpointing` | 30_000 ms | Allows 30 seconds between checkpoints; balances durability vs performance |
| `CheckpointingMode` | EXACTLY_ONCE | Critical for CDC: prevents duplicate Kafka messages on recovery |
| `MaxConcurrentCheckpoints` | 1 | CDC jobs snapshot during checkpoint; only one at a time |
| `CheckpointTimeout` | 60_000 ms | Source snapshot takes time; must exceed the 30s interval to give headroom under load |
| `MinPauseBetweenCheckpoints` | 5_000 ms | Prevents checkpoint storms after one finishes |

---

## For YAML Pipeline

```yaml
pipeline:
  name: Flink CDC YAML Pipeline CDC Job
  parallelism: 1
  schema.change.behavior: evolve
  
  # Checkpoint/recovery configuration
  checkpoint:
    interval: 30000                  # 30 seconds
    timeout: 60000                   # 60 seconds — must exceed the interval
    mode: EXACTLY_ONCE
    max_concurrent_checkpoints: 1    # One snapshot at a time
    min_pause: 5000                  # 5 seconds between checkpoints
```

**Note:** Not all Flink CDC Pipeline YAML keys may be recognized in Flink CDC 3.6.0-2.2. Test thoroughly in your environment.

---

## Critical: Snapshot vs Checkpoint

### What Happens During Checkpoint

1. **Snapshot phase**: MySQL CDC source captures current binlog position
2. **State flush**: All in-flight records reach Kafka sink
3. **Commit**: Checkpoint metadata written to state backend
4. **Resume**: On restart, source reads from saved binlog offset

### Why Non-Overlapping Server-ID Ranges Matter

Each parallel reader uses a **distinct server-ID** from the configured range:
- Variant 1: `5900-5999` (range of 100 for up to 100 parallel readers)
- Variant 2: `6000-6099` (separate range, no collision)

On restart **without savepoint**:
- Old job's MySQL binlog lease expires (~5 min)
- New job can reuse same server-ID range
- ✅ Automatic recovery works

On restart **with overlapping server-IDs but no savepoint**:
- MySQL rejects duplicate IDs
- ❌ Job fails with "Server-ID already registered"

---

## Production Checklist

- [ ] **Checkpointing enabled**: `env.enableCheckpointing()`
- [ ] **Checkpoint mode**: Set to `EXACTLY_ONCE` for CDC
- [ ] **State backend configured**: RocksDB for production (via config, not code)
- [ ] **Max concurrent checkpoints**: Set to 1
- [ ] **Savepoint strategy**: Test savepoint/resume workflow
- [ ] **Server-ID ranges**: Non-overlapping and documented
- [ ] **Checkpoint timeout**: Appropriate for table size and always greater than the interval (60s default here)
- [ ] **Min pause between checkpoints**: Prevents thundering herd

---

## Monitoring Checkpoints

### Via REST API

```bash
# Check latest checkpoint
curl http://localhost:8081/jobs/<JOB_ID>/checkpoints

# Sample response:
# {
#   "counts": {
#     "total": 142,          # Total checkpoints since start
#     "restored": 0,         # Recovered from checkpoint
#     "failed": 3,           # Failed (too slow, timeout)
#     "in_progress": 1       # Currently snapshotting
#   },
#   "latest": {
#     "completed": {
#       "id": 142,
#       "timestamp": 1717872342421,
#       "duration": 2843       # Took 2.8 seconds
#     },
#     "failed": {...},
#     "restored": {...}
#   }
# }
```

### Via Flink Dashboard

- **http://localhost:8081** → Jobs → Select job → Checkpoints tab
- Green = successful, Red = failed
- Duration trend shows if checkpoints are getting slower (possible memory leak)

### Via JM Logs

```bash
podman exec flink-jm tail -f /opt/flink/log/flink-<user>-jobmanager-*.log | grep -i checkpoint
```

---

## Troubleshooting

### Checkpoint Takes > 30 Seconds

**Symptom:** Checkpoint timeout warnings in logs

**Causes:**
1. Table too large — snapshot falls behind checkpoint interval
2. Slow disk I/O — state backend writes slowly
3. Network congestion — MySQL binlog transfer slow

**Solutions:**
- Increase `CheckpointTimeout` to 60_000 or higher
- Increase `MinPauseBetweenCheckpoints` to reduce frequency
- Scale up MySQL with faster network or SSD

### Checkpoint Never Completes

**Symptom:** Checkpoint hangs indefinitely

**Causes:**
1. Network partition to MySQL
2. MySQL binlog lock held by another job
3. Kafka broker unresponsive (sink can't write)

**Solutions:**
```bash
# Check job logs
podman logs flink-jm | grep -i "checkpoint\|timeout"

# Force cancel job
podman exec flink-jm flink cancel <JOB_ID>

# Investigate root cause (MySQL, Kafka, network)
podman exec mysql mysql -u flink -pflink -e "SHOW SLAVE STATUS\G"
```

### Duplicate Messages After Recovery

**Symptom:** Kafka topic has exact duplicate records

**Cause:** Checkpoint mode is `AT_LEAST_ONCE` instead of `EXACTLY_ONCE`

**Fix:**
```java
env.getCheckpointConfig().setCheckpointingMode(
    org.apache.flink.streaming.api.CheckpointingMode.EXACTLY_ONCE);
```

---

## See Also

- [SAVEPOINT_RUNBOOK.md](./SAVEPOINT_RUNBOOK.md) — Manual state management
- [CLAUDE.md](./CLAUDE.md) — Server-ID range allocation
- [Flink Checkpoint Docs](https://nightlies.apache.org/flink/flink-docs-master/docs/dev/datastream/fault_tolerance/checkpointing/)
