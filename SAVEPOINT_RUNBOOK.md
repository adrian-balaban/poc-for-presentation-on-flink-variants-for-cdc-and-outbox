# Savepoint Runbook — Flink CDC POC

This guide explains how to manage savepoints for safe job upgrades and state recovery in the Flink CDC POC.

---

## Quick Reference

| Task | Command |
|------|---------|
| Create savepoint | `flink savepoint <JOB_ID> /path/to/savepoint` |
| List running jobs | `podman exec flink-jm flink list` |
| Stop job with savepoint | `flink stop --savepointPath /path/to/savepoint <JOB_ID>` |
| Resume from savepoint | `flink run -s /path/to/savepoint fat-jar.jar` |

---

## Understanding State in CDC Jobs

### Three Levels of State

1. **MySQL Binlog Position** (source state)
   - Current offset in MySQL binlog
   - Saved in checkpoint
   - On restart: resumes from this position

2. **Kafka Sink Offset** (sink state)
   - Last Kafka message sent
   - Saved in checkpoint
   - With exactly-once semantics: prevents duplicates

3. **Incremental Snapshot State** (CDC-specific)
   - Records which tables have been snapshotted
   - Non-overlapping server-IDs ensure no collision on restart
   - Never mix server-ID ranges across job restarts

---

## Checkpoint vs Savepoint

| Aspect | Checkpoint | Savepoint |
|--------|------------|-----------|
| **Frequency** | Automatic, every 30 seconds | Manual, on-demand |
| **Retention** | Auto-deleted on job finish | Retained indefinitely |
| **Use Case** | Recovery after unexpected failure | Planned upgrades, code deployments |
| **Storage** | Temporary (JobManager state) | External filesystem |

---

## Workflow: Safe Job Upgrade with Savepoint

### Phase 1: Create Savepoint (5 min)

```bash
# Find the job ID
podman exec flink-jm flink list
# Output:
# 261859e8faa1bcc88ef0c282e2d4f76c : Flink DataStream API v.1 CDC Job (RUNNING)

JOB_ID="261859e8faa1bcc88ef0c282e2d4f76c"
SAVEPOINT_DIR="/tmp/savepoint-2026-06-08"

# Create savepoint (takes 10-30s, waits for next checkpoint)
podman exec flink-jm flink savepoint "$JOB_ID" "$SAVEPOINT_DIR"

# Output: Savepoint completed. Path: file:///tmp/savepoint-2026-06-08
```

**What happens:**
- Flink drains all in-flight data
- Snapshots source offset (MySQL binlog position)
- Snapshots sink state (Kafka offset)
- Returns path to savepoint directory

### Phase 2: Verify Savepoint

```bash
# Check savepoint exists in JM container
podman exec flink-jm ls -la /tmp/savepoint-2026-06-08

# Inspect savepoint metadata
podman exec flink-jm cat /tmp/savepoint-2026-06-08/_metadata
```

### Phase 3: Cancel Job Cleanly

```bash
# Stop job, then cancel with the savepoint reference
# (optional: if you want to save state from final seconds)
podman exec flink-jm flink cancel "$JOB_ID"

# Verify job is FINISHED
podman exec flink-jm flink list
```

### Phase 4: Deploy New Version

```bash
# Example: rebuild variant with code changes
cd /home/adrianb/_/claude/WIP-prezentare26062026/flink-cdc-poc
./gradlew :variant-flink-datastream-api-v1-cdc-job:shadowJar

# Copy updated JAR to Flink container
podman cp variant-flink-datastream-api-v1-cdc-job/build/libs/variant-flink-datastream-api-v1-cdc-job.jar \
  flink-jm:/opt/flink/jobs/
```

### Phase 5: Resume from Savepoint

```bash
JOB_JAR="/opt/flink/jobs/variant-flink-datastream-api-v1-cdc-job.jar"
SAVEPOINT_PATH="file:///tmp/savepoint-2026-06-08"

# Submit job restoring from savepoint
podman exec flink-jm flink run \
  -s "$SAVEPOINT_PATH" \
  "$JOB_JAR"

# Verify job is RUNNING
podman exec flink-jm flink list
```

**What happens:**
- JobManager loads savepoint metadata
- MySQL CDC source resumes from saved binlog position
- Kafka sink skips records already sent (exactly-once)
- New CDC events flow without re-snaphotting tables

---

## Common Scenarios

### Scenario 1: Memory Leak Suspected

**Problem:** Job memory usage grows over hours  
**Solution:** Savepoint → upgrade with fix → resume

```bash
# Create savepoint (preserves all state)
podman exec flink-jm flink savepoint <JOB_ID> /tmp/sp-memory

# Stop job
podman exec flink-jm flink cancel <JOB_ID>

# (Fix code, rebuild, deploy)

# Resume from savepoint — job picks up exactly where it left off
podman exec flink-jm flink run -s file:///tmp/sp-memory upgraded-job.jar
```

### Scenario 2: Data Corruption in Kafka Sink

**Problem:** Invalid JSON written to Kafka topic  
**Solution:** Rewind job to savepoint before corruption

```bash
# Don't create a new savepoint — use previous one
# List checkpoints written before corruption
podman exec flink-jm find /tmp -name "savepoint-*" -mtime -1

# Cancel current job (don't save state)
podman exec flink-jm flink cancel <JOB_ID>

# Resume from clean savepoint (resets job to that point in time)
podman exec flink-jm flink run -s file:///tmp/savepoint-2026-06-07 job.jar

# Kafka sink will re-emit records — exactly-once semantics prevent duplicates
```

### Scenario 3: Parallel Task Increase

**Problem:** Need to increase parallelism (more MySQL readers)  
**Solution:** Savepoint → scale out → resume

```bash
# Create savepoint with current parallelism
podman exec flink-jm flink savepoint <JOB_ID> /tmp/sp-scale

# Cancel job
podman exec flink-jm flink cancel <JOB_ID>

# Submit same job with increased parallelism
# (Edit MySQL CDC source to use broader server-ID range, e.g., 5900-5949 for 2 readers)
podman exec flink-jm flink run \
  -s file:///tmp/sp-scale \
  -p 2 \
  scaled-job.jar

# Flink automatically distributes source tasks
```

---

## Savepoint Storage Best Practices

### For Demo/Local

```bash
# Store in container tmp (lost on restart)
/tmp/savepoint-<timestamp>

# Or bind-mount to host for persistence
podman exec -it flink-jm bash
mkdir -p /checkpoints
flink savepoint <JOB_ID> /checkpoints/sp-2026-06-08
```

### For Production

```bash
# Use shared filesystem (NFS, S3, HDFS)
flink savepoint <JOB_ID> hdfs:///flink/savepoints/sp-2026-06-08
# or
flink savepoint <JOB_ID> s3://my-bucket/flink/savepoints/sp-2026-06-08
```

---

## Critical: Do NOT Mix Server-IDs Across Restarts

### ❌ Bad Example

```bash
# Job 1 started with server-id: 5900-5999
podman exec flink-jm flink run job-v1.jar

# Later, restart with different server-id: 5800-5899
podman exec flink-jm flink run job-v2.jar
# MySQL binlog leases still hold 5900-5999
# New job claims 5800-5899 → OK, different ranges
```

### ✅ Good Example

```bash
# Create savepoint (savepoint includes server-id range metadata)
podman exec flink-jm flink savepoint <JOB_ID> /tmp/sp-1

# Resume from savepoint — same server-id range is used automatically
podman exec flink-jm flink run -s /tmp/sp-1 upgraded.jar
```

**Key rule:** If using savepoints, let Flink manage server-IDs. The savepoint metadata ensures the same range is used on resume.

---

## Troubleshooting

### Issue: "Savepoint path not accessible"

```bash
# Make sure path exists in JM container
podman exec flink-jm test -f /tmp/savepoint-2026-06-08/_metadata

# If missing, check:
# 1. Was savepoint actually created? (check JM logs)
podman logs flink-jm | grep -i savepoint

# 2. Did filesystem get wiped? (containers lost on down/up)
podman-compose -f podman-compose.yml down  # ← Data in /tmp is lost
```

### Issue: "Cannot restore from savepoint — job configuration mismatch"

```bash
# Savepoint expects same number of source operators
# If you added a new MySQL source table, that's incompatible

# Solution: Create a fresh job, don't restore savepoint
podman exec flink-jm flink run new-job.jar
```

### Issue: "Job slow to cancel with savepoint"

```bash
# Savepoint waits for current checkpoint to finish
# Check if checkpoint is stuck
podman exec flink-jm tail -100f /opt/flink/log/*.log | grep -i checkpoint

# Increase checkpoint timeout if needed
# (see SAVEPOINT_TIMEOUT in job config)
```

---

## Production Deployment Pattern

```bash
# 1. Pre-production: Run canary with savepoint enabled
flink run -c poc.datastream.DataStreamCdcJob \
  -p 1 \
  variant-flink-datastream-api-v1-cdc-job.jar

# 2. Create savepoint after stable run (1+ hour)
flink savepoint <JOB_ID> hdfs:///flink/production/sp-$(date +%Y%m%d-%H%M%S)

# 3. Production: Deploy from savepoint
flink run -s hdfs:///flink/production/sp-20260608-143022 \
  -p 4 \
  variant-flink-datastream-api-v1-cdc-job.jar

# 4. On upgrade: repeat steps 2-3 with new JAR
```

---

## See Also

- [CLAUDE.md](./CLAUDE.md) — Project structure and server-ID ranges
- [Flink Savepoint Docs](https://nightlies.apache.org/flink/flink-docs-master/docs/ops/state/savepoints/)
- [CDC Concepts](https://nightlies.apache.org/flink/flink-cdc-docs-master/)
