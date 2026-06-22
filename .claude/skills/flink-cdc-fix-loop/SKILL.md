---
name: flink-cdc-fix-loop
description: Continuous fix loop for flink-cdc-poc. Loops until no changes made in git AND http://localhost:8081/#/job/running shows 5 jobs in RUNNING status: launch ./gradlew all, wait 7 minutes, read log, fix any warning/issue/error/exception, update *.md, commit, push.
---

# Flink CDC Fix Loop

Loop until **both** conditions are true:
1. `git status --short` is empty (nothing to commit)
2. `http://localhost:8081/#/job/running` shows 5 jobs in RUNNING status

## Steps (repeat each iteration)

### 1. Launch build

```bash
./gradlew all >log 2>&1 &
```

Run in background.

### 2. Wait 7 minutes

Wait 7 minutes for the build to complete.

### 3. Read output from file `log`

```bash
cat log
```

Also extract key signals:

```bash
grep -E "tests completed|PASSED|FAILED|ERROR|SKIPPED|BUILD" log | tail -30
grep -iE "warn|error|exception|caused by" log \
  | grep -vE "SLF4J|INFO|FINE|DEBUG" \
  | head -60
```

Check running jobs:

```bash
curl -sf http://localhost:8081/jobs/overview | python3 -c "
import sys,json
jobs=json.load(sys.stdin)['jobs']
running=[j for j in jobs if j['state']=='RUNNING']
print(f'RUNNING: {len(running)}/5')
for j in running: print(f'  {j[\"name\"]}')
"
```

### 4. Fix any warning/issue/error/exception

Triage and fix every warning, error, and exception found in the log. Common categories:

| Symptom | Likely cause | Fix |
|---------|-------------|-----|
| Test FAILED — assertion on `variant` field | Topic collision between Flink job and KC connector | Use distinct topic prefix for KC connector |
| Test FAILED — null instead of expected value | Debezium applying column DEFAULT to null | Remove DEFAULT from the column in `mysql-init/init.sql` |
| `UnsupportedClassVersionError` in KC container | `kafka-connect-smts` compiled for Java 17 | Keep `sourceCompatibility = VERSION_11` |
| `TimeoutException: InitProducerId` | Missing Kafka transaction log config | Add `KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR: 1` to podman-compose |
| Spotless check failure | Unformatted Java | Run `./gradlew fmt` |
| Terraform drift warning | Stale `terraform.tfstate` | Delete state before apply |
| `No route to host` between containers | Podman storage split | Verify `graphroot` pinned in `~/.config/containers/storage.conf` |
| Flink job not RUNNING | Fat-jar not rebuilt | Ensure `shadowJar` runs before `podman-compose up` |

After fixing Java files, run `./gradlew fmt`.

### 5. Update `*.md`

Update any relevant `*.md` files (CLAUDE.md, README, runbooks) if the fix reveals a non-obvious invariant or behaviour that should be documented.

### 6. Commit

Stage only files you changed (no `git add -A`):

```bash
git add <specific files changed>
git commit -m "<type>: <short description>

<body explaining root cause and why the fix is correct>

Co-Authored-By: Claude Sonnet 4.6 <noreply@anthropic.com>"
```

### 7. Push

```bash
git push
```

## Check termination

```bash
git status --short

curl -sf http://localhost:8081/jobs/overview | python3 -c "
import sys,json
jobs=json.load(sys.stdin)['jobs']
running=[j for j in jobs if j['state']=='RUNNING']
print(f'RUNNING: {len(running)}/5')
for j in running: print(f'  {j[\"name\"]}')
"
```

If `git status --short` is empty **and** RUNNING count is 5 → **stop and report success**.

Otherwise → go back to **Step 1**.
