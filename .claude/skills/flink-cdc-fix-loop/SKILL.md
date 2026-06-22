---
name: flink-cdc-fix-loop
description: >
  Fix loop for the flink-cdc-poc project. Use whenever the user wants to drive the project
  to a clean passing state. Loops until BOTH are true: (1) `./gradlew all` exits with code 0
  (BUILD SUCCESSFUL, no test failures), AND (2) http://localhost:8081/jobs/overview shows
  5 RUNNING jobs. Per iteration: A. run `./gradlew all`  B. fix errors  C. update README.md
  and CLAUDE.md  D. commit  E. push.
---

# Flink CDC Fix Loop

Repeat the steps below until **both** exit conditions are satisfied:

1. `./gradlew all` exits with **code 0** (BUILD SUCCESSFUL, all tests pass)
2. `http://localhost:8081/jobs/overview` reports **exactly 5 RUNNING jobs**

---

## Steps (each iteration)

### A. Run `./gradlew all`

Run synchronously so the exit code is immediately available:

```bash
cd /home/adrianb/_/claude/github/public_poc-for-presentation-on-flink-variants-for-cdc-and-outbox
./gradlew all 2>&1 | tee /tmp/flink-cdc-all.log
GRADLEW_EXIT=$?
echo "EXIT: $GRADLEW_EXIT"
```

Extract key signals:

```bash
grep -E "BUILD (SUCCESSFUL|FAILED)|tests completed|PASSED|FAILED|ERROR|SKIPPED" /tmp/flink-cdc-all.log | tail -30
grep -iE "error|exception|caused by" /tmp/flink-cdc-all.log \
  | grep -vE "SLF4J|INFO|FINE|DEBUG" \
  | head -60
```

### B. Fix errors

Triage and fix every error, test failure, and exception. Common causes:

| Symptom | Likely cause | Fix |
|---------|-------------|-----|
| Test FAILED — `variant` field assertion | Topic collision between Flink and KC connector | Use distinct topic prefix for KC connector (`poc.kc.*`) |
| Test FAILED — null instead of expected value | Debezium applying column DEFAULT to null | Remove `DEFAULT` from the column in `mysql-init/init.sql` |
| `UnsupportedClassVersionError` in KC container | `kafka-connect-smts` compiled for Java 17 | Keep `sourceCompatibility = VERSION_11` |
| `TimeoutException: InitProducerId` | Missing Kafka transaction log config | Add `KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR: 1` to podman-compose |
| Spotless / formatting failure | Unformatted Java | Run `./gradlew fmt` first |
| Terraform drift | Stale `terraform.tfstate` | `cd local-development/terraform && terraform apply -auto-approve` |
| `No route to host` between containers | Podman storage split (snap VS Code) | Verify `graphroot` pinned in `~/.config/containers/storage.conf` |
| Flink job not RUNNING after build | Fat-jar not picked up | Confirm `shadowJar` ran before `podman-compose up` |
| Flink image build failure | Missing Dockerfile or bad tag | Check `local-development/Dockerfile.*` and image names in `podman-compose.yml` |

After any Java change, always format first:

```bash
./gradlew fmt
```

### C. Update README.md and CLAUDE.md

After each fix, update relevant sections in `README.md` and `CLAUDE.md`:

- New invariant revealed (topic naming rule, Java version constraint, etc.) → add to the **"What to avoid"** section in `CLAUDE.md`
- Infrastructure change (new service, port, env var) → update the service table in `CLAUDE.md`
- Test added or module changed → update the component-tests table
- Keep changes minimal and factual — document the **why**, not just the what

### D. Commit

Stage only files you changed (never `git add -A` or `git add .`):

```bash
git add <specific files changed>
git commit -m "$(cat <<'EOF'
<type>: <short description>

<1–3 sentences on root cause and why the fix is correct>

Co-Authored-By: Claude Sonnet 4.6 <noreply@anthropic.com>
EOF
)"
```

Types: `fix`, `feat`, `style` (formatting only), `docs`, `chore`.

### E. Push

```bash
git push
```

---

## Check exit conditions

After each full iteration (A–E):

```bash
# Exit condition 2 — 5 RUNNING jobs
curl -sf http://localhost:8081/jobs/overview | python3 -c "
import sys, json
jobs = json.load(sys.stdin)['jobs']
running = [j for j in jobs if j['state'] == 'RUNNING']
print(f'RUNNING: {len(running)}/5')
for j in running:
    print(f'  {j[\"name\"]}')
"
```

| Exit condition 1 (`gradlew` exit code) | Exit condition 2 (RUNNING jobs) | Action |
|----------------------------------------|----------------------------------|--------|
| 0 (pass) | 5 | **Stop — report success** |
| 0 (pass) | < 5 | Investigate missing jobs at http://localhost:8081, fix, go to step A |
| non-zero (fail) | any | Go to step B, fix errors, then step A |

---

## Success report

When both conditions are met, output:

```
✅ flink-cdc-fix-loop complete

./gradlew all: BUILD SUCCESSFUL
Running Flink jobs (5/5):
  - <job name 1>
  - <job name 2>
  - <job name 3>
  - <job name 4>
  - <job name 5>

Commits pushed: <N>
```
