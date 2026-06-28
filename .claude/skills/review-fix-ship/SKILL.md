---
name: review-fix-ship
description: >
  Fix loop for this project. Use whenever the user wants to drive the project to a clean passing state. Loops until ALL are true: (1) `./gradlew all allK8s` exits with code 0
  (BUILD SUCCESSFUL, no test failures), (2) Podman — http://localhost:8081/jobs/overview shows 5 RUNNING jobs, AND (3) k8s — after `./local-development-k8s/port-forward.sh start`, each of the 5 variant JMs (18081-18085) shows 1 RUNNING job (5 total).
  Per iteration: A. run `./gradlew all allK8s`  B. fix errors  C. update README.md and CLAUDE.md  D. commit  E. push (only once all exit conditions pass).
---

# Flink CDC Fix Loop

Repeat the steps below until **all** exit conditions are satisfied:

1. `./gradlew all allK8s` exits with **code 0** (BUILD SUCCESSFUL, all tests pass)
2. **Podman** — `http://localhost:8081/jobs/overview` reports **exactly 5 RUNNING jobs** (all variants share one JobManager)
3. **k8s** — after launching `./local-development-k8s/port-forward.sh start`, each of the 5 variant JobManagers (`18081`–`18085`) reports **1 RUNNING job** — **5 RUNNING across the five JMs**. Unlike Podman, each k8s variant is its own FlinkDeployment with its own JM, so the jobs are spread one-per-JM rather than five-on-one.

---

## Steps (each iteration)

### A. Run `./gradlew all allK8s`

Run synchronously so the exit code is immediately available:

```bash
cd "$(git rev-parse --show-toplevel)"

# Stop any Gradle daemons left over from a previous iteration. Stale IDLE daemons
# are the known root cause of `shadowJar` being falsely reported UP-TO-DATE after
# `clean` (the daemon's in-memory task state survives `clean` deleting the output),
# so the variant fat-jars are never (re)created and component tests fail with
# NoSuchFileException for `<variant>-all.jar`.
#
# `./gradlew --stop` is the effective tool here — it gracefully stops all daemons
# of this Gradle wrapper's version (verified: it stops 3+ IDLE daemons cleanly).
#
# Do NOT add any `pkill -f '...'` fallback. `pkill -f` matches the FULL command
# line of every process, INCLUDING the very shell running this skill step: that
# shell's argv contains the pkill pattern itself, so `pkill -f 'gradle'` and
# `pkill -f 'org.gradle.launcher.daemon'` BOTH kill the running shell before the
# build starts (exit 144). There is no pattern that targets daemon JVMs without
# also matching this shell, because the pattern string itself is in the shell's
# command line. `./gradlew --stop` alone is sufficient — no pkill, ever.
./gradlew --stop || true

# Tear down the kind k8s cluster so each iteration starts from a clean slate.
# `allK8s` (via local-development-k8s/deploy.sh) recreates it if absent, so a stale
# cluster carrying half-deployed FlinkDeployments, wedged operators, stuck
# finalizers, or exhausted PIDs can't poison the next run. Cluster name is
# `flink-cdc-poc` (CLUSTER var in deploy.sh). Idempotent: deleting a non-existent
# cluster is a no-op.
kind delete cluster --name flink-cdc-poc || true

set -o pipefail                       # so the captured exit reflects gradlew, not tee
./gradlew all allK8s 2>&1 | tee /tmp/flink-cdc-all.log
GRADLEW_EXIT=${PIPESTATUS[0]}         # tee's exit ($?) is always 0 — read gradlew's instead
echo "EXIT: $GRADLEW_EXIT"
```

Extract key signals:

```bash
grep -E "BUILD (SUCCESSFUL|FAILED)|tests completed|PASSED|FAILED|ERROR|SKIPPED" /tmp/flink-cdc-all.log | tail -40
grep -iE "error|exception|caused by" /tmp/flink-cdc-all.log \
  | grep -vE "SLF4J|INFO|FINE|DEBUG" \
  | head -80
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
| `NoSuchFileException: <variant>-all.jar` in component tests, and `:shadowJar` shows `UP-TO-DATE` right after `:clean` | Stale IDLE Gradle daemon — its in-memory task state survives `clean` deleting the fat-jar, so `shadowJar` is skipped and the jar is never rebuilt | Run `./gradlew --stop` (graceful) before the build; do NOT `pkill -f '...'` (any pattern self-matches the running shell → exit 144). Verify with `ls variant-*/build/libs/*-all.jar` after step 1 |
| Terraform drift | Stale `terraform.tfstate` | `cd local-development-podman/terraform && terraform apply -auto-approve` |
| `No route to host` between containers | Podman storage split (snap VS Code) | Verify `graphroot` pinned in `~/.config/containers/storage.conf` |
| Flink job not RUNNING after build | Fat-jar not picked up | Confirm `shadowJar` ran before `podman-compose up` |
| Flink image build failure | Missing Dockerfile or bad tag | Check `local-development-podman/Dockerfile.*` and image names in `podman-compose.yml` |
| `allK8s` port-forward timeout | k8s stack not running or namespace missing | Run `local-development-k8s/deploy.sh` first; verify `kubectl get pods -n flink-poc` |
| `allK8s` KC test fails but Podman test passes | `SCHEMA_HISTORY_KAFKA_BOOTSTRAP` wrong for k8s | Set `SCHEMA_HISTORY_KAFKA_BOOTSTRAP=poc-kafka-kafka-bootstrap:9092` |
| k8s Flink job not RUNNING | Init-container didn't copy fat-jar | Check init container logs: `kubectl logs <pod> -c copy-jar -n flink-poc` |

After any Java change, always format first:

```bash
./gradlew fmt
```

### C. Update README.md and CLAUDE.md

After each fix, update relevant sections in `README.md` and `CLAUDE.md`:

- New invariant revealed (topic naming rule, Java version constraint, etc.) → add to the **"What to avoid"** section in `CLAUDE.md`
- Infrastructure change (new service, port, env var) → update the service table in `CLAUDE.md`
- Test added or module changed → update the component-tests tables (both Podman and k8s sections)
- Keep changes minimal and factual — document the **why**, not just the what

### D. Commit

Stage only files you changed (never `git add -A` or `git add .`):

```bash
git add <specific files changed>
git commit -m "$(cat <<'EOF'
<type>: <short description>

<1–3 sentences on root cause and why the fix is correct>

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>
EOF
)"
```

Types: `fix`, `feat`, `style` (formatting only), `docs`, `chore`.

### E. Push

Push **only after all exit conditions pass** — iterations exist because the build is
failing, so pushing every pass publishes known-broken commits to `master`. Commit each
iteration (step D), but defer the push until all exit conditions (see "Check exit
conditions" below) are met:

```bash
git push
```

---

## Check exit conditions

After each full iteration (A–E):

```bash
# Launch the k8s port-forwards FIRST. The k8s stack binds no host ports of its
# own — every variant JobManager is reachable only while these forwards are up.
# Idempotent: already-listening forwards are skipped, so re-running is safe.
./local-development-k8s/port-forward.sh start

# count_running <JM base URL> → prints the number of RUNNING jobs, or "x" if the
# JM is unreachable (port-forward down / job not submitted yet).
count_running() {
  curl -sf "$1/jobs/overview" 2>/dev/null | python3 -c "
import sys, json
try:
    jobs = json.load(sys.stdin)['jobs']
except Exception:
    print('x'); sys.exit()
print(len([j for j in jobs if j['state'] == 'RUNNING']))
" 2>/dev/null || echo x
}

# Exit condition 2 — Podman: all 5 variant jobs share ONE JM on :8081 → expect 5/5.
echo "Podman :8081 → $(count_running http://localhost:8081)/5 RUNNING"

# Exit condition 3 — k8s: each variant is its OWN FlinkDeployment with its OWN JM,
# port-forwarded to 18081-18085 → expect 1 RUNNING job each, 5 RUNNING in total.
k8s_total=0
for pl in 18081:DataStream 18082:Table-API 18083:SQL-API 18084:Outbox 18085:YAML-Pipeline; do
  port=${pl%%:*}; label=${pl##*:}
  n=$(count_running "http://localhost:$port")
  echo "  k8s :$port ($label) → $n/1 RUNNING"
  [ "$n" = x ] || k8s_total=$((k8s_total + n))
done
echo "k8s total → $k8s_total/5 RUNNING"
```

| EC1 (`gradlew` exit) | EC2 (Podman :8081) | EC3 (k8s 18081–18085) | Action |
|----------------------|--------------------|-----------------------|--------|
| 0 (pass) | 5/5 | 5/5 (1 per JM) | **Stop — report success** |
| 0 (pass) | < 5 | any | Investigate missing jobs at http://localhost:8081; fix entrypoint / fat-jar; go to step A |
| 0 (pass) | 5/5 | < 5 | Per-JM: if `x`, re-run `port-forward.sh start` (or the FlinkDeployment isn't Running — `kubectl get flinkdeployment -n poc`); if `0/1`, check that variant's job/init-container; go to step A |
| non-zero (fail) | any | any | Go to step B, fix errors, then step A |

---

## Success report

When all conditions are met, output:

```
✅ flink-cdc-fix-loop complete

./gradlew all allK8s: BUILD SUCCESSFUL
Podman jobs (5/5 on one JM :8081):
  - <job name 1> … <job name 5>
k8s jobs (5/5, 1 per JM):
  - :18081 DataStream    → 1 RUNNING
  - :18082 Table API     → 1 RUNNING
  - :18083 SQL API       → 1 RUNNING
  - :18084 Outbox        → 1 RUNNING
  - :18085 YAML Pipeline → 1 RUNNING

Commits pushed: <N>
```
