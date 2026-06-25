# CLAUDE.md — flink-cdc-poc

## What this project is

A 5-variant Gradle multi-project POC demonstrating Apache Flink MySQL CDC as a replacement for Kafka Connect CDC connectors. Each subproject is one variant; all share the `common` module and the same Podman infra.

## Build

```bash
./gradlew shadowJar          # build all fat-jars
./gradlew :variant-flink-datastream-api-v1-cdc-job:shadowJar   # single variant
./gradlew test               # run all tests (unit + component)
./gradlew all                # build all, restart Podman Compose, run component tests
```

Requires Java 17. Gradle wrapper is included — no local Gradle install needed.

### Code formatting (Spotless — Google Java Format)

All Java code must be formatted with Google Java Format via [Spotless](https://github.com/diffplug/spotless).

```bash
./gradlew fmt       # Format all Java files (auto-fix)
./gradlew fmtCheck  # Check formatting without applying changes
```

Formatting is enforced in the `check` task — `./gradlew build` will fail if code is unformatted. IDE integration: most IDEs (VS Code, IntelliJ) have formatter plugins; the easiest flow is to run `./gradlew fmt` before committing.

**Note:** Component tests require Podman to be running (`cd local-development-podman && podman-compose -f podman-compose.yml up -d`). If the stack is unavailable, component tests are skipped gracefully (shown as yellow ⭕ in test explorer).

### Full Integration Test (all)

The `all` task orchestrates a complete build-and-test cycle:

1. **Builds all modules** — `./gradlew clean build -x test shadowJar` (includes the variant fat-jars the component tests submit)
2. **Restarts Podman Compose** — `cd local-development-podman && podman-compose -f podman-compose.yml down --remove-orphans -v && ... up -d --build` (`down` exit is ignored — "container not found" on first run is normal; `--remove-orphans` ensures containers that lost their compose-project label are removed so the subsequent `up --build` doesn't fail with "creating container storage: the container name 'mysql' is already in use"; `--build` ensures images like `flink-cdc-submitter` are always rebuilt from the current `Dockerfile`/`entrypoint.sh` so stale baked-in scripts never survive a stack restart). Layer cache keeps this cheap for unchanged build contexts (e.g. `flink-with-mysql`), so only services whose baked-in code actually changed are rebuilt — the tradeoff (rebuild attempt every run vs. silently stale scripts) is intentionally weighted toward correctness.
3. **Waits for services** — polls MySQL + Kafka + Kafka Connect + Flink (up to 180 s); if Flink container doesn't exist (image build failure) the task throws with a diagnostic message rather than silently skipping
4. **Builds Kafka Connect SMTs** — `./gradlew :kafka-connect-smts:shadowJar`
5. **Deploys Kafka Connect connectors** — REST API (with `DB_HOST=mysql` for bridge networking)
6. **Runs component tests** — `./gradlew :component-tests:test`

Usage:
```bash
./gradlew all
```

The task runs all steps sequentially, stopping on any failure. Useful for CI/CD pipelines or full validation before deployment.

## Versions (change in root build.gradle only)

| Variable | Value |
|----------|-------|
| `flinkVersion` | `2.2.0` |
| `flinkCdcVersion` | `3.6.0-2.2` |
| `flinkKafkaVersion` | `5.0.0-2.2` |
| `kafkaVersion` | `3.7.0` |

All subproject `build.gradle` files reference these via `rootProject.ext.*` — never hardcode versions in subprojects.

**Java versions:** Flink job modules target Java 17. `kafka-connect-smts` targets Java 11 — `cp-kafka-connect:7.6.1` ships JDK 11 and refuses class files compiled for 17.

## Module map

| Module | Entry class | Notes |
|--------|-------------|-------|
| `common` | — | Shared config, deserializer, sink factory. No fat-jar. |
| `variant-flink-datastream-api-v1-cdc-job` | `poc.datastream.DataStreamCdcJob` | Most flexible; supports per-row routing |
| `variant-flink-table-api-cdc-job` | `poc.tableapi.TableApiCdcJob` | DDL-driven; good for future SQL joins |
| `variant-flink-sql-api-cdc-job` | `poc.sqlapi.SqlApiCdcJob` | StatementSet → single JobGraph |
| `variant-flink-datastream-api-v1-outbox-job` | `poc.outbox.OutboxJob` | Reads `outbox_events` table; routes by `destination` field |
| `variant-flink-cdc-yaml-pipeline-cdc-job` | — | No Java; submitted via `flink-cdc.sh pipeline.yaml` |

## Local infra

```bash
cd local-development-podman && podman-compose -f podman-compose.yml up -d --build    # MySQL + Kafka + Flink JM+TM + Kafka Connect + Kafka UI + Prometheus + Grafana
cd local-development-podman && podman-compose -f podman-compose.yml down -v         # tear down (data lost)
```

Services (Podman):
- Flink Dashboard: http://localhost:8081
- Kafka UI:        http://localhost:8080
- Kafka Connect:   http://localhost:8083
- Grafana:         http://localhost:3001 (user: admin, password: admin)
- Prometheus:      http://localhost:9090
- MinIO:           http://localhost:9001 (user: minioadmin, password: minioadmin, bucket: flink-checkpoints)
- MySQL:           localhost:3306  user=flink  password=flink  db=poc_db

Services (k8s — via `port-forward.sh`):
- Flink Dashboard: http://localhost:18081–18085 (one per variant)
- Kafka Connect:   http://localhost:18086
- Grafana:         http://localhost:13001 (user: admin, password: admin)
- Prometheus:      http://localhost:19090
- MinIO:           http://localhost:9001 (user: minioadmin, password: minioadmin, bucket: flink-checkpoints)
- MySQL:           localhost:13306  user=flink  password=flink  db=poc_db
- Kafka:           localhost:19092

MySQL binlog is enabled via `--log-bin=mysql-bin --binlog-format=ROW --binlog-row-image=FULL`.

## Monitoring — Prometheus metrics & Grafana dashboards

All Flink services (JobManager + TaskManager) export Prometheus metrics natively via the `flink-metrics-prometheus` plugin. The local stack includes Prometheus scraper and Grafana for dashboard/alerting.

### Metrics export

- **Flink JobManager** — http://localhost:9249/metrics (port 9249 on bridge network)
- **Flink TaskManager** — http://localhost:9250/metrics (on host; internally 9249 on bridge)

Metrics are scraped by Prometheus every 15 seconds and stored locally.

### Grafana dashboard

**Dashboard URL:** http://localhost:3001/d/flink-cdc-poc-monitoring (admin/admin)

Managed by Terraform (`terraform/dashboard.tf` reads `grafana/provisioning/dashboards/flink-cdc-monitoring.json`). Panels:
- 3 stat panels — Restart Loop, Checkpoint Duration, Checkpoint Failures (mirrors `rtdp-datadog-tf`)
- 4 text placeholders — KC monitors #4–#7, pending Spike S1
- 2 timeseries — JVM heap, job restarts

### Alert rules

**Alerts URL:** http://localhost:3001/alerting/list

Three rules managed by Terraform (`terraform/alerts.tf`):

| Alert | Threshold | Severity |
|-------|-----------|----------|
| Flink Restart Loop | `increase(numRestarts[5m])` > 3 | critical |
| Flink Checkpoint Duration High | `lastCheckpointDuration` > 180 000 ms | warning |
| Flink Checkpoint Failures | `increase(numberOfFailedCheckpoints[5m])` > 3 | critical |

Contact point: `flink-cdc-poc-email`. Notification policy groups by `alertname` + `job_name`.

Each rule has `__dashboardUid__ = "flink-cdc-poc-monitoring"` and `__panelId__` set (1/2/3), so alert state changes appear as annotation markers on the corresponding stat panels. The dashboard JSON includes an "Alert state changes" annotation layer for timeseries panels.

**Do not change the Grafana admin password through the UI.** Terraform uses `admin:admin`. If changed accidentally: `podman exec grafana grafana cli admin reset-admin-password admin && podman restart grafana`.

### Terraform

`./gradlew all` runs `terraform apply -auto-approve` automatically after Grafana is healthy. To apply manually:

```bash
cd local-development-podman/terraform
terraform init   # first time only; downloads grafana/grafana provider ~3.4
terraform apply
```

`terraform apply` is idempotent — safe to re-run. State in `terraform/terraform.tfstate` (local backend). The `all` task must **not** wipe `terraform.tfstate` before applying. Grafana has no data volume — its container layer is ephemeral — but `podman-compose down -v && up` does not always recreate the grafana container (it can reuse the existing one), so terraform-managed resources (folder, dashboard, alert rules, contact point, notification policy) may survive a stack restart. If `terraform.tfstate` is deleted every run, terraform plans to *add* resources that already exist in Grafana → `409 createFolderConflict` / `412 version-mismatch` and the `all` task fails. Persisting tfstate makes `apply` idempotent regardless of whether grafana was recreated (no-op if resources persist, recreate if wiped).

Resources managed: dashboard · folder "Flink CDC POC" · 3 alert rules · contact point · notification policy.

**Datasource ownership:** The Prometheus datasource (uid `prometheus`) is auto-provisioned by Grafana from `local-development-podman/grafana/provisioning/datasources/prometheus.yml`. Terraform reads it as a `data` source (`data.grafana_data_source.prometheus`) — it does **not** create it. Adding it as a Terraform `resource` would cause a 409 conflict on every `all` run.

## Adding a new variant

1. Create `variant-<name>/build.gradle` — apply shadow plugin, depend on `:common`, add CDC deps
2. Add entry class under `src/main/java/poc/<name>/`
3. Register in `settings.gradle`: `include 'variant-<name>'`
4. Assign a non-overlapping MySQL server-ID range (see table below) and document it in `podman-compose.yml`

## Server-ID ranges (do not overlap)

| Variant | Range |
|---------|-------|
| variant-flink-datastream-api-v1-outbox-job | 5600–5699 |
| variant-flink-cdc-yaml-pipeline-cdc-job | 5700–5709 |
| variant-flink-sql-api-cdc-job | 5800–5899 |
| variant-flink-datastream-api-v1-cdc-job | 5900–5999 |
| variant-flink-table-api-cdc-job | 6000–6099 |
| Kafka Connect connectors (kc-*) | 5500–5599 |

Ranges must be non-overlapping because Flink CDC 3.x incremental snapshot allocates IDs for parallel readers and restart attempts. A single ID collides on restart because the previous MySQL binlog lease hasn't expired.

## Configuration

All variants read from env vars via `poc.common.config.JobConfig.fromEnv()`. Defaults work against the Podman Compose setup without any extra config.

Key vars: `MYSQL_HOST`, `MYSQL_PORT`, `MYSQL_USER`, `MYSQL_PASSWORD`, `MYSQL_DATABASE`, `MYSQL_TABLES` (default: `poc_db.orders,poc_db.customers,poc_db.outbox_events`), `KAFKA_BOOTSTRAP`, `KAFKA_TOPIC_PREFIX`.

Per-variant server-ID ranges are also env-overridable (defaults match the [server-ID table](#server-id-ranges-do-not-overlap)): `MYSQL_SERVER_ID` (DataStream, `5900-5999`), `MYSQL_OUTBOX_SERVER_ID` (Outbox, `5600-5699`), `MYSQL_TABLE_API_SERVER_ID` (Table API, `6000-6099`), `MYSQL_SQL_API_ORDERS_SERVER_ID` (SQL API orders, `5800-5849`), `MYSQL_SQL_API_CUSTOMERS_SERVER_ID` (SQL API customers, `5850-5899`). All are validated as non-blank in `JobConfig.Builder.build()`.

## What to avoid

- Do not hardcode versions in subproject `build.gradle` files — always use `rootProject.ext.*`
- Do not reuse server-ID ranges across variants — MySQL will reject duplicate replica IDs
- Do not leave `upgradeMode: stateless` permanently in Flink deployments — every restart would re-snapshot the full table
- Variant 5 (YAML Pipeline) has no Maven shade module — do not add Java sources to it; it is intentionally zero-code
- Do not compile `kafka-connect-smts` for Java 17 — `cp-kafka-connect:7.6.1` runs Java 11 and refuses class files with major version 61 (`UnsupportedClassVersionError`); keep `sourceCompatibility = VERSION_11` in that subproject
- When deploying Kafka Connect connectors on Podman bridge, pass `DB_HOST=mysql` to `deploy-connectors.sh` — the Connect container cannot reach MySQL via `localhost`; `./gradlew all` does this automatically
- Do not remove `KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR: 1` and `KAFKA_TRANSACTION_STATE_LOG_MIN_ISR: 1` from the Kafka service in `podman-compose.yml` — without them, a single-broker KRaft cluster cannot serve `InitProducerId` requests, causing Flink's exactly-once Kafka sinks to restart-loop indefinitely with `TimeoutException: Timeout expired after 60000ms while awaiting InitProducerId`
- Keep the Flink and Kafka Connect engines in separate topic namespaces — Flink jobs use prefix `poc.flink` (→ `poc.flink.yaml-pipeline.orders` for the YAML variant) and the KC connectors use `poc.kc` (→ `poc.kc.yaml-pipeline`). Do not put a KC connector under `poc.flink.*` (or vice versa): both engines run simultaneously, and if a variant's Flink job and KC connector shared a topic, the test would find Flink-produced messages (which carry a `variant` field) instead of KC-produced ones (which don't). See [TOPICS.md](./TOPICS.md) for the full map.
- Do not add `DEFAULT 'PENDING'` (or any non-null default) to the `orders.status` column — Debezium 1.9.x applies column DEFAULT values when serialising null, so `DEFAULT 'PENDING'` causes CDC events to carry `"status":"PENDING"` instead of `"status":null` for explicitly-null inserts. The column is intentionally defined without a default (`status VARCHAR(1024)`) so that null values are faithfully preserved in CDC output
- Do not manually append `FLINK_PROPERTIES` to `$FLINK_HOME/conf/config.yaml` inside `flink-cdc-submitter/entrypoint.sh` — the container restarts on failure (`restart: on-failure`), so a second startup appends the same keys again, producing a duplicate-key YAML that SnakeYAML rejects with `YamlEngineException: found duplicate key execution.checkpointing.dir`. The Flink CDC CLI reads `FLINK_PROPERTIES` directly from the environment; no shell-level append is needed.
- When using Podman from a snap-installed VS Code: the snap overrides `XDG_DATA_HOME`, which splits podman storage between VS Code terminals (`~/snap/code/<rev>/.local/share/containers`) and the rest of the system (`~/.local/share/containers`). Symptoms: healthchecks stuck in `(starting)` forever, compose app invisible outside VS Code, stale aardvark-dns entries causing "No route to host" between containers. Fix: pin `graphroot` in `~/.config/containers/storage.conf` (already done on this machine)
- Do not use `apiVersion: kafka.strimzi.io/v1beta2` for KafkaConnect/KafkaConnector CRs with Strimzi 1.0.x — the cluster serves only `kafka.strimzi.io/v1` (v1beta2 is rejected). The v1 schema also moves `group.id` / `offset.storage.topic` / `config.storage.topic` / `status.storage.topic` out of `spec.config.*` to **required top-level spec fields** (`groupId`, `offsetStorageTopic`, `configStorageTopic`, `statusStorageTopic`); keeping them under `spec.config` fails validation with `spec.groupId: Required value` etc.
- Do not omit `mode: standalone` on the session-mode yaml-pipeline FlinkDeployment (`flinkdeployment-yaml.yaml`). The Flink Kubernetes Operator's default `native` mode dynamically scales TaskManagers from submitted-job parallelism and **ignores `taskManager.replicas`** — so no TM pod is created until a job is submitted. But the submitter (`flink-cdc-submitter/entrypoint.sh`) waits for a TM to register with the JM *before* submitting the pipeline, creating a deadlock (no TM until a job, no job until a TM). `mode: standalone` makes the operator respect `taskManager.replicas` and pre-deploy the TM so slots exist before the submitter runs (FLIP-225; operator ≥1.3.1 avoids the FLINK-30361 full-cluster-recreate bug — we run 1.15.0). The operator **cannot switch native→standalone in place** (`Cannot switch from native kubernetes to standalone kubernetes cluster`); delete the FlinkDeployment before re-applying with `mode: standalone`.
- Do not point a host-side Kafka consumer at the Strimzi `plain` (internal) listener via `kubectl port-forward svc/poc-kafka-kafka-bootstrap`. The internal listener advertises the in-cluster broker pod FQDN (`poc-kafka-dual-role-0.poc-kafka-kafka-brokers.poc.svc`); the bootstrap connection succeeds, but the broker returns that FQDN in its metadata response and the host client fails with `UnknownHostException` when it tries to reconnect to it. The `kafka.yaml` `external` listener (`type: nodeport`, port 9094, `advertisedHostTemplate: localhost` / `advertisedPortTemplate: "19092"`) exists for this: port-forward `svc/poc-kafka-kafka-external-bootstrap 19092:9094` and set `KAFKA_BOOTSTRAP=localhost:19092`. Strimzi `internal` listeners cannot override advertised host/port — only the external types (`nodeport`/`loadbalancer`/`ingress`/`route`) honor `advertisedHostTemplate`/`advertisedPortTemplate` — hence a second listener rather than reconfiguring `plain`.
- Do not use Debezium 3.0.x with the Strimzi Kafka 4.x base image (`quay.io/strimzi/kafka:1.0.1-kafka-4.2.0`). Debezium 3.0.x still calls `KafkaConsumer.poll(long)` and `DescribeTopicsResult.values()` which were removed in Kafka 4.0, causing `NoSuchMethodError` in `KafkaSchemaHistory.recoverRecords()` at connector task startup. Use Debezium 3.1+ (3.5.2.Final is the current stable). The `deploy.sh` KC image build uses `--no-cache` to prevent a cached Debezium download layer from surviving a version bump in the Dockerfile.
- Do not use `kind load docker-image` to load Podman-built images into kind. kind's experimental podman provider resolves images by querying the podman runtime socket, which may point at a different storage root than where `podman build` wrote the image (e.g. a snap-installed VS Code splits storage between `~/snap/code/<rev>/.local/share/containers` and `~/.local/share/containers`). The symptom is `ERROR: image: "localhost/flink-with-mysql:latest" not present locally` even though `podman images` shows the image. The fix in `deploy.sh` uses `podman save <img> | kind load image-archive /dev/stdin --name <cluster>` instead: podman serialises the image from whichever storage root it was actually built in, and kind loads the archive directly — no provider lookup, no storage-root mismatch.
- Do not leave the kind node at its default `pids_limit=2048` when running the full k8s slice (5 Flink JM+TM pairs + Kafka + Kafka Connect + MySQL + MinIO + kube-prometheus-stack — ~1950 PIDs at steady state). The simultaneous cold-start burst during `deploy.sh` pushes past 2048, and a FlinkDeployment TaskManager's containerd shim fails with `failed to create new OS thread (have 6 already; errno=11)` (EAGAIN from `clone()`) → the TM pod goes `Error` → the job logs `NoResourceAvailableException: Slot request bulk is not fulfillable!` and stays `CREATED` → `kubectl wait flinkdeployment/<v> --for=Running` times out at 600 s and `deploy.sh` aborts. `deploy.sh` section 1b raises it to 8192 via `podman update --pids-limit 8192 <kind-node>` (live + idempotent; `kind-config.yaml` has no PID-limit knob, so `deploy.sh` is the right place). The authoritative live value is `/sys/fs/cgroup/pids.max` inside the node container, not the cached `podman inspect` field.
- Do not hardcode `podman exec` or `podman update` in `deploy.sh` for operations on the kind node container. When both Docker and Podman are installed and `KIND_EXPERIMENTAL_PROVIDER` is unset, kind defaults to Docker — so the kind node container is managed by Docker (not Podman). `podman exec flink-cdc-poc-control-plane` then fails with `container state improper` (the container exists in podman's stale DB from a previous run but is not running there). `deploy.sh` section 1b detects the runtime: if `docker inspect <node> → running` it sets `CTR_EXEC=docker exec` / `CTR_UPDATE=docker update`; otherwise falls back to podman. All `ctr images tag` re-tagging and pids_limit update use `$CTR_EXEC`/`$CTR_UPDATE` so the script works with either runtime.
- Do not set k8s FlinkDeployment memory resources above 1500Mi per JM/TM when the host machine has ~20Gi RAM. Running `./gradlew all allK8s` leaves the Podman Compose stack running (~6GB RSS) while the k8s stack deploys; 5 JMs × 2Gi + 5 TMs × 2Gi = 20Gi in k8s requests exceeds the 19.4Gi node capacity and the last TM (yaml-pipeline) stays `Pending` with `Insufficient memory`. The FlinkDeployment manifests in `local-development-k8s/flink/` use 1500Mi for both JM and TM — 5 × 2 × 1.5Gi = 15Gi, well within the 19.4Gi node with room for Kafka/MySQL/MinIO system pods.
- Do not delete `terraform/terraform.tfstate` in the `all` task (or anywhere) before `terraform apply`. Grafana has no data volume — its container layer is ephemeral — but `podman-compose down -v && up` does not always recreate the grafana container (it can reuse the existing one), so terraform-managed resources (folder, dashboard, 3 alert rules, contact point, notification policy) can survive a stack restart. Wiping `terraform.tfstate` every run forces terraform to *add* resources that already exist in Grafana → `409 createFolderConflict` / `412 version-mismatch` and `all` fails mid-run. Persisting tfstate keeps `apply` idempotent (no-op when resources persist, recreate when grafana is wiped). The `all` task therefore runs `terraform init && terraform apply` without touching state.
- Do not issue `kubectl delete flinkdeployment <name>` without `--wait=false` in `local-development-k8s/deploy.sh`. `kubectl delete` blocks until the object is fully gone, and a FlinkDeployment carries the operator finalizer `flinkdeployments.flink.apache.org/finalizer` which only the Flink Kubernetes Operator can remove. If the operator's kubernetes-client watch is wedged (operator pod `Running` but not reconciling — visible as a fabric8 watch stack trace in its logs), the finalizer never clears, the delete hangs indefinitely, and `allK8s` stalls there for hours (observed: 93 min) before the bounded `kubectl wait --for=delete --timeout=120s` on the next line is ever reached. `deploy.sh` wraps every FlinkDeployment delete in `delete_flinkdeployment()`, which deletes with `--wait=false`, bound-waits 120 s, and if the finalizer is still stuck force-patches `{"metadata":{"finalizers":[]}}` and bound-waits again 60 s — so a wedged operator can stall at most ~3 min instead of indefinitely. To unblock a live stall: `kubectl -n flink-system delete pod -l app.kubernetes.io/name=flink-kubernetes-operator` (restarts the operator, often re-establishes the watch and clears pending finalizers), then `kubectl -n poc patch flinkdeployment <name> --type=merge -p '{"metadata":{"finalizers":[]}}'` for any still-Terminating object.
- Do not run `./gradlew all` (or `clean build ... shadowJar`) while stale IDLE Gradle daemons are alive. A stale daemon's in-memory task state survives `clean` deleting the variant fat-jars, so `:shadowJar` is falsely reported `UP-TO-DATE` and the `<variant>-all.jar` files are never rebuilt — component tests then fail with `NoSuchFileException: .../variant-flink-datastream-api-v1-cdc-job-all.jar` even though `component-tests:test` `dependsOn :variant-...:shadowJar` (the dependency is satisfied by the stale UP-TO-DATE marker, not by an actual file). Fix: run `./gradlew --stop` before the build to stop daemons of this Gradle version. Do NOT use `pkill -f '...'` to clear them — `pkill -f` matches the full command line of every process including the shell running the command, so any pattern (`'gradle'`, `'org.gradle.launcher.daemon'`, …) that names what to kill also appears in that shell's own argv and kills it before the build starts (exit 144). `./gradlew --stop` alone is sufficient. Verify the jars exist after the build: `ls variant-*/build/libs/*-all.jar`.
- Do not `kubectl delete flinkdeployment yaml-pipeline-cdc` while its session-cluster pipeline job is still RUNNING. In session mode (`mode: standalone`, no `spec.job`) the Flink Kubernetes Operator does **not** own job lifecycle, so a delete issued with a running job fails its cleanup with `Warning CleanupFailed: "non terminated jobs [...] that should be cancelled first"` and the finalizer never clears. The `delete_flinkdeployment` helper then force-patches the finalizer off; k8s GCs the CR but the operator's in-flight DELETING reconcile keeps running and ~6 min later **destroys the freshly-recreated JM Deployment** (same name `yaml-pipeline-cdc`) — which the operator will **not** recover without HA enabled (it logs `Could not recover lost jobmanager deployment without HA enabled` and sits in permanent `RECONCILING`/`DELETING`, `jobManagerDeploymentStatus: MISSING`). The submitter then waits forever for a TM that never registers and `YamlPipelineCdcTest` fails ~6 min later with an opaque `waitForKafkaMessage timed out` instead of a deploy-time error. Fix: `local-development-k8s/deploy.sh` §9 cancels every running pipeline job via a throwaway port-forward to the operator-auto-created `<name>-rest:8081` ClusterIP Service + `curl -X PATCH /jobs/:jid?mode=cancel` **before** `delete_flinkdeployment`, then polls until no non-terminal jobs remain, so the operator's cleanup sees no running job, the finalizer clears normally (no force-patch, no stale reconcile, no delayed JM destruction). The JM destruction is delayed (~6 min after `Running`), so a "verify JM exists right after `Running`" check passes while the JM is still alive — only **prevention** (cancel-before-delete) stops the cascade, not detection. The submitter `kubectl wait --for=condition=complete` is made **fatal** (`exit 1` on timeout + submitter logs) so a broken deploy fails fast instead of letting the component test time out.

## Component tests

Each Flink variant and Kafka Connect variant has corresponding component tests in the `component-tests` subproject. Flink tests submit fat-jars to the Flink JobManager REST API; Kafka Connect tests use the Kafka Connect REST API.

**Podman Compose prerequisites:**
```bash
cd local-development-podman && podman-compose -f podman-compose.yml up -d
```

**Run tests (Podman stack):**
```bash
# All tests (unit + component) — component tests auto-skip if stack unavailable
./gradlew test

# Component tests only
./gradlew :component-tests:test

# Single component test
./gradlew :component-tests:test --tests "poc.component.DataStreamCdcTest"

# Or run everything (build, restart Podman stack, deploy connectors, test):
./gradlew all
```

**Run tests (k8s stack — port-forwards must be active):**
```bash
# Fully automated: deploy + port-forward + test + teardown tunnels
./gradlew allK8s

# Single variant manually (port-forward its REST service first):
FLINK_REST_URL=http://localhost:18081 MYSQL_PORT=13306 KAFKA_BOOTSTRAP=localhost:19092 \
  ./gradlew :component-tests:test --tests "poc.component.DataStreamCdcTest"

# KC tests manually:
KAFKA_CONNECT_URL=http://localhost:18086 \
  SCHEMA_HISTORY_KAFKA_BOOTSTRAP=poc-kafka-kafka-bootstrap:9092 \
  MYSQL_PORT=13306 KAFKA_BOOTSTRAP=localhost:19092 \
  ./gradlew :component-tests:test --tests 'poc.component.KafkaConnect*'
```

**Test targeting env vars:**

| Variable | Default | Purpose |
|----------|---------|---------|
| `FLINK_REST_URL` | `http://localhost:8081` | Flink JM REST (`FlinkRestClient`) — override per variant for k8s |
| `KAFKA_CONNECT_URL` | `http://localhost:8083` | KC REST (`KafkaConnectBase`) — override to 18086 for k8s |
| `SCHEMA_HISTORY_KAFKA_BOOTSTRAP` | `kafka:29092` | Bootstrap inside KC for schema history; `poc-kafka-kafka-bootstrap:9092` for k8s |
| `MYSQL_PORT` | `3306` | Set to `13306` for k8s port-forward |
| `KAFKA_BOOTSTRAP` | `localhost:9092` | Set to `localhost:19092` for k8s port-forward |

### Flink Variants

Tests submit the variant fat-jar to the JM at `FLINK_REST_URL`, wait for RUNNING, assert Kafka output. `FlinkTestBase.ensureJobRunning()` reuses an already-RUNNING job of the same name (jobs are not cancelled after tests — a second instance would collide on MySQL server-id). Server-IDs come from `JobConfig` defaults, overridable per [Configuration](#configuration).

| Test class | Variant | Server-ID range | Kafka topic | Status |
|---|---|---|---|---|
| `DataStreamCdcTest` | variant-flink-datastream-api-v1-cdc-job | 5900–5999 (`MYSQL_SERVER_ID` default) | `poc.flink.datastream.orders` | ✅ PASS |
| `TableApiCdcTest` | variant-flink-table-api-cdc-job | 6000–6099 (`MYSQL_TABLE_API_SERVER_ID` default) | `poc.flink.table-api.orders` | ✅ PASS |
| `SqlApiCdcTest` | variant-flink-sql-api-cdc-job | 5800–5899 (`MYSQL_SQL_API_*_SERVER_ID` defaults) | `poc.flink.sql-api.orders` | ✅ PASS |
| `DataStreamOutboxTest` | variant-flink-datastream-api-v1-outbox-job | 5600–5699 (`MYSQL_OUTBOX_SERVER_ID` default) | `poc.flink.outbox.outbox-events` | ✅ PASS |
| `YamlPipelineCdcTest` | variant-flink-cdc-yaml-pipeline-cdc-job | 5700–5709 (submitter container) | `poc.flink.yaml-pipeline.orders` | ✅ PASS |

### Kafka Connect Variants

| Test class | Variant | Server-ID | Status |
|---|---|---|---|
| `KafkaConnectVariantTest` (parameterized) | kc-datastream-cdc | 5510 | ✅ PASS |
| `KafkaConnectVariantTest` (parameterized) | kc-table-api-cdc | 5520 | ✅ PASS |
| `KafkaConnectVariantTest` (parameterized) | kc-sql-api-cdc | 5530 | ✅ PASS |
| `KafkaConnectVariantTest` (parameterized) | kc-yaml-pipeline-cdc | 5540 | ✅ PASS |
| `KafkaConnectOutboxTest` | kc-outbox-cdc | 5550 | ✅ PASS |

**Stack availability:**
- If Podman stack is running (MySQL + Kafka + Flink JM): tests pass (✅ green in VS Code)
- If Podman stack is stopped: tests skip gracefully (⭕ yellow in VS Code)
- If Flink JM is not available: Flink tests skip gracefully
- If Kafka Connect is not available: Kafka Connect tests skip gracefully

**Note:** Flink jobs submitted by component tests are **not cancelled** after the test — they remain visible at http://localhost:8081/#/job/running for the lifetime of the stack. Tests submit via `FlinkTestBase.ensureJobRunning()`, which reuses an already-RUNNING job of the same name instead of resubmitting — a second instance of the same variant would collide on its MySQL server-id. Because all 5 Flink jobs and all 5 Kafka Connect connectors run **simultaneously**, every consumer needs its own server-ID range: the Kafka Connect connectors use the dedicated `5500–5599` range, and `OutboxJob` defaults to its own `5600–5699` range (via `MYSQL_OUTBOX_SERVER_ID`) instead of sharing the `MYSQL_SERVER_ID` default (`5900–5999`) with `DataStreamCdcJob`.

## Production Deployment

All variants are configured with **exactly-once semantics** checkpoints (30-second interval, 60-second timeout). For safe job upgrades and state recovery, see the runbooks below.

## Kafka Connect Variants (Alternative CDC Approach)

In addition to Flink variants, this POC includes **Kafka Connect** versions of all 5 CDC patterns using Debezium's MySQL connector with custom Single Message Transformers (SMTs):

**Quick start:**
```bash
./gradlew all
```

**What's included:**
- 5 connectors mirroring Flink variants (DataStream, Table API, SQL API, Outbox, YAML Pipeline)
- Custom SMT code for enrichment and dynamic topic routing
- Gradle project for building SMT JARs (Java 11 target)
- Deployment script with health checks and bridge-network hostname support
- Unit tests for transformations

**Key differences from Flink:**
- Kafka Connect: Stateless workers, horizontal scaling, REST API management
- Flink: Full state API, windowing, timers, fine-grained control
- Same Debezium source and Kafka sink
- Useful for benchmarking and understanding CDC trade-offs

See [KAFKA_CONNECT.md](./KAFKA_CONNECT.md) and [local-development-podman/KAFKA_CONNECT_QUICKSTART.md](./local-development-podman/KAFKA_CONNECT_QUICKSTART.md).

## Code Quality Analysis

**Checkstyle** — Code style and formatting rules (imports, naming, indentation)
```bash
./gradlew codeQuality          # Run on all subprojects
./gradlew :MODULE:checkstyleMain  # Single module
```

Reports are generated in `build/reports/checkstyle/` for each subproject.

**Configuration:** `config/checkstyle/checkstyle.xml` — customize rules here (default includes unused import detection).

To add SpotBugs or OWASP Dependency Check in the future, see the archived configuration files in `config/`.

## See Also

- [**FLINK_CHECKPOINT_CONFIG.md**](./FLINK_CHECKPOINT_CONFIG.md) — Flink 2.2 checkpoint semantics, monitoring, troubleshooting
- [**FLINK_SAVEPOINT_RUNBOOK.md**](./FLINK_SAVEPOINT_RUNBOOK.md) — 5-phase safe upgrade workflow, server-ID management, disaster recovery
- [**KAFKA_CONNECT.md**](./KAFKA_CONNECT.md) — Kafka Connect CDC variants, custom SMTs, detailed comparison
- [**kafka-connect-at-scale-74-connectors-migration.md**](./kafka-connect-at-scale-74-connectors-migration.md) — presentation: real-world 74-connector migration case study (EN)
- [**kafka-connect-at-scale-74-connectors-migration.ro.md**](./kafka-connect-at-scale-74-connectors-migration.ro.md) — traducere română a prezentării

## Context

This POC supports the architecture described in `../markdown/proposalA/`. The five variants map directly to the decision matrix in section 5 of that proposal. The recommended production path is the **DataStream CDC shared-job model** : one parametrisable image per tribe, Helm-chart env overrides, no per-tribe Java fork.
