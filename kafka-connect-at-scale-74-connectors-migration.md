# Kafka Connect @ Scale: 74 Connectors Migration Case

**Author:** Adrian Balaban  
**Date:** 2026-06-26


---

## Slide 0 — Why this talk is useful (what you'll be able to do after it)

> Not just a retelling of one client's migration — a **reusable playbook**.
> After this talk you can reproduce CDC with Kafka Connect or with Flink at
> another client.

Five things you take away:

1. **The client's journey** — how a few years ago they moved from a
   **DB-centric architecture** to an **event-driven** one by adding just Kafka
   and a handful of Kafka Connect connectors. The context that makes the
   outcome relevant.
2. **The real production pain with KC** — cascading rebalances across unrelated
   teams, lag with no per-team tuning, a shared blast radius on one cluster,
   Confluent Cloud licensing.
3. **Flink, Flink connectors, and Debezium in short** — what they are, where
   they overlap, where they differ; Debezium as a binlog parser reused
   internally by Flink CDC (not the same KC connector).
4. **Flink is fully event-driven** — not just a CDC connector, but a stateful
   stream-processing engine with event-time and exactly-once checkpoints,
   each job as an isolated K8s deployment.
5. **The info + POC code to do CDC at another client** — 5 variants running
   simultaneously, near-production code, reproducible Podman infra, component
   tests that validate the Kafka output.

> Goal: at the end you can choose between KC and Flink with arguments — and you
> have code to start from, not from zero.

---

## Slide 1 — The Problem in One Sentence

> Real client. Real scale. 95 connectors, 26 teams, one shared cluster — and the
> question whether Flink is the right way out.

Proposed migration of **74 MySQL connectors** from Confluent Kafka Cloud to Flink, with a proof of concept covering all 5 variants.

Presentation demo originally delivered to the **Cognizant Java Community Romania**.  

Second goal: produce near-production-quality code.

---

## Slide 1b — Agenda (45 minutes)

1. **Where we are** (2 min) — The client context + migration scope: 95 connectors on one cluster, 74 MySQL targets, 21 staying on KC
2. **Why it hurts & what we require** (4 min) — Challenges + the 3 requirements any solution must meet
3. **What is Flink, and why it's the structural fix** (3 min) — Flink in one frame; shared-pool vs per-job isolation
4. **The POC + evidence** (8 min) — 5 Flink variants running simultaneously; one code snippet; POC evidence table
5. **The solution + improvements** (5 min) — Shared-job model; concrete improvements vs today's challenges
6. **Architecture & collision avoidance** (7 min) — K8s deployment, server-ID ranges, monitoring
7. **The trade-offs** (4 min) — What changes, what remains, new operational surface
8. **Why this over alternatives** (5 min) — Decision matrix: why Flink CDC vs KC vs others
8b. **Cost of the change** (2 min) — TCO: what you stop paying, what you add
9. **Open questions** (3 min) — 8 spikes
10. **Recommendation & next step** (2 min) — commit decision, first tribe, timeline

**Q&A: 15 minutes**

*(agenda total: 45 min + 15 min Q&A; Slide 1c is an optional ~75 s Kafka primer and Slide 12b live screenshots are shown only if time permits — neither counted in the 45 min.)*

---

## Slide 1c — Context in ~75 seconds (for those new to Kafka)

```
MySQL binlog  →  Debezium  →  Kafka  →  consumers
               (captures       topics      (other systems,
                changes)                      databases)
```

| Term | What it is (one sentence) |
|------|---------------------------|
| **MySQL binlog** | MySQL's internal journal of every INSERT/UPDATE/DELETE — Debezium reads it like a replica |
| **Debezium** | Open-source library that turns the binlog into JSON change events |
| **Kafka Connect** | The platform that runs Debezium (and other connectors) as managed workers |
| **SMT** | Single Message Transformer — a KC plugin that modifies each record in-flight (enrichment, routing) |
| **Confluent Cloud** | Kafka + Kafka Connect as a managed service (you pay for it, you don't operate it) |
| **Apache Flink** | Stream-processing engine; can do the same job as Debezium + KC, but as an isolated K8s job |
| **Flink Operator / CR** | K8s operator that runs each Flink job as a `FlinkDeployment` Custom Resource (own JM + TM) |
| **StatementSet** | Flink Table API construct that compiles several INSERTs into one JobGraph (one checkpoint) |
| **IRSA** | IAM Roles for Service Accounts — how K8s pods get AWS permissions (S3 checkpoint access) |
| **RDS** | AWS managed relational database — the production MySQL source here (IAM auth) |
| **transactron** | The client's internal outbox connector, migrated in Phase 3 (see Spike S4) |

> **Key point:** every variant in this talk reads the same thing — the MySQL binlog — and writes to Kafka.
> The difference is *how* and *where* the reading process runs.

---

## Slide 2 — The Client Context (Where We Are Today)

**Real client experience: Confluent Kafka Cloud at scale**

- **95 connectors** on **one shared Kafka Connect cluster** across **26 teams**
- Two connector families today:
  - **Debezium (Kafka Connect)** — reads MySQL binlog via Confluent-managed KC, one event per change per topic
  - SFTP + SingleStore sink/source connectors
- Everything shares one cluster: one config, one rebalance group, one blast radius

> The shared cluster was convenient at 5 connectors. At 95 across 26 teams — and
> growing — it is the single biggest source of cross-team incidents. That scaling
> pressure is why we're looking now.

---

## Slide 3 — What We Require, and What Hurts Today

**What we require of any solution** *(Kafka Guild, solution-agnostic — the yardstick for the options on the Alternatives slide):*

1. Base image + security patching stay **centralised** — tribes don't own the runtime.
2. **Move away from Confluent Platform** — licensing and lock-in.
3. **Tribe-based clusters don't solve ownership** — they multiply cost 26× without fixing the root cause.

**What hurts today — and what it costs:**

| Pain | Who | How often | Business impact |
|------|-----|-----------|-----------------|
| Rebalancing storms — one bad connector destabilises all | All 26 teams | Multiple×/quarter | Cross-tribe incidents; consumer downtime during cascade |
| Shared blast radius — 95 connectors, one cluster | All 26 teams | Every incident | No isolation between tribes |
| Recurring lag — no per-tribe lever | Team + consumers | Ongoing | SLA risk on downstream consumers |
| Production-only failures — surface only after deploy | New-connector teams | New-connector window | Defects reach prod undetected |
| Confluent Kafka Cloud licensing | Organisation | Monthly | **Material monthly licensing cost** |
| Centralised security patching | Maintenance team | Every release cycle | Fleet-wide coordination overhead |

> One bad connector restart triggers a **cascade rebalance across unrelated tribes** — and most rows above map to a concrete improvement (see the Improvements slide).

---

## Slide 4 — What Is Flink, and Why It's the Structural Fix

**Apache Flink** is a stateful stream-processing engine: a continuous job that reads events, keeps state, and writes results — with **exactly-once checkpoints** (durable, recoverable) and **event-time** semantics. Each job runs as its **own isolated K8s deployment** (own JobManager + TaskManager) under the Flink Operator.

**Flink CDC** is the connector that does the same job as Debezium-on-Kafka-Connect — reads the MySQL binlog and emits change events to Kafka — but with an **incremental snapshot** algorithm that needs **no shared offset topic and no signal topic**, running inside that isolated job.

The structural argument in one frame — this is the bridge from "why it hurts" to "why Flink fixes it":

| | Kafka Connect today | Flink (proposed) |
|--|--|--|
| Deployment | 1 shared worker cluster | N isolated K8s jobs (Flink Operator) |
| Blast radius | 1 — all 95 connectors | 1 per tribe — contained |
| Rebalance | one group → cascade across 26 teams | none — no shared group |
| Offsets / state | shared offset topic | per-job exactly-once checkpoints (S3) |
| Licensing | Confluent Cloud (paid) | Apache 2.0 (free) |

> **"CDC" means two things — don't confuse them:** (1) **Flink CDC `MySqlSource`** — the connector that reads the MySQL binlog via Flink CDC's own incremental-snapshot algorithm (variants 1–4: DataStream / Table API / SQL API / Outbox; it reuses Debezium's binlog parser internally but does **not** run on the Debezium Kafka Connect connector). (2) **Flink CDC YAML pipeline** — the declarative *YAML pipeline framework* on top of that same source, no Java (Variant 5). This talk covers both senses.
---

## Slide 5 — Scope of the Migration

**What we're migrating:** 74 MySQL connectors → Apache Flink MySQL CDC Connector

**What stays on Kafka Connect:** 21 SFTP + SingleStore connectors (Flink has no equivalent)

![Migration Pattern: Before and After](images/migration-before-after.svg)

---

## Slide 6 — The POC: Five Flink Variants

We built **5 variants** and ran them
**simultaneously**.

| # | Variant | Entry-Class Size | Output Format | Java Required |
|---|---------|-----------|---------------|---------------|
| 1 | DataStream CDC | 50 lines | Debezium envelope + enrichment | Yes |
| 2 | Table API | 99 lines | Flattened projected row (upsert-kafka) | Yes |
| 3 | SQL API | 156 lines | Flattened projected row (upsert-kafka) | Minimal |
| 4 | Outbox | 56 lines | Debezium envelope of outbox row (single topic; per-destination routing is production, not in POC) | Yes |
| 5 | YAML Pipeline | 47 lines YAML | Native Debezium envelope | **No** |

> All four Java variants additionally share ~391 lines of `common/` infrastructure
> (`JobConfig`, `CheckpointConfigurer`, deserializer, routers, `KafkaSinkFactory`) —
> entry classes contain only variant-specific wiring.

---

## Slide 7 — Decision Matrix: Which Variant for Which Connector?

![Connector Decision Tree: Which Variant for Which Connector?](images/connector-decision-tree.svg)

| Connector Shape | Recommended Variant | Why |
|----------------|--------------------|----|
| Outbox (transactional, per-row routing) | DataStream | Table/SQL API can't do per-row routing |
| CDC with custom enrichment/transformation | DataStream CDC | Java access to `CdcEventRouter` + custom `MapFunction` |
| Simple CDC (table → topic, no transform) | YAML Pipeline/SQL API | Zero Java; SQL API already building shade modules |
| CDC with future SQL joins/aggregation | Table API | Unlocks Flink's Table API ecosystem (type-safe Java) |

---

## Slide 8 — The Java Dev's View: Code Comparison

### DataStream CDC (50-line entry class, most control)

```java
MySqlSource<String> source = MySqlSource.<String>builder()
    .hostname(config.mysqlHost).port(config.mysqlPort)
    .databaseList(config.mysqlDatabase).tableList(config.mysqlTables)
    .username(config.mysqlUser).password(config.mysqlPassword)
    .serverTimeZone("UTC")
    .serverId(config.serverId)
    .deserializer(new PocJsonDeserializationSchema())
    .build();

env.fromSource(source, WatermarkStrategy.noWatermarks(), "MySQL CDC Source")
   .process(new CdcEventRouter(config))
   .sinkTo(KafkaSinkFactory.create(config, "datastream"));
```

> All connection details come from `JobConfig.fromEnv()` — nothing is hardcoded;
> this is the same parametrisation the shared-job model relies on (see the Shared Job Model slide).

### YAML Pipeline (47 lines, zero Java)

```yaml
source:
  type: mysql
  hostname: ${MYSQL_HOST}
  port: ${MYSQL_PORT}
  username: ${MYSQL_USER}
  password: ${MYSQL_PASSWORD}
  tables: ${MYSQL_DATABASE}.orders
  server-id: 5700-5709
sink:
  type: kafka
  properties.bootstrap.servers: ${KAFKA_BOOTSTRAP}
  topic: ${KAFKA_TOPIC_PREFIX}.yaml.orders
pipeline:
  name: Flink CDC YAML Pipeline CDC Job
```

---

## Slide 9 — Recommended Architecture: Shared Job Model

**One base image per variant. 74 MySQL connectors. No per-tribe Java fork.**

Flink Platform Team owns and maintains parametrisable images for the 5 variants.
Each tribe gets their connector by overriding Helm values only — no fork, no per-tribe release pipeline (YAML/SQL/Table variants need no Java; DataStream tribes customize the platform-owned image, not their own Java repo).

![K8s Deployment Topology: Shared Job Model](images/k8s-deployment-topology.svg)

```yaml
# All a tribe needs
applicationJobs:
  my-tribe-cdc:
    image: flink-stream-api-base-image:1.0.0
    extraEnvs:
      MYSQL_HOST: my-db.internal
      MYSQL_DATABASE: my_schema
      KAFKA_TOPIC_PREFIX: my-tribe.cdc
```

| Variable | Description |
|----------|-------------|
| `MYSQL_HOST/PORT/USER/PASSWORD` | CDC source |
| `MYSQL_DATABASE` / `MYSQL_TABLES` | Scope of capture |
| `MYSQL_SERVER_ID` | Binlog replica range (non-overlapping) |
| `KAFKA_BOOTSTRAP` / `KAFKA_TOPIC_PREFIX` | Sink config |

---

## Slide 10 — K8s Deployment Model

**One repo, five variants, one Jenkins pipeline definition — one execution per variant.**

The `flink-base-chart` `applicationJobs` map emits per key:
- `FlinkDeployment` CR (its own JobManager + TaskManager)
- `<jobName>-rest` ClusterIP Service
- `FlinkStateSnapshot` CR

### Collision Avoidance — Each Variant Gets Its Own Lane

| Axis | Allocation |
|------|------------|
| MySQL server-ID | outbox=5600–5699, pipeline=5700–5709, sql-api=5800–5899, cdc=5900–5999, table-api=6000–6099 |
| MySQL schema | `cdc_db`, `sql_api_db`, `table_api_db`, `pipeline_db`, `outbox_db` |
| Kafka topic prefix | `shared-cdc.cdc-db.*`, `sql-api.sql-api-db.*`, `table-api.table-api-db.*`, `pipeline.pipeline-db.*`, `outbox.destination.*` |
| S3 checkpoint paths | Auto-namespaced by `jobId` — shared bucket, safe |

> **Why ranges, not single IDs?** Flink CDC 3.x incremental snapshot allocates IDs for
> parallel readers + restart attempts. A single int collides on restart because the
> previous MySQL binlog lease hasn't timed out.

---

## Slide 11 — CDC Snapshotting: Before vs After

**Post-Migration:** re-snapshotting is now native to Flink CDC.

![CDC Snapshotting: Before vs After — re-snapshot workflow](images/cdc-resnapshot-sequence.svg)

**What disappears:** `OneShotUnboundedSource`, `SnapshotSignalProcessFunction`, `SignalMessage` (3 Java classes) + signal Kafka topic
— **3 Java classes and 1 Kafka topic eliminated**.

> **Caution:** `stateless` is a one-shot re-snapshot lever — always revert to `last-state`.
> Left on permanently, **every** restart re-snapshots the full table.

---

## Slide 12 — POC Evidence

| Verification | Result |
|-------------|--------|
| Unit tests | 57/57 passing |
| All 8 modules compile | Clean |
| Format (Spotless — Google Java Format) | Compliant |
| Flink CDC 3.6.0 on Flink 2.2 | Verified |
| Per-variant component tests | Passing (5 Flink + 5 KC variants) |
| StatementSet → 1 JobGraph | Verified (SQL API only; Table API uses single INSERT, not StatementSet) |
| All 5 variants running simultaneously | Runs at POC scale (localhost:8081; 3 tables, 2 outbox destinations, in-memory state) |

> The POC validates the mechanism at POC scale — 5 variants, 57 unit tests, all green; outbox routing is logged to a single topic in the POC (per-destination side-output fan-out is production, Spike S3).
Production scale (15M-row table, ~15 destinations, RocksDB, prod failure modes) is the open spike work (S2/S3/S5).

---

## Slide 12b — POC Evidence: Live Screenshots

**All 5 Flink variants running simultaneously on localhost — captured during the live POC.**

### Flink Dashboard — 5/5 Jobs RUNNING

![Flink Dashboard — 5 variants running simultaneously](images/slides/flink-dashboard.png)

> All five CDC variants (DataStream, Table API, SQL API, Outbox, YAML Pipeline) live in one
> Flink cluster. Each has its own MySQL server-ID range; zero collisions.

### Kafka UI — poc Cluster (32 topics, 109 partitions)

![Kafka UI — poc cluster overview](images/slides/kafka-ui.png)

> Topics auto-created by CDC connectors. 32 topics = per-table topics for all 5 variants
> plus schema-history topics. Signal topics (`private.debezium.signal.*`) are KC-only;
> Flink CDC does not use them.

### Kafka Connect REST API — 5 KC Connectors (side-by-side comparison)

![Kafka Connect — 5 connectors list](images/slides/kafka-connect.png)

> KC connectors run in parallel for output comparison only. Server-IDs in the reserved
> `5500–5599` range to avoid collision with the Flink variants.

### Grafana Dashboard — Flink CDC POC Monitoring

![Grafana — Flink CDC POC Monitoring dashboard](images/slides/grafana-dashboard.png)

> 3 monitors shipped (mirroring Datadog): Restart Loop, Checkpoint Duration, Checkpoint Failures — all 5 variants green. Monitors #4–#7 (connector lag, snapshot status, binlog position) pending Spike S1.

---

## Slide 13 — Improvements Addressed

| Challenge (Slide 3) | Improvement |
|---------------------|-------------|
| Rebalancing storms — one bad connector destabilises all | **Isolated blast radius** — each tribe's Flink job is isolated; failure stays per-tribe |
| Shared blast radius — 95 connectors, one cluster | **Clear ownership** — tribe owns their connector repo and deploy cadence |
| Recurring lag — no per-tribe lever | **Per-job state** — exactly-once checkpoints give each job its own recovery point |
| Production-only failures | **Native Kubernetes lifecycle** — Flink Operator; local component tests catch issues before deploy |
| Confluent licensing | **Partial licensing savings** — 74 connectors removed from billable pool; 21 SFTP/SingleStore connectors retained on KC |
| Fleet-wide coordinated upgrades | **Independent upgrades** — per-job versioning; no fleet-wide coordination |

> **Note:** Exactly-once sink requires Kafka transactions (`DeliveryGuarantee.EXACTLY_ONCE` + transactional ID prefix in `KafkaSinkFactory`); Kafka broker must have transactions enabled.

---

## Slide 14 — The Trade-offs (Risk Register)

| Risk | Status / Mitigation | Where addressed |
|------|---------------------|-----------------|
| KC remains for 21 SFTP/SingleStore connectors — two systems to operate | Accepted; SFTP/SingleStore have no Flink equivalent | Slide 5 (scope), Slide 15b (TCO) |
| Field-level encryption: SMT logic must be ported to Flink `MapFunction` | Open; assessed per tribe during wave planning | S6 (cutover automation) |
| Learning curve — Flink Operator, checkpoints, savepoints | Mitigated for most tribes by shared-job model (no Java required for YAML/SQL/Table variants; DataStream still needs Java) | Slide 7 (decision tree), Slide 9 (shared-job) |
| Cutover sequencing — no wave plan, dual-run, parity gate, or rollback runbook yet | **Not yet mitigated** — S6 must deliver: wave plan, dual-run period, byte-for-byte parity gate, binlog server-ID overlap coordination, rollback runbook | Slide 16, Spike S6 |
| New operational surface — Flink Operator, checkpoints, savepoints | Mitigated by Flink Platform Team ownership of base image and monitoring module | Slide 17, Spike S1 |
| Observability regression — Debezium JMX metrics (lag, snapshot status, binlog position) have no direct Flink CDC equivalent; Datadog monitors #4–#7 blocked | **Not yet mitigated** — interim: Flink restart/backlog metrics + MySQL-side binlog-position checks as lag proxy; full resolution pending Spike S1 | Slide 17, Spike S1 |
| Schema evolution (ALTER TABLE) — behavior differs by variant; downstream Kafka schema compatibility not validated | **Not yet mitigated** — no dbhistory.* equivalent; validate per tribe + schema-registry compat policy | Slide 16, Spike S8 (new) |

---

## Slide 15 — Alternatives Considered & Reasoning

![Alternatives Analysis: Why Flink Shared-Job Model?](images/alternatives-analysis.svg)

> **Root cause:** the *shared-worker-pool* architecture — one cluster, one rebalance
> group, one blast radius. Any fix must remove the sharing *or* remove the blast
> radius. The table below is each option judged against that.

| Option | Why Not Chosen |
|--------|---------------|
| Stay on Confluent Kafka Cloud (status quo) | Blast radius, licensing cost, no per-tribe lever — the pain remains |
| Self-managed Kafka Connect (drop license) | Removes license cost but keeps shared blast radius + adds ops burden (not a managed service like Confluent) |
| Per-tribe dedicated KC clusters | Solves isolation but multiplies cost and operational overhead 26× (one KC cluster per tribe vs one shared) |
| Flink — per-tribe Java fork | True isolation, but every tribe maintains Java + a release pipeline |
| **Flink — shared-job model (chosen)** | Isolation + no per-tribe Java fork; one base image per variant, Helm-only overrides |

> **Reasoning:** Flink is the only option that removes blast radius **and** licensing
> cost. Within Flink, the shared-job model keeps the isolation win without forcing 26
> teams to each own Java code — the lowest-friction path to the same guarantees.
>
> **Framing:** scope is bounded — migrate the 74 MySQL CDC connectors to Flink; retain the 21 SFTP/SingleStore connectors on KC (no Flink equivalent). Flink is adopted where it fits, not as a blanket replacement.

---

## Slide 15b — Total Cost of Ownership (High-Level)

**Current state (Confluent KC):** one shared cluster bill covers all 95 connectors.

**Proposed state (Flink):** Confluent bill reduced to 21 connectors; Flink runs on existing K8s compute.

| Cost axis | KC today (95 connectors) | Flink proposal (74 CDC → Flink; 21 KC retained) |
|-----------|--------------------------|--------------------------------------------------|
| Confluent licensing | Full 95-connector bill | ~22% of connectors retained (21/95); pricing is per task/throughput, not strictly per-connector — see caveat |
| Compute (K8s CPU/RAM) | KC managed by Confluent (included in license) | One JM + TM pod pair per tribe; size per tribe against peak change rate (POC estimate: ~0.5 vCPU + 1 GB RAM at low binlog throughput; production sizing pending Spike S2) |
| Operational overhead | Shared cluster ops centralised | Per-tribe isolation; Flink Platform Team owns base image |
| Per-tribe migration cost | Zero (status quo) | S5/S6 spike deliverables (cutover automation) |

> **Caveat:** exact Confluent per-connector pricing depends on contract tier.
> The directional saving (74 connectors removed from billed pool) is certain;
> the K8s compute uplift should be sized against the existing KC worker fleet.

---

## Slide 16 — Open Spikes

| ID | Topic | Why It Matters | Phase | Timebox |
|----|-------|---------------|-------|---------|
| S1 | Flink metric parity — Debezium JMX metrics via Flink? | Determines monitoring module design; blocks #4–#7 KC monitor mapping | Phase 0 | 3 days |
| S2 | Initial snapshot memory pressure on largest table (~15M rows) | Prevents surprise in Phase 1/2 | Phase 0 | 2 days |
| S3 | Outbox multi-topic routing at scale (POC tests at 2; production outbox uses ~15 destinations) | Phase 1 go-live blocker | Phase 0 | 2 days |
| S4 | `snapshot.aborted`/`snapshot.running` Flink equivalent | outbox-transactron-connector migration (Phase 3) | Phase 0 | 2 days |
| S5 | Production failure modes (RDS IAM, binlog leases, IRSA rotation) | POC can't surface these; staging soak needed | Phase 1 | ≥7-day soak |
| S6 | Cutover automation (KC → Flink): wave plan, dual-run period, byte-for-byte parity gate, binlog server-ID overlap coordination, rollback runbook | No cutover plan exists yet; manual switches won't scale to 26 tribes | Phase 2 | ~5 days |
| S7 | Self-service Claude migration tooling for tribes | Tribes can't wait for Flink Platform Team hand-holding | Phase 1 | 3 days |
| S8 | Schema evolution — ALTER TABLE behavior per Flink variant; no dbhistory.* equivalent; downstream schema-registry compat policy | Per-tribe blast radius for schema changes; daily production reality | Phase 0 | 2 days |

**Phase 0 total (S1–S4, S8): ~11 engineering days — parallelisable within 1 sprint.**

**Phase legend:** 0 = spikes (pre-pilot) · 1 = first-tribe pilot go-live · 2 = expansion across tribes · 3 = outbox + transactron cutover

---

## Slide 17 — Centralised Monitoring: KC and Flink

| | Now (KC / Debezium JMX) | Gap | Target (Flink, post-S1) |
|--|-------------------------|-----|-------------------------|
| **Connector lag** | Debezium JMX `debezium.mysql:type=connector-metrics` → `MillisSinceLastEvent` | No direct Flink CDC equivalent | Flink source backlog metric (S1 investigation) |
| **Snapshot status** | Debezium JMX `snapshot.running` / `snapshot.aborted` | No equivalent yet (Spike S4) | Flink job status + custom metric via S1/S4 |
| **Binlog position** | Debezium JMX `source.pos` | No direct equivalent | MySQL-side binlog position check or Flink offset metric (S1) |
| **Restarts** | Kafka Connect worker restarts | ✅ Available — Flink `numRestarts` (Prometheus + Datadog) | Same |
| **Checkpoint health** | N/A (KC stateless) | ✅ Improvement — Flink `lastCheckpointDuration`, `numberOfFailedCheckpoints` | Same |

**Datadog monitors #4–#7** (connector lag, snapshot status, binlog position, snapshot abort) cannot be directly mapped until Spike S1 resolves Flink metric equivalents.

**Interim mitigation (pre-S1):**
- Monitor `numRestarts` as a lag proxy (repeated restarts → binlog position stale)
- MySQL-side: query `SHOW MASTER STATUS` + compare against last known Flink binlog offset from checkpoint metadata
- Datadog alert on Flink source `records.consumed.rate` dropping to 0

**Target state:** One shared Terraform module per platform (KC module owned by Module Owner; Flink module by Flink Platform Team), consumed by each tribe's `config.tf` — ~600 monitors across 26 teams at end-state.

---

## Slide 18 — Recommendation

**Adopt the shared-job Flink CDC model.** It removes the shared blast radius and licensing cost (see the Improvements slide) while keeping per-tribe isolation — and the POC validates the mechanism at POC scale (see the POC Evidence slide: 5 variants running simultaneously, 3 tables, 2 outbox destinations, in-memory state); production scale (15M-row table, ~15 destinations, RocksDB, prod failure modes) is pending S2/S3/S5.

Per-tribe cost is Helm overrides only — no per-tribe Java fork or release pipeline (YAML/SQL/Table variants need no Java; DataStream tribes customize the platform-owned image).

**Next step:** approve Phase 0 spikes (see the Open Spikes slide) — S2 and S3 are the Phase-1 go/no-go blockers.

**Phased timeline (indicative):**
- **Phase 0** (1 sprint, parallelisable): spikes S1–S4 + S8 (~11 engineering days) → metric-parity, memory, outbox-scale, snapshot-status, schema-evolution answers
- **Phase 1** (first-tribe pilot): pick one pilot tribe (`<pilot-tribe>`, TBD with guild); ≥7-day staging soak (S5); go-live gated on S2 + S3 + S5
- **Phase 2** (expansion): roll the shared-job model across tribes in waves (S6 cutover automation: dual-run, parity gate, rollback runbook)
- **Phase 3** (outbox + transactron): migrate outbox/transactron connectors (S4 unblocks)


---

## APPENDIX — Backup Slides (Not Part of the 45-Minute Talk)

> The three lists below are reference material for Q&A only. Do not present them live —
> they are here so you can jump to a specific table if asked a detailed infra question.

---

## Detailed Reference — POC Module Structure

### POC Module Structure (`flink-cdc-poc`)

```
flink-cdc-poc/
├── common/                             # JobConfig, CheckpointConfigurer, PocJsonDeserializationSchema, CdcEventRouter, OutboxRouter, KafkaSinkFactory, DdlValidator (~391 lines)
├── variant-flink-datastream-api-v1-cdc-job/   # DataStreamCdcJob.java  (50 lines, server-ID 5900–5999)
├── variant-flink-table-api-cdc-job/           # TableApiCdcJob.java    (99 lines, server-ID 6000–6099)
├── variant-flink-sql-api-cdc-job/             # SqlApiCdcJob.java      (156 lines, server-ID 5800–5899)
├── variant-flink-datastream-api-v1-outbox-job/ # OutboxJob.java        (56 lines, server-ID 5600–5699)
├── variant-flink-cdc-yaml-pipeline-cdc-job/   # pipeline.yaml         (47 lines, canonical: src/main/resources/pipeline.yaml, server-ID 5700–5709)
├── component-tests/                    # end-to-end: DataStreamCdcTest, TableApiCdcTest, SqlApiCdcTest,
│                                       #   DataStreamOutboxTest, YamlPipelineCdcTest,
│                                       #   KafkaConnectVariantTest, KafkaConnectOutboxTest
└── local-development/
    ├── podman-compose.yml              # MySQL + Kafka + Flink JM/TM + KC + kafka-ui + flink-cdc-submitter
    ├── flink-with-mysql/Dockerfile     # Flink 2.2 + mysql-connector-j
    ├── flink-cdc-submitter/            # runs flink-cdc.sh for YAML Pipeline variant
    ├── kafka-connect/                  # Debezium + custom SMTs; 5 connector JSON configs
    └── kafka-connect-smts/             # EnrichmentTransform + OutboxRoutingTransform (Java 11)
```

## Backup — Checkpoint Configuration (production-ready)

All five variants share one extraction point — `CheckpointConfigurer.applyExactlyOnce(env)` —
rather than repeating the five calls below in every entry class:

```java
// common/src/main/java/poc/common/checkpoint/CheckpointConfigurer.java
public static void applyExactlyOnce(StreamExecutionEnvironment env) {
    env.enableCheckpointing(30_000);
    env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
    env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
    env.getCheckpointConfig().setCheckpointTimeout(60_000);
    env.getCheckpointConfig().setMinPauseBetweenCheckpoints(5_000);
    env.getCheckpointConfig()
        .setExternalizedCheckpointRetention(
            ExternalizedCheckpointRetention.RETAIN_ON_CANCELLATION);
    // State backend (RocksDB + incremental) configured at cluster level via
    // FLINK_PROPERTIES: state.backend=rocksdb, state.backend.incremental=true
    // This keeps job code independent of the backend choice (operational flexibility).
}
```

Checkpoint state is persisted to S3-compatible storage (MinIO locally, AWS S3 in production),
configured in `flink-conf.yaml`:

```yaml
# Local POC: uses default HashMapStateBackend (no state.backend override needed)
# Production: set state.backend: rocksdb via cluster config or FLINK_PROPERTIES
state.checkpoints.dir: s3://flink-checkpoints/checkpoints
state.savepoints.dir:  s3://flink-checkpoints/savepoints
s3.endpoint: http://minio:9000
s3.path.style.access: "true"
s3.access-key: minioadmin
s3.secret-key: minioadmin
```

**POC evidence — MinIO `flink-checkpoints` bucket after all 5 variants running:**

![MinIO flink-checkpoints bucket — checkpoints and yaml-pipeline-checkpoints folders](images/slides/minio-checkpoints.png)

| Setting | Value | Reason |
|---------|-------|--------|
| `enableCheckpointing` | 30,000 ms | Balances durability vs performance |
| `CheckpointingMode` | EXACTLY_ONCE | Prevents duplicate Kafka messages on recovery |
| `MaxConcurrentCheckpoints` | 1 | CDC jobs snapshot during checkpoint; one at a time |
| `CheckpointTimeout` | 60,000 ms | 2× the interval; gives headroom for large-state jobs under load |
| `MinPauseBetweenCheckpoints` | 5,000 ms | Prevents checkpoint storms after one finishes |

---

## Infrastructure List 1 — Client (Production) Infrastructure

### Kubernetes

- **Flink Operator** — manages `FlinkDeployment` CRs; under pressure with 4 deployments in preview; TaskManager slot capacity must be watched
- **`FlinkDeployment` CRs** — one per job/variant; each with its own JobManager + TaskManager pod pair (Application Mode)
- **`FlinkStateSnapshot` CRs** — one per job, managed by chart
- **ClusterIP Services** — `<jobName>-rest` per job, port 8081
- **Helm chart: `flink-base-chart`** — `applicationJobs` map, init-container delivery, topology spread, probes, graceful shutdown, restart strategy
- **Namespace isolation** — per-variant schema + server-ID ranges enforced by values, not by chart

### Apache Flink

- **Flink runtime 2.2** — base image `flink-base-image` (Flink Platform Team)
- **Flink CDC 3.6.0** (suffix `3.6.0-2.2`) — bundled in variant images; version must match runtime
- **Built-in plugins** — `flink-s3-fs-presto-2.2.1.jar` (version-stamped, must match base image)
- **Built-in plugins** — `flink-metrics-prometheus-2.2.1.jar` (enables Prometheus scraping on port 9249)
- **`mysql-connector-j`** — mounted into both JobManager and TaskManager; classloader parent-first pattern required (`com.mysql.`)
- **Checkpointing** — per-job unique `checkpointing.dir`; exactly-once; S3-backed

### MySQL / Databases

- **MySQL binlog access** — Flink CDC reads binlog directly; requires `log-bin`, `binlog-format=ROW`, `binlog-row-image=FULL`
- **Per-variant schemas**: `cdc_db`, `sql_api_db`, `table_api_db`, `pipeline_db`, `outbox_db`
- **Non-overlapping `server-id` ranges**: outbox 5600–5699, pipeline 5700–5709, sql-api 5800–5899, shared-cdc 5900–5999, table-api 6000–6099; KC reserved 5500–5599
- **RDS IAM token rotation** — production-only failure mode (S5); IAM pattern captured in base image template
- **IRSA** — for checkpoint-store S3 permissions; rotation tested in ≥7-day Phase 1 soak
- **MySQL privileges** — `RELOAD` + `LOCK TABLES` required for initial snapshot

### Kafka

- **Kafka topics** (per-variant prefixes): `shared-cdc.cdc-db.*`, `sql-api.sql-api-db.*`, `table-api.table-api-db.*`, `pipeline.pipeline-db.*`, `outbox.destination.*`
- **Schema history topics** (KC/Debezium): `dbhistory.<variant>` — one per connector
- **Signal topic** (pre-migration only, dropped after): `private.debezium.signal.<connector>.v1`
- **Kafka Connect (Confluent)** — retained for SFTP (20) and SingleStore (1); 74 Debezium MySQL connectors migrated to Flink
- **Heartbeat topic** — KC monitor #1; Flink equivalent: Restart Loop + TM heartbeat

### Container Registry / Images

- `flink-base-image` — Flink runtime (existing)
- Proposed new base images (Shared Job / Platform Architect):
  - `flink-cdc-base-image`
  - `flink-stream-api-base-image`
  - `flink-table-api-base-image`
  - `flink-sql-api-base-image`
- Per-variant fat-jar images: 4 Flink fat-jars + 1 KC SMT shadow JAR; Shadow plugin

### Object Storage (S3)

- Shared S3 bucket for checkpoints/savepoints — auto-namespaced by `jobId`
- Per-job `checkpointing.dir` paths must not overlap

### Observability (Datadog via Terraform)

- **`<datadog-tf-repo>`** — central Terraform repo for all 26 teams (target state)
- **Shipped monitors** (3 Flink-specific confirmed: Restart Loop, Checkpoint Duration, Checkpoint Failures; full count across all teams tracked in `<datadog-tf-repo>`)
- **Shipped dashboards** (2): `[Platform] Flink Jobs Monitoring`, `[Platform] Flink CDC Streamer`
- **~600 monitors** at end-state across 26 teams — quota forecast needed (open item)
- **Notification routing**: 3 global channels (1/env) + per-team Slack/Zendesk/PagerDuty
- **Terraform modules**: `<kafka-datadog-tf-module>/kafka-connector-outbox` → `<datadog-tf-repo>/monitors/shared-definitions/kafka-connect` (Module Owner); new Flink module (Flink Platform Team)
- **Validation**: `terraform plan` + `*.tftest.hcl`; no `terraform apply` from branches

### IAM / Security

- **IRSA** — S3 checkpoint access; rotation must be tested
- **RDS IAM tokens** — short-lived; expiry surfaces only in production
- **Binlog leases** — MySQL binlog replica; server-ID lease timeout must be managed on restart

### Not Needed Post-Migration

- Kafka signal topic (`private.debezium.signal.*.v1`)
- `OneShotUnboundedSource`, `SnapshotSignalProcessFunction`, `SignalMessage` Java classes
- Confluent Kafka Connect for the 74 Debezium MySQL connectors
- `dbhistory.*` schema history topics for those 74 connectors — **replaced by in-job schema tracking in Flink CDC** (no external topic; ALTER TABLE behavior differs by variant — see Spike S8)

---

## Infrastructure List 2 — Local POC Infrastructure
*Source: `flink-cdc-poc/` folder (`podman-compose.yml`, `build.gradle`, `README.md`, `KAFKA_CONNECT.md`, `FLINK_CHECKPOINT_CONFIG.md`)*

### Software Versions

| Component | Version |
|-----------|---------|
| Apache Flink | 2.2.0 |
| Flink CDC | 3.6.0-2.2 |
| flink-connector-kafka | 5.0.0-2.2 |
| Kafka (Confluent) | KRaft mode, cp-kafka 7.6.1 |
| MySQL | 8.0 |
| Java (Flink jobs) | 17 |
| Java (Kafka Connect SMTs) | 11 (cp-kafka-connect 7.6.1 JDK) |
| mysql-connector-j | 8.0.33 |
| Gradle | 8.7 |
| Shadow plugin | 8.1.1 |

### Podman-Compose Services

| Service | Image | Port(s) | Role |
|---------|-------|---------|------|
| `mysql` | `mysql:8.0` | 3306 | CDC source; `log-bin`, `binlog-format=ROW`, `binlog-row-image=FULL`, `server-id=1` |
| `kafka` | `cp-kafka:7.6.1` | 9092 (ext), 29092 (int), 9093 (controller) | KRaft broker + controller; `auto.create.topics.enable=true` |
| `flink-jobmanager` | custom (Flink 2.2 + mysql-connector-j) | 8081 (REST), 6123 (RPC) | JobManager; 8 task slots; `taskmanager.slot.timeout=60000` |
| `flink-taskmanager` | custom (same image) | 8082, 6124 | TaskManager; 8 task slots |
| `flink-cdc-submitter` | custom | — | Runs `flink-cdc.sh` for YAML Pipeline variant on JM ready; `restart: on-failure` |
| `kafka-connect` | custom (Debezium + SMT JARs) | 8083 | KC REST API; side-by-side comparison; `restart: on-failure` |
| `kafka-ui` | `provectuslabs/kafka-ui:latest` | 8080 | Kafka topic browser |
| `minio` | `minio/minio:latest` | 9000 (API), 9001 (console) | S3-compatible checkpoint storage; `flink-checkpoints` bucket |
| `minio-init` | `minio/mc` | — | One-shot: creates `flink-checkpoints` bucket on startup |
| `prometheus` | `prom/prometheus:v2.52.0` | 9090 | Scrapes Flink JM/TM metrics every 15 s; local only |
| `grafana` | `grafana/grafana:10.4.3` | 3001 | Dashboard + alert rules (managed by Terraform); admin/admin |

### Gradle Modules TO MODIFY with links to README.md

| Module | Role |
|--------|------|
| `common` | `JobConfig`, `CheckpointConfigurer`, `PocJsonDeserializationSchema`, `CdcEventRouter`, `OutboxRouter`, `KafkaSinkFactory` |
| `variant-flink-datastream-api-v1-cdc-job` | DataStream CDC; server-ID 5900–5999 |
| `variant-flink-table-api-cdc-job` | Table API CDC; server-ID 6000–6099 |
| `variant-flink-sql-api-cdc-job` | SQL API CDC; server-ID 5800–5899 |
| `variant-flink-datastream-api-v1-outbox-job` | Outbox; server-ID 5600–5699 |
| `variant-flink-cdc-yaml-pipeline-cdc-job` | YAML Pipeline; server-ID 5700–5709 |
| `component-tests` | End-to-end: submits fat-jars to JM REST; polls Kafka; covers all 5 Flink + 5 KC variants |
| `kafka-connect-smts` | `EnrichmentTransform` + `OutboxRoutingTransform` (Java 11, shadow JAR) |

### Build & Test Commands TO MODIFY with links to README.md

| Command | What it does |
|---------|-------------|
| `./gradlew shadowJar` | Builds all 4 Flink fat-jars + KC SMT shadow JAR |
| `./gradlew :component-tests:test` | Runs all component tests (Flink + KC) |
| `./gradlew all` | Full cycle: build → podman-compose restart → wait for services (180 s) → deploy KC connectors → run CTs |
| `podman-compose -f podman-compose.yml up -d` | Starts the full 11-service stack |
| `podman exec flink-jm flink run /tmp/<jar>` | Submits a variant to the running JM |

### Kafka Connect Side-by-Side (POC only)

Five KC connectors mirror the Flink variants, using server-IDs in the reserved `5500–5599` range:

| KC Connector | Server-ID | SMT |
|-------------|-----------|-----|
| `kc-datastream-cdc` | 5510 | `EnrichmentTransform` |
| `kc-table-api-cdc` | 5520 | `EnrichmentTransform` |
| `kc-sql-api-cdc` | 5530 | `EnrichmentTransform` |
| `kc-yaml-pipeline-cdc` | 5540 | `EnrichmentTransform` |
| `kc-outbox-cdc` | 5550 | `OutboxRoutingTransform` |

### Local Monitoring Endpoints

| URL | What | Screenshot |
|-----|------|------------|
| `http://localhost:8081` | Flink Dashboard (running jobs, task slots, checkpoints) | ![](images/slides/flink-dashboard.png) |
| `http://localhost:8080` | Kafka UI (topics, messages) | ![](images/slides/kafka-ui.png) |
| `http://localhost:8083` | Kafka Connect REST API | ![](images/slides/kafka-connect.png) |
| `http://localhost:3306` | MySQL (user: `flink`, password: `flink`, db: `poc_db`) | — |
| `localhost:9092` | Kafka (external; topics: `poc.cdc.*`) | — |
| `http://localhost:9090` | Prometheus (Flink metrics scraper) | — |
| `http://localhost:3001` | Grafana (dashboard + alerts; admin/admin) | — |

---

## Infrastructure List 3 — Comparison: Client vs Local POC

| Area | Client (Production) | Local POC (`flink-cdc-poc`) |
|------|--------------------|-----------------------------|
| **Orchestration** | Kubernetes + Flink Operator + Helm (`flink-base-chart`) | Podman-compose (11 containers, bridge network) |
| **Flink deployment unit** | `FlinkDeployment` CR per job (Application Mode; own JM+TM) | Single shared JM + TM containers; all 5 variants submitted as jobs |
| **Flink version** | 2.2 (via `flink-base-image`) | 2.2.0 (custom Dockerfile: `flink-with-mysql`) |
| **Flink CDC version** | 3.6.0-2.2 (bundled in variant images) | 3.6.0-2.2 (Gradle dep in `build.gradle`) |
| **MySQL** | RDS (AWS); IAM auth; IRSA for S3; production data | `mysql:8.0` container; user `flink`/`flink`; `poc_db`; seed data via `init.sql` |
| **MySQL binlog server-ID** | Non-overlapping ranges 5600–6099 enforced by CI lint + base image template | Same ranges enforced by `JobConfig`; KC uses reserved 5500–5599 |
| **Kafka** | Confluent Kafka Cloud (managed) | `cp-kafka:7.6.1` KRaft container; single broker; `localhost:9092` |
| **Kafka Connect** | Confluent managed KC for SFTP (20) + SingleStore (1); being replaced for 74 CDC connectors | Local KC container + Debezium + custom SMTs; side-by-side comparison only |
| **Checkpointing** | S3 bucket (per-job `checkpointing.dir`); IRSA permissions | S3-compatible (MinIO) via `s3://flink-checkpoints`; HashMapStateBackend (state in-memory, checkpoints persisted to MinIO); same code config (30 s interval, EXACTLY_ONCE) |
| **CI/CD** | Jenkins (image build, `yq` delete, variant select) + ArgoCD (deploy/restart) | `./gradlew all` (build → compose restart → deploy connectors → CTs) |
| **Monitoring** | Datadog via `<datadog-tf-repo>` (16 monitors total, 2 dashboards; target: ~600) | Flink Dashboard `:8081` + Kafka UI `:8080` + KC REST `:8083` + Prometheus `:9090` + Grafana `:3001` |
| **Java version** | 17 (Flink jobs); SMT not applicable (no KC in production Flink path) | 17 (Flink jobs); 11 (KC SMTs — cp-kafka-connect 7.6.1 constraint) |
| **IAM / Security** | RDS IAM tokens, IRSA, binlog lease management | No IAM; plain `flink`/`flink` credentials; no rotation testing possible |
| **Re-snapshot** | `upgradeMode: stateless` + `restartNonce` in ArgoCD — **one-shot only**, revert to `last-state` immediately after | Cancel job, delete state, re-submit (`flink cancel <JOB_ID>` + `flink run`) |
| **State backend** | RocksDB (production recommendation; configured via cluster config) | In-memory / HashMapStateBackend (default for local demo) |
| **Kafka topic naming** | `<tribe>.<schema>.<table>` with per-variant prefixes across all 26 teams | `poc.cdc.<variant>.<table>` (single `poc_db` schema) |
| **Observability ownership** | Three-way: Module Owner (KC module) / Flink Platform Team (Flink module) / each tribe (config.tf) | Single developer; no ownership model needed |
| **Scale** | 74 CDC connectors → 26 teams → ~600 monitors at end-state | 1 schema (`poc_db`), 3 tables (`orders`, `customers`, `outbox_events`), 5 variants, 57 unit tests + CT per variant |
| **YAML Pipeline submission** | `flink-cdc.sh` via init-container or `kubectl exec`; `FlinkDeployment` comes up with empty JM until wired | `flink-cdc-submitter` container runs `flink-cdc.sh` automatically on JM ready |

---

## References

- [Apache Flink 2.2.0 documentation](https://nightlies.apache.org/flink/flink-docs-release-2.2/)
- [Apache Flink CDC 3.6 documentation](https://nightlies.apache.org/flink/flink-cdc-docs-release-3.6/)
- [Apache Flink CDC project home (GitHub)](https://github.com/apache/flink-cdc)
- [Debezium MySQL Connector documentation](https://debezium.io/documentation/reference/stable/connectors/mysql.html)
- [Kafka Connect overview (Apache Kafka docs)](https://kafka.apache.org/documentation/#connect)
- [Confluent Kafka Connect documentation](https://docs.confluent.io/platform/current/connect/index.html)
- [Flink Kubernetes Operator documentation](https://nightlies.apache.org/flink/flink-kubernetes-operator-docs-main/)
- [Apache Flink documentation (stable)](https://nightlies.apache.org/flink/flink-docs-stable/)
- [Flink Kubernetes Operator documentation (stable)](https://nightlies.apache.org/flink/flink-kubernetes-operator-docs-stable/)
- [Apache Flink CDC documentation (stable)](https://nightlies.apache.org/flink/flink-cdc-docs-stable/) ![Apache Flink CDC](images/flink-cdc-logo.png)
- [Flink CDC MySQL Connector documentation](https://nightlies.apache.org/flink/flink-cdc-docs-release-3.6/docs/connectors/flink-sources/mysql-cdc/)
- `flink-cdc-poc/FLINK_CHECKPOINT_CONFIG.md` — checkpoint semantics, monitoring, troubleshooting
- `flink-cdc-poc/FLINK_SAVEPOINT_RUNBOOK.md` — safe upgrade workflows, state recovery
- `flink-cdc-poc/KAFKA_CONNECT.md` — KC CDC variants, SMTs, Flink vs KC comparison
