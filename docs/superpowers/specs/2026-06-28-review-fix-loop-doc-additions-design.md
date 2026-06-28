# Design — Review/Fix-Loop + Doc Additions

**Date:** 2026-06-28
**Branch:** master
**Status:** Approved (user confirmed in clarification Q&A)

## Goal

One user message, four deliverables. They run sequentially because the fix-loop
gates the screenshot capture (stacks must be healthy for screenshots).

1. **Review & fix-loop.** Run `/flink-cdc-fix-loop` to convergence:
   EC1 = `./gradlew all allK8s` exit 0; EC2 = 5/5 Podman jobs RUNNING on :8081;
   EC3 = 5/5 k8s jobs RUNNING across :18081–:18085 (1 per JM). Fix anything
   blocking on the way.

2. **Prereq installer script.** Add `scripts/install-prereqs.sh` (real,
   executable). Installs Java 17, Podman, Docker, kind, kubectl, Helm,
   Terraform, podman-compose, **Podman Desktop via flatpak** (user picked
   "Full prereq installer + flatpak Podman Desktop"). Idempotent — safe to
   re-run on a half-configured machine. Targets Ubuntu 24.04 (matches
   `INSTALLATION.md`). HOW-TO-RUN-THIS-POC.md gains a "Prerequisites" pointer
   to this script; INSTALLATION.md either cross-references or is folded in.

3. **Appendix rename + screenshots.** Rename `Kafka-Connect-At-Scale-Appendix.md`
   → `KAFKA-CONNECT-AT-SCALE-APPENDIX.md` (ALL CAPS — user picked this).
   Update every cross-reference: README.md, CLAUDE.md, K8S.md, the appendix
   itself (line 4 says "Main file: ..." — references the *deck*, not the
   appendix, so that one stays; the appendix's own header is the rename target).
   After the fix-loop converges (stacks healthy), capture 3 new screenshots
   referenced by the Local Monitoring Endpoints table:
   `images/slides/kafka-ui-topics.png` (Podman Kafka UI topic list showing
   `poc.flink.*` + `poc.kc.*` topics), `images/slides/podman-stack-running.png`
   (host terminal showing `podman ps` with all 11 services healthy),
   `images/slides/k8s-stack-running.png` (host terminal showing `kubectl get
   pods -n poc` with all FlinkDeployment pods Running).

4. **Commit + push.** Each fix-loop iteration commits (per the fix-loop skill).
   Doc additions commit when their part is done. Single push at the end, after
   all 4 deliverables verified.

## Non-goals

- Not writing a NEW design doc beyond this one — the existing presentation and
  docs are authoritative.
- Not changing the appendix's content beyond the rename + the 3 new screenshot
  rows.
- Not adding more screenshot sections beyond Local Monitoring Endpoints.
- Not adding a CI workflow for the install script — out of scope; the script is
  for humans running on a fresh VM.
- Not refactoring the install script for non-Ubuntu distros — INSTALLATION.md
  already scopes to Ubuntu 24.04.

## Sequencing & dependencies

```
  ┌─► (1) review & fix-loop iterations ─► (stacks healthy) ─►
  │                                                          │
  │                                                          ▼
  │                                  (3) appendix rename + screenshot capture
  │                                                          │
  │   (2) install-prereqs.sh written in parallel ───────────┤
  │       (does NOT need stacks; uses only apt/flatpak)     │
  │                                                          ▼
  └──────────────────────────── final commit & push ──────────────────►
```

(1) gates (3) for the screenshot capture only; (2) is independent and can be
written while (1) iterates. (4) waits for (1)+(2)+(3).

## Risks & mitigations

| Risk | Mitigation |
|---|---|
| `./gradlew all allK8s` fails with a stale IDLE Gradle daemon | Run `./gradlew --stop` first per fix-loop skill step A |
| Stale `terraform.tfstate` drift | Per fix-loop skill: do not delete tfstate; `terraform apply` is idempotent |
| kind cluster from previous run has stale finalizers | `kind delete cluster --name flink-cdc-poc` first |
| Fix-loop iterates 3+ times and uses a lot of context | Commit each iteration; defer push until convergence per fix-loop skill |
| Screenshot capture needs both stacks healthy — they're torn down between fix-loop iterations? | Fix-loop skill leaves Podman up; k8s is torn down by `allK8s` end. For k8s screenshot, do one extra `./local-development-k8s/deploy.sh` + `port-forward.sh start`, capture, then teardown |
| Podman Desktop flatpak install requires `flatpak` setup (flathub remote, user permissions) | Script includes `flatpak remote-add --if-not-exists flathub` and a `--user` flag fallback for permission-denied cases |
| `install-prereqs.sh` runs `apt update` and pulls network — slow on fresh VM | Script accepts `--yes` / `-y` flag; defaults to non-interactive. Print progress so user can see what's happening |
| The 3 new screenshots are taken manually (no headless browser for Kafka UI / kubectl output) | Use `gnome-screenshot` / `grim` / `flameshot` for the desktop, and image-render text outputs with `import` or `cut`-based ASCII if no GUI is available. Defer to manual capture in user instructions if tooling unavailable |

## Acceptance criteria

- [ ] `./gradlew all allK8s` → exit 0
- [ ] `curl http://localhost:8081/jobs/overview` → 5 RUNNING
- [ ] `curl http://localhost:18081..18085/jobs/overview` → 1 RUNNING each (5 total)
- [ ] `scripts/install-prereqs.sh` exists, is executable, runs cleanly on a fresh Ubuntu 24.04 VM, and installs every prereq mentioned in CLAUDE.md / INSTALLATION.md
- [ ] `HOW-TO-RUN-THIS-POC.md` references the new script
- [ ] File renamed: `KAFKA-CONNECT-AT-SCALE-APPENDIX.md` exists, old name gone
- [ ] `README.md`, `CLAUDE.md`, `K8S.md` cross-references point at the new name
- [ ] 3 new screenshot files exist under `images/slides/`: `kafka-ui-topics.png`, `podman-stack-running.png`, `k8s-stack-running.png`
- [ ] `Kafka-Connect-At-Scale-Appendix.md` → `KAFKA-CONNECT-AT-SCALE-APPENDIX.md` Local Monitoring Endpoints table has the 3 new rows
- [ ] All commits pushed to master

## Out of scope (explicit YAGNI)

- No changes to the fix-loop skill itself.
- No changes to component tests.
- No changes to flink/flink-cdc versions.
- No documentation beyond the 4 named files (HOW-TO-RUN-THIS-POC.md, README.md, CLAUDE.md, K8S.md) plus the new script + new screenshots.
