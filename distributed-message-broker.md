# Distributed Message Broker (mini-Kafka)

> A from-scratch, fault-tolerant message broker in **Java 21** that durably stores messages in an append-only log, replicates each partition across brokers using **Raft consensus**, and survives leader crashes with **zero committed-message loss and zero duplication** — proven by a chaos harness, not claimed.
>
> This is the **distributed-systems anchor** of the portfolio. It is built **spec-driven** with Claude Code: every feature starts as a spec, becomes a plan, then an implementation with tests, then a measured result. This file is the single source of truth for the project — the resume framing, the locked design decisions, the Claude Code setup, and the ordered backlog of specs to build.

---

## Part A — The Resume & Interview Layer

This is what a recruiter or interviewer sees first. Everything below it exists to make these claims true.

### A.1 One-line description
A distributed, replicated message broker (a mini-Kafka) with a durable append-only log, Raft-based partition replication, automatic leader failover, and an idempotent producer — with correctness proven under thousands of injected crashes and network partitions.

### A.2 Headline results (targets to earn, then report your measured numbers)
These are the numbers the project is engineered to produce. Replace the ranges with your actual measured results once the chaos harness and benchmarks run.

- **Correctness under failure (the headline):** 0 message loss and 0 duplication across **~1,000 induced leader crashes over ~10M messages**.
- **Partition tolerance:** recovered from N injected network partitions with **no data divergence / no split-brain**.
- **Consistency:** a linearizability/consistency checker passes across X randomized operation schedules.
- **Throughput:** sustained **~200K msgs/sec at 1KB payloads, replication factor 3**.
- **Latency:** **p99 publish→commit ~8 ms at RF=3**.
- **Cost-of-guarantee:** quantified throughput delta between RF=1 and RF=3 (the measured price of durability).

### A.3 Resume bullet variants (pick one or two; lead with the measured result)
- *Built a distributed message broker in Java 21 with a durable append-only log and Raft-replicated partitions; proved **0 message loss / 0 duplication across ~1,000 induced leader crashes** via an automated chaos-test harness.*
- *Engineered partition replication and automatic leader failover on a custom Raft implementation; sustained **~200K msgs/sec at RF=3** while maintaining per-partition ordering and an idempotent producer.*
- *Designed a custom length-prefixed binary wire protocol over TCP using Java 21 virtual threads; built a fault-injection harness (leader kills, network partitions, disk faults) and a linearizability checker as the project's correctness deliverable.*

### A.4 Skills this proves (recruiter / ATS keyword surface)
Distributed systems · consensus (Raft) · leader election & failover · replication · durability (append-only log, fsync, segment files, memory-mapped index) · exactly-once / idempotent producer · message ordering · partitioning · consumer groups & offset management · backpressure / flow control · custom network protocol design · Java 21 virtual threads · high-concurrency servers · chaos / fault-injection testing · linearizability testing · benchmarking (JMH) · Docker Compose multi-node clusters · systems design.

### A.5 Interview talking points (the "genuinely hard parts" you can now defend)
Each of these is a question an interviewer will probe; building the project gives you a real answer.

- **Durability:** how a write survives a crash — append-only segments, the fsync policy you chose, the sparse memory-mapped offset index, and recovery on restart.
- **Replication & commit safety:** why a write only commits after a majority of the Raft group has it, and how that guarantees no committed data is lost on leader death.
- **Failover without split-brain:** how a new leader is elected, how **leader epochs / fencing** stop a stale leader from corrupting the log, and why two leaders can never both commit.
- **Ordering & delivery semantics:** how per-partition ordering is preserved, what "exactly-once" honestly means here (idempotent producer + offset-committed consumers = effectively-once), and the trap of over-claiming it.
- **Backpressure:** why a slow consumer can't take the broker down.
- **The cost of the guarantee:** the measured throughput/latency you pay for RF=3 vs RF=1 — a great "engineering tradeoff" discussion.

---

## Part B — Locked Design Decisions (do not re-litigate during the build)

These were decided up front so that every spec is built against a fixed target. The rationale is included because it *is* the interview answer.

| # | Decision | Choice | Why |
|---|----------|--------|-----|
| 1 | **Replication / consensus** | **Raft, one Raft group per partition** (multi-Raft) | Makes "no loss, no dupes, no split-brain" true *by construction* from one provably-correct algorithm. Cleanest correctness story. (Honest framing: real Kafka uses an ISR + controller model, not Raft per partition — so describe this as "Kafka-style broker with Raft-replicated partitions," never as literal Kafka internals.) |
| 2 | **Network protocol** | **Custom length-prefixed binary protocol over TCP**, virtual-thread-per-connection blocking IO | Earns "network protocol design" honestly and exercises Java 21 virtual threads. Blocking IO on virtual threads is far simpler to get correct than NIO selectors while scaling similarly. |
| 3 | **Delivery semantics** | **Idempotent producer** (producer-id + per-partition sequence numbers, dedupe on append) **+ at-least-once consumer with explicit offset commits** | Delivers a defensible "effectively-once" guarantee. Full Kafka-style cross-partition transactions are explicitly **out of scope** (optional far-future stretch) to keep the project bounded. |
| 4 | **Consumer groups** | **Static partition assignment in core**; dynamic rebalancing is a clearly-marked **stretch spec** | Keeps the durability/replication centerpiece unblocked. Rebalancing is impressive but must not gate the headline. |
| 5 | **Storage** | Append-only **segment files + fsync + sparse memory-mapped offset index**; **retention by size/time only, no log compaction** | Standard, defensible durability design. Compaction is a large add with little extra signal; left out (stretch at most). |
| 6 | **Cluster membership** | **Static configuration** via Docker Compose (fixed broker list); a lightweight metadata/controller role tracks partition→broker assignment | A resume project doesn't need dynamic membership discovery; static membership removes a whole class of complexity without weakening the story. |
| 7 | **Build & tooling** | **Gradle (Kotlin DSL)**, JUnit 5, JMH, Testcontainers, Docker Compose, GitHub Actions CI | Mainstream Java tooling that reviewers recognize; reused across the other portfolio projects. |
| 8 | **Consumer model** | **Pull** (consumer polls `poll(topic, offset)`) | Matches the spec and Kafka; gives consumers control of pace and is the natural fit for backpressure. |

---

## Part C — Architecture Overview

```
                 custom binary protocol over TCP (virtual-thread-per-connection)
Producer ──publish(topic, key, msg)──► Broker = partition leader (Raft leader for that partition)
                                          │  append to local log segment, assign offset
                                          ▼
                                  Raft AppendEntries → follower brokers
                                          │  entry commits once a MAJORITY of the Raft group has it
                                          ▼
                              committed offset advances (only committed data is readable)
                                          ▼
Consumer group ──poll(topic, partition, offset)──► ordered messages ; commit offset after processing

On leader crash:  the partition's Raft followers elect a new leader (higher term),
                  leader epoch fences the old leader, committed entries survive,
                  consumers resume from their committed offset → no loss, no dupes, no split-brain.
```

**Units of the system**
- **Topic** — a named stream, split into partitions.
- **Partition** — the unit of ordering, parallelism, and replication. Each partition is its own Raft group with one leader and N−1 followers.
- **Log** — per-partition append-only sequence of records on disk (segments + index).
- **Offset** — monotonic position of a record within a partition.
- **Consumer group** — a set of consumers sharing the read load of a topic; each group tracks a committed offset per partition.
- **Controller/metadata** — lightweight role that knows which broker leads which partition (statically configured membership, dynamically updated leadership).

### Tech stack
Java 21 (virtual threads), custom TCP binary protocol, custom append-only log + memory-mapped index, a from-scratch Raft implementation, JUnit 5 + a custom chaos/fault-injection harness, a linearizability checker, JMH for benchmarks, Docker Compose for the multi-broker cluster, GitHub Actions for CI. Build with Gradle (Kotlin DSL).

---

## Part D — Spec-Driven Development with Claude Code

The project is built as a sequence of **specs**. Each spec is a short markdown document describing *what* and *why* (not line-level *how*); Claude Code turns each spec into a plan, then an implementation with tests, then a measured result. This section is the complete Claude Code setup — deliberately **opinionated: only scaffolding that earns its place is included.**

### D.1 Repository layout
```
distributed-message-broker/
├─ CLAUDE.md                     # project constitution (see D.2)
├─ specs/                        # the SDD backlog — one file per spec (see Part E)
│   ├─ 00-foundations.md
│   ├─ 01-wire-protocol.md
│   └─ ...
├─ docs/
│   ├─ architecture.md           # the diagram + design rationale (interview ammo)
│   └─ results.md                # chaos + benchmark results tables (auto-updated)
├─ .claude/
│   ├─ skills/                   # invocable workflows = slash commands (see D.3)
│   ├─ agents/                   # subagents (see D.4)
│   └─ settings.json             # hooks + permissions (see D.5)
├─ broker/                       # broker server module
├─ raft/                         # reusable Raft consensus module
├─ log/                          # append-only log + index module
├─ protocol/                     # wire protocol + codec module
├─ client/                       # producer/consumer client library
├─ chaos/                        # fault-injection + linearizability harness
├─ bench/                        # JMH benchmarks + load generator
├─ docker/                       # Dockerfile + docker-compose.yml (multi-broker)
└─ build.gradle.kts
```

### D.2 `CLAUDE.md` (project constitution — keep it under ~200 lines)
Paste this into `CLAUDE.md` at the repo root. It anchors every Claude Code session so the locked decisions and invariants are never re-litigated.

```markdown
# Distributed Message Broker — Claude Code Constitution

## What this is
A from-scratch, Raft-replicated, durable message broker (mini-Kafka) in Java 21.
Headline guarantee: zero committed-message loss and zero duplication under crashes,
PROVEN by the chaos harness. Correctness is the deliverable.

## Locked decisions (do NOT re-litigate — see distributed-message-broker.md Part B)
- Replication: Raft, ONE Raft group per partition (multi-Raft).
- Protocol: custom length-prefixed binary over TCP, virtual-thread-per-connection.
- Delivery: idempotent producer (producer-id + per-partition seq) + at-least-once
  consumer with explicit offset commits. NOT full cross-partition transactions.
- Storage: append-only segments + fsync + sparse mmap offset index; retention by
  size/time only; NO log compaction.
- Membership: static config (Docker Compose); lightweight controller tracks leadership.
- Consumer groups: static assignment in core; rebalancing is a stretch spec only.
- Build: Gradle Kotlin DSL, JUnit 5, JMH, Testcontainers, Docker Compose.

## Invariants that must ALWAYS hold (assert these in tests)
1. A committed write is never lost, even across leader crashes.
2. Per-partition ordering is preserved for a given producer.
3. No duplicate delivery from producer retries (idempotent producer dedupes on append).
4. Two leaders for the same partition can never both commit (leader epoch fences stale leaders).
5. Consumers only read committed (majority-replicated) data.

## SDD workflow (follow for every feature)
1. Specs live in /specs, numbered, in dependency order. Build them IN ORDER.
2. For a spec: /spec-plan first (produce a plan, get it reviewed), THEN /implement-spec.
3. Test-first where practical. A spec is DONE only when: code compiles, unit +
   property tests pass, and (where applicable) chaos/bench targets produce results
   recorded in docs/results.md.
4. Any replication/consensus change MUST be reviewed by the distributed-systems-reviewer
   subagent before being considered done.

## Build / test / run commands
- Build:            ./gradlew build
- Unit tests:       ./gradlew test
- Format:           ./gradlew spotlessApply
- Chaos (headline): ./gradlew chaosTest -Pcrashes=1000
- Benchmarks:       ./gradlew jmh
- Cluster up/down:  docker compose -f docker/docker-compose.yml up / down

## Conventions
- Java 21. Use virtual threads for connection handling; one virtual thread per connection.
- Keep the consensus, log, protocol, and broker concerns in separate modules.
- No silent failure: durability/replication errors propagate, never swallowed.
- Every public guarantee has a test that tries to break it.
```

### D.3 Skills (`.claude/skills/<name>/SKILL.md`) — these are your slash commands
In current Claude Code, custom slash commands are **skills**: a `SKILL.md` with YAML frontmatter, invocable as `/name`, and optionally auto-invoked by description. Build these; each one is a repeatable step in the SDD loop.

| Skill (`/command`) | Purpose | Notes |
|---|---|---|
| `/spec-new` | Scaffold a new spec file in `/specs` from a fixed template (what / why / acceptance criteria / out-of-scope). | Keeps every spec uniform and high-level. |
| `/spec-plan` | Read a spec and produce an implementation plan (modules touched, test plan, risks) *before* any code. | Core SDD gate. Consider `context: fork` so planning runs in a subagent and doesn't pollute main context. |
| `/implement-spec` | Implement the current spec against its plan, test-first, updating only the modules in scope. | Takes the spec number as an argument (`$ARGUMENTS`). |
| `/chaos-test` | Run the fault-injection harness with a crash count / partition count and summarize pass/fail. | e.g. `/chaos-test 1000`. Delegates to the `chaos-runner` subagent so huge output stays out of context. |
| `/bench` | Run JMH benchmarks, parse the output, and write the throughput/latency table to `docs/results.md`. | Delegates to `benchmark-analyst`. |
| `/raft-review` | Audit consensus/replication code against the five invariants (split-brain, lost commit, stale leader, ordering, double-commit). | Delegates to `distributed-systems-reviewer`. Run before marking any consensus spec done. |
| `/update-results` | Pull the latest chaos + benchmark numbers into `docs/results.md` and the README headline table. | Keeps the resume numbers honest and current. |

Minimal skill file shape (example: `/spec-plan`):
```markdown
---
name: spec-plan
description: Turn a spec in /specs into a reviewed implementation plan before any code is written. Use whenever starting a new spec.
context: fork
---
Read the spec file passed in $ARGUMENTS. Produce a plan with: modules touched,
data structures, the test plan (unit + property + chaos as applicable), the
invariants it must preserve, and the top risks. Do NOT write implementation code.
```

### D.4 Subagents (`.claude/agents/<name>.md`) — isolated context for heavy work
Subagents get their own context window and tool set; use them for tasks that produce huge output or need deep, focused reasoning so the main session stays clean.

| Subagent | Job | Why a subagent |
|---|---|---|
| `distributed-systems-reviewer` | Reviews replication/consensus/failover code against the five invariants; hunts for split-brain, lost-commit, and ordering bugs. | The single most important review in the project; deserves a focused, fresh context. |
| `chaos-runner` | Executes long crash/partition loops, returns only failures + a summary. | Chaos output is enormous; isolate it and return only the actionable result. |
| `benchmark-analyst` | Runs JMH, parses results, returns clean throughput/latency tables. | Keeps verbose benchmark logs out of the main conversation. |
| `test-author` | Writes property-based and concurrency tests for a target module. | Parallelizable grunt work; can run on a cheaper model. |

Example frontmatter (`distributed-systems-reviewer`):
```markdown
---
name: distributed-systems-reviewer
description: Expert reviewer for consensus, replication, and failover correctness. Use after any change to the raft/ or broker/ replication code, before marking the spec done.
tools: Read, Grep, Glob, Bash
model: opus
---
You are a distributed-systems correctness reviewer. Check the changed code against
the five project invariants (no lost committed write; per-partition ordering;
no producer-retry duplicates; no two committing leaders / stale-leader fencing;
consumers read only committed data). Report concrete violation scenarios, not style.
```

### D.5 Hooks (`.claude/settings.json`) — deterministic quality gates
Hooks fire on lifecycle events and enforce rules even when you're not watching. Keep them aligned with the project's correctness focus.

| Event | Action | Purpose |
|---|---|---|
| `PostToolUse` on file edit (`*.java`) | run `./gradlew spotlessApply` then compile-check | format + catch breakage immediately after every edit |
| `PreToolUse` on `Bash` | block destructive commands (`rm -rf`, force-push, etc.) | safety; deterministic, not "please don't" |
| `Stop` / `SubagentStop` | run the fast unit-test subset before a task is considered finished | enforce the "definition of done" from CLAUDE.md |
| `SessionStart` | print which spec is in progress + last results summary | orient every session inside the SDD flow |
| `PreCompact` | snapshot spec-progress notes / transcript | don't lose context across long consensus debugging sessions |

> Hooks can also be scoped *inside* a skill or subagent's frontmatter so they only fire while that component runs (e.g. a `PreToolUse` guard that runs only during the `chaos-runner` subagent). Use this to keep global hooks minimal.

### D.6 MCP servers — only one earns its place
For a self-contained Java systems project, most MCP servers add nothing. The one that does:

- **GitHub MCP** — track each spec as an issue, open PRs per spec, watch CI status, and let Claude Code drive the issue/PR workflow. Worthwhile because a resume project lives on GitHub and the green-CI / clean-PR history is itself part of the impression.
  `claude mcp add github -- npx -y @modelcontextprotocol/server-github`

Everything else (databases, browser automation, etc.) is irrelevant here — deliberately omitted rather than padded in.

### D.7 Plugins — optional, only once the workflow stabilizes
A **plugin** bundles the skills, subagents, and hooks above into one installable unit. You don't need it for a single repo. Once this SDD setup is proven, you can package `.claude/` as a plugin (e.g. `sdd-systems-kit`) so the same workflow drops into your *other* portfolio projects (Payments Ledger, Matching Engine) without copy-pasting. Treat it as a nice-to-have after the broker is done, not a prerequisite.

---

## Part E — The Specs (ordered backlog)

Build these **in order** — each depends on the ones before it. Every spec is intentionally **high-level (what + why + acceptance criteria)**; the exact technical implementation is written later, per spec, via `/spec-plan`. The first three specs deliver a **thin end-to-end slice** (a working single-broker, single-partition publish/consume) before any distribution complexity is added.

> Acceptance criteria below are the "definition of done." A spec isn't finished until its criteria are met and any results are recorded in `docs/results.md`.

### Phase 1 — Thin end-to-end slice

**Spec 00 · Foundations & scaffolding**
Gradle multi-module project, CI (GitHub Actions: build + test), logging, configuration loading, and the module skeletons (`protocol`, `log`, `raft`, `broker`, `client`, `chaos`, `bench`).
*Done when:* `./gradlew build` is green in CI and empty modules wire together.

**Spec 01 · Wire protocol & network layer**
A custom length-prefixed binary request/response protocol over TCP, with a virtual-thread-per-connection server and a minimal client. Define framing, message types (publish, poll, metadata), and serialization.
*Done when:* a client can round-trip a request to the server over the real protocol; malformed frames are rejected cleanly.

**Spec 02 · In-memory log + single-topic publish/consume (THIN SLICE)**
Topic/partition abstraction, in-memory append with monotonic offset assignment, and `poll(offset)` returning ordered records. One broker, one partition, no persistence yet.
*Done when:* a producer publishes N messages and a consumer reads exactly those N, in order, end-to-end over the wire. **This is the first working slice.**

### Phase 2 — Durability (single broker, survives restart)

**Spec 03 · Durable append-only log**
Persist each partition to disk as rolling **segment files** with a chosen **fsync** policy, a **sparse memory-mapped offset index**, crash-safe recovery on startup, and **retention by size/time**.
*Done when:* a broker restart loses zero acknowledged messages; recovery rebuilds offsets correctly; old segments are reclaimed by the retention policy.

### Phase 3 — Partitioning & consumer groups (single broker)

**Spec 04 · Partitions & consumer groups (static)**
Multiple partitions per topic, key-based partition routing, per-partition ordering, consumer-group abstraction, and durable **committed-offset storage per group**. Static (non-rebalancing) partition assignment.
*Done when:* messages with the same key land in the same partition and stay ordered; a group resumes from its last committed offset after a consumer restart.

### Phase 4 — Distribution: replication & failover (the centerpiece)

**Spec 05 · Cluster membership & metadata**
Static broker configuration, a lightweight **controller/metadata** role tracking partition→leader assignment, broker-to-broker connectivity, and failure detection (heartbeats).
*Done when:* brokers form a cluster from static config; the controller reports a consistent partition→leader map; a dead broker is detected.

**Spec 06 · Raft consensus core (reusable module)**
A from-scratch Raft implementation in the `raft/` module: persistent state, leader election, log replication (`AppendEntries`), `RequestVote`, commit-index advancement, and safety properties — independent of the broker.
*Done when:* a standalone Raft cluster elects a stable leader, replicates a command log, commits on majority, and re-elects after a leader is killed; tested in isolation.

**Spec 07 · Replicated partitions via Raft (multi-Raft)**
Make each partition its own Raft group, with the durable append-only log (Spec 03) as the replicated state machine. A publish commits only after a **majority** of the partition's group has the entry; only committed offsets are readable.
*Done when:* a 3-broker cluster replicates a partition at RF=3; killing a follower doesn't affect availability; consumers never read uncommitted data.

**Spec 08 · Leader failover**
Detect partition-leader death, elect a new leader, **fence the old leader with a leader epoch**, redirect producers/consumers to the new leader, and resume consumers from the committed offset.
*Done when:* killing a partition leader mid-stream causes a clean failover with **no committed-data loss and no split-brain**; a stale leader cannot commit after fencing.

### Phase 5 — Delivery semantics & resilience

**Spec 09 · Idempotent producer & delivery semantics**
Producer-id + per-partition **sequence numbers**, dedupe-on-append so producer retries never create duplicates, and explicit consumer **offset-commit** semantics for at-least-once → effectively-once delivery.
*Done when:* replaying/retrying producer requests yields no duplicate records; consumer offset commits survive restarts; the "effectively-once" guarantee is test-backed.

**Spec 10 · Backpressure & flow control**
Bounded queues, producer throttling under load, and slow-consumer handling so a lagging or stalled consumer cannot exhaust broker memory or crash it.
*Done when:* a deliberately slow consumer leaves the broker and other consumers unaffected; broker memory stays bounded under a fast producer.

### Phase 6 — Proof & numbers (the deliverables)

**Spec 11 · Chaos / fault-injection harness + linearizability checker**
Automated fault injection (kill leaders, partition the network, slow/fill the disk), the **headline ~1,000-crash / ~10M-message test**, and a **consistency/linearizability checker** over randomized op schedules.
*Done when:* the headline test reports **0 loss / 0 duplication**; network-partition tests show no divergence; the checker passes across randomized schedules. Results in `docs/results.md`.

**Spec 12 · Benchmarks (JMH + load generator)**
Throughput at varying message size and replication factor, publish→commit p50/p99 latency, and the **RF=1 vs RF=3 cost comparison**, plotted.
*Done when:* `docs/results.md` holds throughput/latency tables and plots, including the cost-of-durability delta.

### Phase 7 — Stretch (only after the headline is solid)

**Spec 13 · Dynamic consumer-group rebalancing (STRETCH)**
A group coordinator that reassigns partitions when consumers join or leave, with a rebalance protocol.
*Done when:* adding/removing a consumer triggers a clean rebalance with no double-consumption and no gap.

**Spec 14 · Demo & polish (deliverable)**
README with the architecture diagram and headline results table, a Docker Compose cluster, and a **live "kill a broker, watch zero loss" demo script**, plus `make`/Gradle targets (`chaosTest`, `jmh`, `up`).
*Done when:* a stranger can clone the repo, run the cluster, kill a broker, and watch the no-loss guarantee hold — from the README alone.

**(Far-future, explicitly out of scope unless you choose to extend):** full cross-partition transactions / read-committed isolation, and log compaction. Mentioned here only so their absence is a deliberate, defensible scoping decision rather than an oversight.

---

## Part F — Build order summary & cadence

1. **Specs 00–02** — thin slice working (publish/consume over the real protocol).
2. **Spec 03** — durable, survives restart.
3. **Specs 04** — partitions + consumer groups.
4. **Specs 05–08** — the distributed centerpiece: membership → Raft → replication → failover. *This is where the resume claim is earned; don't rush it.*
5. **Specs 09–10** — idempotency + backpressure.
6. **Specs 11–12** — chaos proof + benchmarks → the numbers that go on the resume.
7. **Specs 13–14** — stretch rebalancing + the demo.

Rough effort: this is the most ambitious of the portfolio (~5–7 weekends). The riskiest, highest-signal work is Specs 06–08; everything before them is setup, everything after is proof and polish.

---

## Part G — What to ship (the artifact a reviewer actually opens)
- **Public GitHub repo** with green CI.
- **`README.md`** — architecture diagram + the headline results table (0 loss / 0 dupes across N crashes; ~200K msgs/sec @ RF=3; p99 ~8ms) + a one-command demo.
- **`docs/architecture.md`** — the design rationale (Part B decisions) = your interview script.
- **`docs/results.md`** — chaos + benchmark tables and plots.
- **A 60-second demo:** `docker compose up`, kill a broker mid-publish, show the consumer still receives every message in order.

That demo + that results table is the whole point: a claim about distributed-systems correctness that a skeptic can reproduce in one command.
