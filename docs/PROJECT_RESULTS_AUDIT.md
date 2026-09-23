# Project Results Audit — Distributed Message Broker (mini-kafka)

**Audit date:** 2026-09-03
**Audited commit:** `45e6147` (branch `main`, clean except untracked `PROJECT_JOURNEY.md`)
**Auditor environment:** macOS Darwin 25.6.0, Apple Silicon (arm64), OpenJDK 21.0.11 (Homebrew), Gradle 9.6.1
**Docker daemon:** **not running during this audit** — every Docker-dependent result below is therefore *unreproduced*, not *disproven*.

> This file supersedes `docs/results.md` as the statement of *what is verified*.
> `docs/results.md` remains the harness-written record of individual runs; this file
> says which of those numbers are currently backed by evidence and which are not.

---

## 1. Original Project Objective

### What it was built to be
The **distributed-systems anchor of a job-search portfolio** (`distributed-message-broker.md` §A, `planning/01-requirements.md` BG-1..BG-4). Not a product; an artifact designed to survive a top-tier technical interview.

### The problem being solved
Portfolio distributed-systems projects almost universally *assert* correctness guarantees rather than prove them. The stated thesis (`CLAUDE.md`, `specs/11-chaos-harness.md` §Why) is that a from-scratch Raft-replicated broker whose invariants are **attacked by an automated fault-injection harness** is categorically more credible than one whose invariants are described in a README.

### The expected outcome
A Java 21 Kafka-style broker with:
- One Raft group per partition (multi-Raft), custom length-prefixed binary wire protocol over TCP, virtual-thread-per-connection.
- Durable append-only segment log with fsync + sparse offset index.
- Idempotent producer (producer-id + per-partition sequence) and at-least-once consumer with explicit offset commits.
- A chaos harness with four independent checkers (loss, duplication, linearizability, replica divergence) that runs against a real Docker Compose cluster with real `SIGKILL`s.

### The definition of success (as written at project inception)
Two claims, in priority order, from `distributed-message-broker.md` §A.2 and `README.md`:

1. **Correctness (the stated headline):** *0 committed-message loss and 0 duplication across ~1,000 injected leader crashes over ~10M messages*, with linearizability and replica-divergence checks passing. — `specs/11-chaos-harness.md` AC-1.
2. **Performance (the supporting claim):** *~200,000 msgs/sec at 1KB payloads, RF=3*, with *p99 publish→commit ~8 ms*. — `planning/01-requirements.md` NFR-7/NFR-8, `specs/12-benchmarks.md` AC-3/AC-4.

### What would genuinely impress a top-tier engineer
Not the throughput number — a laptop-hosted 3-broker cluster with fsync on the commit path was never going to reach Kafka-class throughput, and any interviewer will know it. What impresses is:
- A real consensus implementation with a *falsifiable* correctness claim and a harness that actually tries to falsify it.
- Honest, self-limiting measurement (stating the conditions and the known limits of the evidence).
- Bugs found *by* the harness and fixed, documented in commit history — evidence the harness works rather than decorates.

On the first two of these the project is strong. On the third it is genuinely strong: commits `17a1039` and `45e6147` are records of the harness and the demo finding real bugs (silently-unreplicated chaos topic, a global `synchronized` refresh lock that collapsed under failover, a demo script that printed `DEMO PASSED` on failure).

---

## 2. Is the Project Actually Complete?

### Verdict: **Functionally complete but insufficiently validated at the scale its own headline claim requires.**

Every one of the 15 specs (00–14) is implemented, wired, and reachable end-to-end. There are **no placeholders, no mocked results, no hardcoded outputs, and no TODO/FIXME markers anywhere in `src/main`** (verified by grep across all `*.java`, `*.kts`, `*.sh`, `*.yml`; the only two `UnsupportedOperationException`s are deliberate no-op stubs inside a property test). The full workflow — build → cluster up → publish → replicate via Raft → kill a leader → fail over → consume → verify — has actually been executed against real Docker at least once (`docs/results.md` demo row, 2026-08-11, corroborated by commit `45e6147`).

What it is **not** is validated at the scale the headline claim was written for. That is a distinct failure from being incomplete, and the distinction matters for the resume.

### Component-by-component status

| Component | Status | Evidence |
|---|---|---|
| `protocol/` — binary codec, 20+ message types | **Complete, tested** | 71 tests pass |
| `log/` — segments, sparse index, fsync policy, retention, crash recovery | **Complete, tested** | 50 tests pass, incl. `PartialWriteRecoveryTest`, `CrashRecoveryTest` (real subprocess kill via `CrashWriterMain`) |
| `raft/` — election, replication, log matching, term fencing, persistent state | **Complete, tested** | 17 tests pass, incl. `LogMatchingPropertyTest`, `LeaderCrashTest` |
| `client/` — producer/consumer, leader redirect, busy-retry, group consumer | **Complete, tested** | 21 tests pass |
| `broker/` — multi-Raft partitions, idempotency, backpressure, group coordinator, rebalancing | **Complete; 1 deterministically failing test** | 130 tests, **129 pass / 1 fail** (see §4.1) |
| `chaos/` — fault injector, 4 checkers, orchestrator, demo runner | **Implemented; validated only at ~0.01% of headline scale** | 31 unit tests pass; `chaosTest` last run 999 msgs / 4 crashes |
| `bench/` — 2 JMH benchmarks, load generator, results writer | **Implemented; executed once; raw output not retained** | 16 unit tests pass; no `results.json` in repo or build dir |
| CI (`.github/workflows/ci.yml`) | **Complete** | Runs `build test` + `:broker:integrationTest`; chaos/JMH deliberately excluded |
| Docker Compose cluster (3 brokers, RF=3, bench overlays) | **Complete** | `docker/docker-compose{,.bench,.bench-rf1,.chaos}.yml` |

### Components that exist but were never executed at their designed scale
- **`HeadlineCrashChaosTest` at `-Pcrashes=1000 -Pmessages=10000000`** — never run. Three independent hard blockers are identified in §6.1.
- **`LoadGenerator` 5-minute sustained run (spec 12 AC-6)** — no evidence in the repo that this was ever executed or recorded.
- **JMH `-Prf=1` / `-Prf=3` runs** — executed once (numbers are in `docs/results.md`), but no raw `results.json` was committed or retained, so the numbers cannot currently be traced to their source.

---

## 3. The Success Metrics That Actually Matter

| # | Metric | What it measures | Why it matters here | Target | Resume-worthy range | Where the target came from |
|---|---|---|---|---|---|---|
| M1 | **Committed-message loss under injected leader crashes** | Count of `ACKED` publishes never observed by any consumer (INV-1) | *This is the project's thesis.* A single lost message invalidates the entire correctness claim. | **0**, at ~1,000 crashes / ~10M messages | 0 loss at a scale large enough to be non-trivial — realistically ≥100 crashes over ≥1M messages | `specs/11-chaos-harness.md` AC-1; `CLAUDE.md` INV-1 |
| M2 | **Duplicate delivery under producer retries** | Any `(producerId, seqNo)` occupying two offsets (INV-3) | Idempotent-producer claim; distinguishes this from at-least-once-only systems | **0** | 0 at the same scale as M1 | `specs/11-chaos-harness.md` AC-1; INV-3 |
| M3 | **Linearizability of observed history** | Existence of a valid total order per partition explaining every read (INV-2/INV-5) | Separates "no loss" from "no *reordering*"; the hardest claim to fake | **PASS** | PASS over a real crash-laden history, not a quiescent one | `specs/11-chaos-harness.md` AC-1/AC-4 |
| M4 | **Replica divergence** | Any offset where two replicas' committed logs disagree (INV-5) | Direct split-brain detector; reads replica logs, not client responses | **PASS / 0 divergent offsets** | 0 across crashes *and* network partitions | `specs/11-chaos-harness.md` AC-1/AC-2 |
| M5 | **Split-brain events under network partition** | Two leaders of the same partition both committing (INV-4) | The classic consensus failure mode | **0** | 0 across ≥10 injected partitions with heal | `specs/11-chaos-harness.md` AC-2 |
| M6 | **Sustained throughput @ RF=3, 1KB** | Aggregate committed msgs/sec | The supporting resume number | **≥ 200,000 msgs/s** | See §6.2 — this target is not attainable by this architecture and should be reframed, not chased | `distributed-message-broker.md` §A.2; NFR-7. **Aspirational; no baseline derivation exists in the repo.** |
| M7 | **p99 publish→commit latency @ RF=3** | End-to-end producer-visible commit latency | Supporting number; pairs with M6 | **≤ ~8 ms** | p99 in the low single-digit ms at a *stated* offered concurrency | `distributed-message-broker.md` §A.2; NFR-8. **Aspirational.** |
| M8 | **RF=1 → RF=3 cost-of-durability delta** | Throughput/latency cost of majority replication | The best *defensible* performance talking point the project has | No pass/fail — **must be reported** | Any honest, well-conditioned number | NFR-9 (explicitly "no threshold, but must be reported") |
| M9 | **Bounded heap under a stalled consumer** | Broker heap over a sustained no-consumer run | Backpressure correctness (NFR-10) | **No OOM; flat heap** | Flat heap over ≥60s at high producer concurrency | NFR-10; `specs/10-backpressure.md` |
| M10 | **Automated test suite health** | Pass rate of `./gradlew test` | A green suite is table stakes; a red one undermines every other claim | **100% pass** | 100% pass, deterministic | `CLAUDE.md` §SDD workflow item 3 |
| M11 | **Crash-recovery / failover time** | Time from leader SIGKILL to a new leader accepting writes | The most common interview follow-up to "it fails over" | **No target was ever defined** | Sub-second to low-seconds, measured | **Not defined anywhere in the repo.** Stated here as a gap, not manufactured. |

---

## 4. Actual Verified Results

### 4.1 Results this audit measured directly (highest confidence)

#### M10 — Test suite health: **335 / 336 pass, 1 deterministic failure**

Command: `./gradlew test --continue --rerun-tasks` (JDK 21.0.11, this machine, 2026-09-03). Parsed from `*/build/test-results/test/*.xml`:

| Module | Tests | Failures |
|---|---|---|
| protocol | 71 | 0 |
| log | 50 | 0 |
| raft | 17 | 0 |
| client | 21 | 0 |
| chaos | 31 | 0 |
| bench | 16 | 0 |
| **broker** | **130** | **1** |
| **Total** | **336** | **1** |

**The failure:** `PartitionOrderingTest.concurrentProducersOnDifferentPartitionsPreserveEachOthersOrder` — `TimeoutException` at `PartitionOrderingTest.java:81`. It failed identically on two independent runs. **Root cause is confirmed, not suspected** (see §4.2): the test publishes 2×2,000 records synchronously with a 30-second budget, and the durable-write path on this machine cannot physically complete 8,000 serialized `fsync`s in 30 seconds.

**Two additional intermittent failures** were observed on the first full run of the suite and passed on re-run:
- `ConsumerResumeAfterFailoverTest.consumerResumesFromCommittedOffsetAfterFailover` — `ProtocolException` at line 56
- `FollowerKillTest.publishingContinuesAfterAFollowerIsKilled` — failed twice at *different* lines with *different* exceptions (`UncheckedIOException`/`ClosedChannelException` at line 36; `ProtocolException` at line 54), then passed

These are timing-sensitive, not deterministic. They are still a defect: a suite that fails ~2% of the time under load is not a suite you want a recruiter's CI to run.

#### M11 / root-cause evidence — Durable-write ceiling on this hardware: **231 fsync/s, 4.338 ms per fsync**

Measured directly with a standalone probe (`FileChannel.force(true)` on a temp file, host APFS, 1KB writes, 100 warmup + 2,000 timed):

```
force(true):                        2000 appends in 8.676 s -> 231 fsync/s, 4.338 ms each
group commit (1 fsync per 2000):    2000 appends in 0.015 s -> 131,066 appends/s
```

Conditions: host filesystem (macOS APFS, Apple Silicon), *not* through Docker. Fully reproducible — probe source retained at the audit scratchpad path; it is 25 lines and trivially re-derivable.

This single measurement explains essentially the entire performance story of the project, and it is the most useful number in this document.

#### Structural facts (verified by reading the code, not documentation)

- **Three serialized `fsync`s per publish on the leader**, on the critical path before the producer is acked:
  1. `FileRaftLogStore.append()` → `channel.force(true)` **unconditionally, once per entry**, inside a `synchronized` method called under `RaftNode`'s lock (`FileRaftLogStore.java:106,131`; `RaftNode.propose()` at `RaftNode.java:189`).
  2. `LogSegment.append()` → `force()` under the default `FsyncPolicy.EVERY_WRITE` (`LogSegment.java:167`; `LogConfig.DEFAULT_FSYNC_POLICY` = `EVERY_WRITE`).
  3. `AppliedIndexStore.record()` → `channel.force(true)` (`AppliedIndexStore.java:77`), ordered *after* the segment append (`PartitionReplica.java:121`).

  Each follower repeats (1) and (2)+(3) on its own apply, and the leader cannot commit until a majority has completed (1).
- **No batching exists anywhere in the write path.** `PublishReq` carries exactly one `payload` (`PublishReq.java`), `ProducerClient.publish()` is one synchronous request→ack round trip per message (`ProducerClient.java:141`), and `RaftNode.propose()` takes one `byte[] command` per Raft entry. Replication *fan-out* does batch (`config.maxEntriesPerAppend()` = 100 at `RaftNode.java:502`), but the leader's own durable append does not.
- **JMH configuration for the recorded benchmark run:** 8 threads (`bench.threads` default in `bench/build.gradle.kts`), 1 fork, 2 warmup + 5 measurement iterations × 5s, 1KB payload, 3 partitions, `bench` topic.
- **`HistoryRecorder` holds the entire chaos run history in JVM heap** as `ConcurrentLinkedQueue<ProducerEvent>` / `<ConsumerEvent>`, and `history()` then materialises it *twice more* (`new ArrayList<>(...)` then `List.copyOf(...)` in the record's compact constructor) — `HistoryRecorder.java:19,80,151`. The `chaosTest` task sets **no `maxHeapSize`**, so it runs at Gradle's 512 MB default (verified: no `maxHeapSize`/`Xmx` in any `build.gradle.kts` or `gradle.properties`).
- **`LinearizabilityChecker` documents its own scalability ceiling** in its class javadoc: *"a single very large, densely concurrent partition history (millions of ops with no quiescent gaps) is not tractable here — this checker is meant to run over per-scenario or per-partition windows of a chaos run, not the raw unbounded headline-run history."* (`LinearizabilityChecker.java:38-44`).
- **Fault injection is genuine.** `FaultInjector.killLeader()` resolves the *current* leader via live metadata and issues a real Docker `killContainerCmd(...).withSignal("KILL")` (`FaultInjector.java:37-45`); `partitionNetwork()` installs real `iptables -j DROP` rules. These are not simulated faults.

### 4.2 Results recorded by the project, **not reproducible during this audit**

Docker was not running, so none of the following could be re-executed. They are reported with their provenance and their evidentiary status.

| Metric | Recorded value | Source | Status |
|---|---|---|---|
| M1 Loss | **0** over 999 messages, 4 crashes | `docs/results.md` chaos table, row 1 (2026-07-28) | Harness-written row; **no raw artifact retained** (`chaos/build/` is clean, nothing committed). Unreproduced. |
| M2 Duplication | **0**, same run | same | same |
| M3 Linearizability | **PASS**, same run | same | same |
| M4 Divergence | **PASS**, same run | same | same |
| M5 Split-brain | **0 events**, 1 injected partition | `docs/results.md` network-partition table (2026-07-28) | Same status. Note: **1** partition injected, against AC-2's "N partitions between random broker pairs". |
| M6 Throughput RF=3 | **549 msgs/s** | `docs/results.md` throughput table | Written by `BenchResultsWriter` from JMH JSON. **The JSON no longer exists** in `bench/build/results/jmh/` and was never committed. Unreproduced. |
| M6 Throughput RF=1 | **1,668 msgs/s** | same | same |
| M8 RF=3 overhead | **67.1%** throughput reduction | same | Derived from the two above. |
| M7 Latency RF=3 | **p50 7.86 ms / p99 61.28 ms / p999 63.09 ms** | `docs/results.md` latency table | Same status. Measured at 8 offered threads, **not at a pinned target rate** — the benchmark's own javadoc is explicit that JMH has no rate limiter and this is offered-concurrency latency. |
| M9 Heap soak | **PASS** — 7 MB first-half max, 7 MB second-half max, 60s, 32 producer threads, queue capacity 1000 | `docs/results.md` backpressure table (2026-07-27) | Runnable *without* Docker via `./gradlew :broker:soakTest`. **Re-runnable today; not re-run in this audit.** Also proxied by `BoundedInFlightTest`, which does pass in the suite above. |
| Demo | **20,000 acked / 20,000 received, 0 loss, 0 dup, 0 offset gaps**, broker-2 SIGKILLed at 2,001 acks | `docs/results.md` demo table (2026-08-11), corroborated by commit `45e6147` | The best-corroborated Docker result in the project — the commit message documents two real bugs the run exposed and fixed. Unreproduced today. |
| M11 Failover time | — | — | **Not measured / no verified result available.** No target was ever defined and no measurement exists. |
| Spec 12 AC-6 (5-min sustained `LoadGenerator` run) | — | — | **Not measured / no verified result available.** |

---

## 5. Expected vs. Actual

| Metric | Target | Actual (verified) | Gap | Met? | Evidence | Reason |
|---|---|---|---|---|---|---|
| **M1 Loss** | 0 @ ~1,000 crashes / ~10M msgs | 0 @ **4 crashes / 999 msgs** | **~0.01% of target scale** | **Partially** — the guarantee held, at a scale ~250× below target in crashes and ~10,000× below in messages | `docs/results.md`; unreproduced | Headline run blocked by three hard limits (§6.1) |
| **M2 Duplication** | 0 @ same scale | 0 @ same reduced scale | same | Partially | same | same |
| **M3 Linearizability** | PASS @ headline scale + 10 randomized seeds | PASS @ validated scale; `RandomizedScheduleChaosTest` (10 seeds) passes | Scale gap only; AC-4 appears met | Mostly | `docs/results.md` narrative + `chaos/build/test-results/chaosTest/` (now cleaned) | Checker is documented-intractable at 10M ops |
| **M4 Divergence** | PASS @ headline scale | PASS @ validated scale | Scale gap | Partially | same | same |
| **M5 Split-brain** | 0 across N injected partitions | 0 across **1** injected partition | N=1 is thin for a "no split-brain" claim | Weakly | `docs/results.md` | Never re-run at higher N |
| **M6 Throughput RF=3** | ≥ 200,000 msgs/s | **549 msgs/s** | **~364× below target** | **No** | `docs/results.md`; unreproduced | Architectural, and fully explained: 3 serialized fsyncs/msg × ~4.3 ms + zero batching (§6.2) |
| **M7 p99 latency** | ≤ ~8 ms | **61.28 ms** @ 8 threads | **~7.7× above target** | **No** | same | Queueing on a ~550/s service rate at 8 offered threads; p50 of 7.86 ms is roughly at target, p99 is not |
| **M8 RF cost delta** | Report it | **67.1%** throughput reduction RF=1→RF=3 | None — it was reported | **Yes** | `docs/results.md` | Methodologically the cleanest number here: the RF=1 overlay spreads 3 partitions one-per-broker so both configs use all 3 brokers, isolating replication cost from broker-count cost |
| **M9 Bounded heap** | No OOM, flat heap | 7 MB / 7 MB flat over 60s | None | **Yes** | `docs/results.md`; `BoundedInFlightTest` passes today | Admission gate works |
| **M10 Test suite** | 100% pass | **335/336**, +2 intermittent | 1 deterministic + 2 flaky | **No** | This audit, §4.1 | Same fsync ceiling; 30s test budget vs. ~34.6s of required fsync time |
| **M11 Failover time** | *(never defined)* | **Not measured** | Unknown | n/a | — | Never instrumented |

---

## 6. Why the Results Landed Where They Did

### 6.1 Why the headline chaos claim was never validated at scale — **three confirmed blockers**

These are confirmed by code inspection and arithmetic, not speculation.

1. **Wall-clock infeasibility.** At the measured 549 msgs/s, 10M messages is **≈5.1 hours of pure publishing**, before adding 1,000 kill→elect→restart→`awaitHealthy` cycles (`ChaosOrchestrator.java:246-249` restarts with a 30-second budget each). Realistically 6–9 hours. `ChaosConfig.DEFAULT_RUN_TIMEOUT_MS` is 15 minutes; nothing in the repo suggests an overnight run was ever attempted.
2. **The harness will OOM long before 10M messages.** `HistoryRecorder` retains every producer and consumer event in heap, and `history()` triples the peak by copying twice (`HistoryRecorder.java:80,151`). At 10M produced + 10M consumed events that is tens of GB of live objects. The `chaosTest` task sets no heap size, so it runs at Gradle's **512 MB default**. This is a hard blocker independent of blocker 1.
3. **The linearizability checker is documented-intractable at that scale**, by its own author's javadoc (`LinearizabilityChecker.java:38-44`): its per-candidate readiness scan is O(n) per step and it is explicitly scoped to per-scenario/per-partition windows, not a 20M-event unified history.

**Confirmed cause, therefore:** AC-1 as literally written is not achievable with the harness as built. This was never a matter of "we ran out of time to run it" — the run would have failed on blocker 2 within minutes. The project's own documentation (`README.md`, `docs/results.md`, `MANUAL_TESTING.md` §9, commit `17a1039`) is consistently honest that the headline scale was not reached, but it frames this as a pending follow-up rather than as blocked, which understates the work required.

### 6.2 Why throughput is 549 msgs/s instead of 200,000 — **confirmed, architectural**

This is not a tuning problem and not primarily a hardware problem. Ranked by contribution:

1. **No batching, anywhere — confirmed, and the dominant cause.** One published message = one wire round trip = one Raft entry = one `fsync`. Kafka's headline throughput is *almost entirely* a batching result: producers accumulate records into batches, and a batch is one append, one replication unit, one fsync. This project's `PublishReq` cannot represent more than one record, so no amount of tuning can close the gap. The measured cost of this is stark: the same disk that does **231** durable single appends/s does **131,066** appends/s when 2,000 of them share one fsync — a **~567×** difference on the durability path alone.
2. **No group commit in the Raft log — confirmed.** `FileRaftLogStore.append()` forces on *every* entry, inside a `synchronized` block, under `RaftNode`'s lock. Even with concurrent producers on one partition, the durable appends serialize. A batching-free group-commit change (coalesce concurrent proposals into one `force()`) would help without touching the protocol.
3. **Three serialized fsyncs per publish, not one — confirmed.** Raft log + partition segment + applied-index marker. At 4.338 ms each on host APFS that is a **~77 msgs/s per-partition ceiling** before any network or replication cost. The applied-index fsync in particular buys crash-recovery idempotency, not durability of the record itself, and is a candidate for relaxation.
4. **Measurement environment — likely, evidence-supported.** All three brokers, the JMH harness, and the client shared one laptop, one CPU package, and one disk, inside Docker Desktop's VM. Three brokers each doing 2–3 fsyncs per message on the same physical device means the "3-broker cluster" is really one disk pretending to be three. Note also the inverse: 549 msgs/s across 3 partitions implies ~5.5 ms per message covering 3 leader fsyncs *plus* network *plus* follower fsyncs — which is **faster than 3× the 4.34 ms host fsync**, suggesting Docker Desktop's virtualised filesystem does not honour `F_FULLFSYNC` the way the host does. **This is not verified** and cuts against the durability story if true; it is listed in the action plan as something to check.
5. **JMH benchmark shape — confirmed contributor to the *latency* number.** `PublishLatencyBenchmark` runs at a fixed 8-thread offered concurrency with no rate limiter (its javadoc says so plainly). Against a ~550/s service rate, 8 saturating threads guarantee a queueing tail. The p99 of 61 ms is therefore a *saturated-system* number, not a p99-at-target-load number. p50 of 7.86 ms is the more meaningful figure and is roughly at NFR-8's target.

**On the target itself:** 200K msgs/s @ RF=3 and p99 ≤ 8 ms were written into `distributed-message-broker.md` §A.2 at project inception as *resume aspirations*. There is **no baseline, no capacity model, and no derivation** anywhere in the repo justifying them. They were never engineering targets; they were marketing targets that later got promoted into `planning/01-requirements.md` as NFR-7/NFR-8. Chasing 200K on this architecture on this hardware is not a realistic plan; reframing the performance claim honestly is.

### 6.3 Why one unit test fails deterministically — **confirmed by arithmetic**

`PartitionOrderingTest` publishes 2 partitions × 2,000 records synchronously with a 30-second budget. Even with an in-memory partition log, each publish still costs the Raft-log fsync **and** the applied-index fsync — 2 durable syncs × 4,000 records = **8,000 fsyncs**. At the measured 231 fsync/s that is **≈34.6 seconds**, against a 30-second timeout. The test is not wrong about ordering; it budgeted for a faster disk than this machine has. The two intermittent failures (`FollowerKillTest`, `ConsumerResumeAfterFailoverTest`) are the same pressure expressed as timing flakiness rather than a clean timeout.

### 6.4 Things that have *not* been tested (distinct from things that failed)

- Any hardware other than one Apple Silicon laptop. No multi-host, no cloud, no server-class NVMe.
- Throughput with fsync relaxed (`BROKER_FSYNC_POLICY=PERIODIC`/`OS_MANAGED`) — note this only relaxes the *segment* fsync; the Raft-log fsync is unconditional and has no policy knob.
- Failover time under any condition.
- Sustained `LoadGenerator` runs (spec 12 AC-6).
- Network partitions at N > 1.
- Whether Docker Desktop's virtualised fsync is actually durable (§6.2 item 4).

---

## 7. Resume Readiness

### 7.1 Safe to claim today — defensible under questioning

- **"Built a Raft-replicated, durable message broker from scratch in Java 21 — custom binary wire protocol, per-partition Raft consensus, idempotent producer, virtual-thread I/O; ~12K lines of production code with ~9.5K lines of tests across 336 tests."** Every number here is verified in §4.1.
- **"Built an automated fault-injection harness that SIGKILLs real partition leaders in a live 3-broker Docker cluster and verifies message loss, duplication, per-partition linearizability, and cross-replica divergence."** The harness exists, the faults are real Docker kills and real `iptables` DROP rules, and the checkers are genuine (the linearizability checker is a real bounded backtracking search, not an offset-monotonicity check).
- **"The harness found real bugs: a silently-unreplicated topic, a global lock in the client's metadata-refresh path that collapsed under failover, and a demo script that reported success on failure."** Documented in commits `17a1039` and `45e6147`. **This is the single most credible thing in the project** — it is evidence the harness does work rather than decorate, and it is exactly what a senior interviewer probes for.
- **"Quantified the cost of durability: majority replication (RF=3) costs 67% of RF=1 throughput on an otherwise identical 3-broker configuration."** Methodologically clean (the RF=1 overlay deliberately keeps all 3 brokers busy) and honest.
- **"Zero committed-message loss and zero duplication across a live broker SIGKILL mid-publish, verified over 20,000 messages."** — the 2026-08-11 demo row. Modest, specific, corroborated by a commit, and re-runnable in about 10 minutes.

### 7.2 Technically valid but not impressive

- The **549 msgs/s** and **p99 61 ms** figures. They are honestly obtained and honestly caveated in the README, but on a resume they invite exactly one question — "why so slow?" — and the answer ("no batching") reads as an incomplete design rather than a deliberate trade-off, unless you own it explicitly.
- **0 loss over 999 messages / 4 crashes.** True, but "4 crashes" is small enough that an interviewer may reasonably read it as a smoke test rather than a proof.
- **1 injected network partition** with 0 split-brain. One partition event does not support a split-brain claim.

### 7.3 Do **not** put these on a resume right now

- ❌ **"~200,000 msgs/sec at RF=3"** — off by ~364×. Claiming it would be false.
- ❌ **"p99 ~8ms"** — measured p99 is 61 ms. (p50 7.86 ms is real, but "p50" on a resume reads as evasion.)
- ❌ **"0 loss across 1,000 crashes / 10M messages"** — never run, and currently *cannot* be run without the fixes in the action plan.
- ❌ **"Fully tested / green CI"** — one test fails deterministically on the author's own machine and two more are flaky. Any interviewer who clones and runs `./gradlew test` will see red. This is the highest-embarrassment-per-unit-effort item in the whole audit.
- ⚠️ **Any benchmark number, until re-run and the raw `results.json` committed** — the current figures cannot be traced to an artifact. They are almost certainly accurate; they are simply not *evidenced*, and "show me" is a normal interview move.

### 7.4 Evidence needed before a stronger claim is possible

| Stronger claim | Evidence required |
|---|---|
| "0 loss / 0 dup across N leader crashes over M messages" with N,M worth citing | A chaos run at meaningful scale, which requires fixing the heap blocker first (Action 2, then 3) |
| Any throughput number that isn't embarrassing | Either batched publish (Action 5) or an explicit, owned reframe of the fsync-bound design (Action 4) |
| "p99 X ms at Y msgs/s" | A rate-limited latency measurement at a stated sub-saturation load (Action 4) |
| "Fails over in X ms" | Failover-time instrumentation — currently zero measurements exist (Action 6) |
| "Green on every commit" | Fix the deterministic timeout + the two flakes (Action 1) |

---

## 8. Reproducibility Status

| Result | Reproducible today? | Requires |
|---|---|---|
| Unit test suite (336 tests) | **Yes** — reproduced in this audit | JDK 21 + `JAVA_HOME` export |
| fsync ceiling (231/s, 4.338 ms) | **Yes** — measured in this audit | JDK 21 only |
| Heap soak (M9) | **Yes**, not re-run here | `./gradlew :broker:soakTest`, no Docker |
| Chaos results (M1–M5) | **No** — Docker not running; no raw artifacts retained | Docker + a fresh `./gradlew chaosTest` run |
| Benchmarks (M6–M8) | **No** — Docker not running; `results.json` not retained or committed | Docker + `./gradlew :bench:jmh -Prf=1` and `-Prf=3` |
| Demo (20K msgs, 0 loss) | **No** today, but the cheapest to restore (~10 min) | Docker + `./scripts/demo.sh` |

**Systemic reproducibility weakness:** no benchmark or chaos run in this project has ever had its raw machine-readable output committed. `docs/results.md` is a hand-off point where numbers stop being traceable. `.gitignore` excludes `build/` wholesale, so every artifact the harnesses produce is discarded. Fixing this is cheap and materially raises the credibility of every number.

---

## 9. Bottom Line

**What was built:** a genuinely complete, well-tested, well-documented from-scratch Raft-replicated broker with a real fault-injection harness. There is no vaporware here — no stubs, no mocked results, no fabricated numbers. The documentation is unusually honest, to the point of pre-emptively caveating its own headline table.

**What it achieved:** the correctness thesis is *supported* at small scale and *demonstrated convincingly once* (the 20K-message live-kill demo). The performance thesis failed by more than two orders of magnitude, for a confirmed architectural reason.

**What can be proven right now:** the test suite (minus one failure), the fsync ceiling, the code and harness structure. Everything Docker-dependent is currently unreproduced and unartifacted.

**Is it resume-ready?** The *correctness* story is nearly resume-ready and needs a few hours of work to become fully defensible. The *performance* story is not resume-ready and should be either reframed or fixed with batching — not quietly cited. The most urgent item is not any of that: it is that `./gradlew test` is red on the author's own machine.

See `docs/ACTION_PLAN.md` for the prioritised remaining work.
