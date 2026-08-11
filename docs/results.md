# Results — Distributed Message Broker

> This file is the single source of truth for all headline numbers.
> Updated by `/update-results`, `/chaos-test`, and `/bench`.
> Replace placeholder values with measured results as specs complete.

---

## Chaos / Fault-Injection Results

| Run | Date | Crashes Injected | Messages | Loss | Duplication | Linearizability | Divergence | Result |
|-----|------|-----------------|----------|------|-------------|-----------------|------------|--------|
| 1 | 2026-07-28 | 4 | 999 | 0 | 0 | PASS | PASS | PASS |

**Headline target:** 0 loss / 0 duplication across ~1,000 crashes / ~10M messages. The run above is
at validated (not headline) scale — the full `-Pcrashes=1000` run over ~10M messages has not yet
been executed and is a follow-up, not part of Spec 11's close-out. `DiskSlownessChaosTest` (AC-3:
non-zero `BROKER_FSYNC_DELAY_MS` on the leader) and `RandomizedScheduleChaosTest` (AC-4: 10
randomized seeds) both pass but aren't captured by this table's columns; see
`chaos/build/test-results/chaosTest/` for their per-run JUnit XML.

---

## Throughput (1KB payload, 3-broker cluster)

| Metric              | RF=1        | RF=3        | RF=3 overhead |
|---------------------|-------------|-------------|---------------|
| Throughput (msgs/s) | 1,668/s | 549/s | 67.1% |

**Target:** ≥ 200,000 msgs/sec at RF=3.

---

## Publish-to-commit Latency (RF=3, 1KB payload)

| Percentile | Latency   |
|------------|-----------|
| p50        | 7.86 ms |
| p99        | 61.28 ms |
| p999       | 63.09 ms |

**Target:** p99 ≤ 8ms at RF=3 under target throughput.

---

## Backpressure Heap Soak (Spec 10, single broker, no consumer)

| Run | Date | Duration | Publish Queue Capacity | Producer Threads | First-half Max Used Heap | Second-half Max Used Heap | Result |
|-----|------|----------|------------------------|-------------------|--------------------------|----------------------------|--------|
| 1 | 2026-07-27 | 60s | 1000 | 32 | 7 MB | 7 MB | PASS |

Ran via `./gradlew :broker:soakTest` (`HeapSoakTest`, `@Tag("soak")`, excluded from `./gradlew test`).
32 virtual-thread producers publish as fast as possible with no consumer draining the partition;
used heap is sampled every 2s after a forced GC. Flat heap across the second half of the run
confirms the per-partition admission gate (`BackpressureController`, default capacity 1000) keeps
`RaftNode.pendingProposals` bounded rather than growing unboundedly with producer throughput
(NFR-10). `BoundedInFlightTest` is the fast, deterministic proxy for this same guarantee and runs
as part of `./gradlew test`.

---

## Network Partition Tests

| Run | Date | Partitions Injected | Split-brain Events | Result |
|-----|------|--------------------|--------------------|--------|
| 1 | 2026-07-28 | 1 | 0 | PASS |

---

## One-command demo (`scripts/demo.sh`)

| Run | Date | Messages | Kill point | Service killed | Result |
|-----|------|----------|------------|-----------------|--------|
| 1 | 2026-08-11 | 20,000 acked / 20,000 received | 2,001 acked | broker-2 (SIGKILL) | PASS |

Run end-to-end against a live 3-broker Compose cluster for the first time on 2026-08-11.
The first attempt actually surfaced two real bugs, both fixed before this row was recorded:

1. **False-positive `LossChecker` failure** (139 "lost" records, all in a contiguous tail
   range per partition, well after the kill point): `DemoRunner`'s post-load drain window
   only waited for a single 3-second quiet period before stopping consumers, and
   `demo.settleMs`/`demo.runTimeoutMs` were defined in `DemoConfig` but never actually wired
   to the `:chaos:demo` Gradle task, so a real Docker-backed, RF=3, fsync-on-commit run at
   this scale (10+ minutes wall clock) had no way to get a longer drain window than an
   in-process test needs. Fixed by wiring both properties through and requiring two
   consecutive quiet windows (raised default `settleMs` 3s → 10s) before concluding the
   consumers have actually drained — see `DemoRunner.run()` and `chaos/build.gradle.kts`.
2. **`scripts/demo.sh` printed `DEMO PASSED` even when the run failed**: `if ! cmd; then
   RUNNER_EXIT=$?; fi` captured the *negated* condition's exit status (always 0 on entering
   the `then` branch), not `cmd`'s real exit code, so the script's own claimed "exits with
   the verdict's own exit code" guarantee (spec 14) didn't hold. Fixed by using
   `cmd || RUNNER_EXIT=$?` instead.

After both fixes, a clean re-run produced 0 loss / 0 duplication / 0 offset gaps across all
20,000 messages with 1 real container kill mid-publish.

---

## Spec Completion Tracker

| Spec | Title | Status | Done Date |
|------|-------|--------|-----------|
| 00 | Foundations & Scaffolding | done | 2026-07-01 |
| 01 | Wire Protocol & Network Layer | done | 2026-07-02 |
| 02 | In-Memory Log + Thin Slice | done | 2026-07-21 |
| 03 | Durable Append-Only Log | done | 2026-07-21 |
| 04 | Partitions & Consumer Groups | done | 2026-07-21 |
| 05 | Cluster Membership & Metadata | done | 2026-07-22 |
| 06 | Raft Consensus Core | done | 2026-07-22 |
| 07 | Replicated Partitions via Raft | done | 2026-07-22 |
| 08 | Leader Failover & Epoch Fencing | done | 2026-07-27 |
| 09 | Idempotent Producer | done | 2026-07-27 |
| 10 | Backpressure & Flow Control | done | 2026-07-27 |
| 11 | Chaos Harness & Linearizability | done | 2026-07-28 |
| 12 | Benchmarks | done | 2026-07-28 |
| 13 | Dynamic Rebalancing (STRETCH) | done | 2026-07-28 |
| 14 | Demo & Polish | done | 2026-07-28 |
