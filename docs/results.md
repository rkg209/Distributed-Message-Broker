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
| 13 | Dynamic Rebalancing (STRETCH) | stretch | — |
| 14 | Demo & Polish | stretch | — |
