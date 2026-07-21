# Results — Distributed Message Broker

> This file is the single source of truth for all headline numbers.
> Updated by `/update-results`, `/chaos-test`, and `/bench`.
> Replace placeholder values with measured results as specs complete.

---

## Chaos / Fault-Injection Results

| Run | Date | Crashes Injected | Messages | Loss | Duplication | Linearizability | Divergence | Result |
|-----|------|-----------------|----------|------|-------------|-----------------|------------|--------|
| — | — | — | — | — | — | — | — | PENDING |

**Headline target:** 0 loss / 0 duplication across ~1,000 crashes / ~10M messages.

---

## Throughput (1KB payload, 3-broker cluster)

| Metric              | RF=1        | RF=3        | RF=3 overhead |
|---------------------|-------------|-------------|---------------|
| Throughput (msgs/s) | PENDING     | PENDING     | PENDING       |

**Target:** ≥ 200,000 msgs/sec at RF=3.

---

## Publish-to-commit Latency (RF=3, 1KB payload)

| Percentile | Latency   |
|------------|-----------|
| p50        | PENDING   |
| p99        | PENDING   |
| p999       | PENDING   |

**Target:** p99 ≤ 8ms at RF=3 under target throughput.

---

## Network Partition Tests

| Run | Date | Partitions Injected | Split-brain Events | Result |
|-----|------|--------------------|--------------------|--------|
| — | — | — | — | PENDING |

---

## Spec Completion Tracker

| Spec | Title | Status | Done Date |
|------|-------|--------|-----------|
| 00 | Foundations & Scaffolding | done | 2026-07-01 |
| 01 | Wire Protocol & Network Layer | done | 2026-07-02 |
| 02 | In-Memory Log + Thin Slice | done | 2026-07-21 |
| 03 | Durable Append-Only Log | done | 2026-07-21 |
| 04 | Partitions & Consumer Groups | todo | — |
| 05 | Cluster Membership & Metadata | todo | — |
| 06 | Raft Consensus Core | todo | — |
| 07 | Replicated Partitions via Raft | todo | — |
| 08 | Leader Failover & Epoch Fencing | todo | — |
| 09 | Idempotent Producer | todo | — |
| 10 | Backpressure & Flow Control | todo | — |
| 11 | Chaos Harness & Linearizability | todo | — |
| 12 | Benchmarks | todo | — |
| 13 | Dynamic Rebalancing (STRETCH) | stretch | — |
| 14 | Demo & Polish | stretch | — |
