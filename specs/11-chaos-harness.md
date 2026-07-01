---
id: "11"
title: Chaos / Fault-Injection Harness & Linearizability Checker
status: todo
phase: 6
depends_on: ["10"]
requirements: [FR-50, FR-51, FR-52, FR-53, FR-54, NFR-1, NFR-2, NFR-3, NFR-4, NFR-6]
invariants: [INV-1, INV-2, INV-3, INV-4, INV-5]
---

# Spec 11 · Chaos / Fault-Injection Harness & Linearizability Checker

## What
Build the automated fault-injection harness that is the project's primary
correctness deliverable. The harness injects leader kills, network partitions,
and disk faults, runs the headline 1,000-crash test over ~10M messages, and
verifies zero loss and zero duplication. A linearizability checker validates
operation histories across randomized schedules.

## Why
"Correctness proven by a chaos harness" is the resume headline. Without this spec,
the project's invariant claims are unverified assertions. This is what separates
the project from typical portfolio work.

## What to build (all in `chaos/`)

### Fault injection
- `FaultInjector` — injects faults via Docker API:
  - `killLeader(partition)` — kills the container running the current leader
  - `partitionNetwork(brokerA, brokerB)` — adds iptables DROP rules between two brokers
  - `healNetwork(brokerA, brokerB)` — removes the iptables rules
  - `slowDisk(brokerId, latencyMs)` — uses `tc` to add latency to the broker's disk I/O path
- `ChaosOrchestrator` — top-level test driver:
  - Configures fault schedule (crash every N messages, or random interval)
  - Runs producers and consumers in parallel
  - Injects faults; waits for recovery; continues
  - Collects complete operation history

### Verification
- `HistoryRecorder` — thread-safe; records all `(producerId, seqNo, offset, timestamp, outcome)`
  events from producers (SENT, ACKED, TIMEOUT) and consumers (RECEIVED at offset)
- `DivergenceChecker` — after a test run, connects to all brokers and compares committed
  log contents per partition; reports any offset where replicas disagree (split-brain detector)
- `LinearizabilityChecker` — WGL-style checker over the recorded history; models each
  partition as a register; verifies that the observed history is consistent with some
  sequential execution
- `LossChecker` — verifies every ACKED message appears in some consumer's received set
- `DuplicationChecker` — verifies no offset appears more than once in any consumer's view

### Results reporting
- On test completion, write to `docs/results.md`:
  - Total messages sent/acked/received
  - Loss count, duplication count
  - Number of crashes injected, network partitions injected
  - Linearizability: PASS/FAIL
  - Divergence: PASS/FAIL

## Acceptance criteria (the headline)
1. **Headline test:** `./gradlew chaosTest -Pcrashes=1000` runs ~10M messages with
   ~1,000 induced leader crashes.
   - Result: **0 message loss, 0 duplication**.
   - `DivergenceChecker`: no offset where replicas disagree.
   - `LinearizabilityChecker`: PASS.
   - Results recorded in `docs/results.md`.
2. **Network partition test:** N injected network partitions between random broker pairs
   (healing after 5–10 seconds each). No split-brain; no data divergence.
3. **Disk slowness test:** random disk latency injected on the leader; no message loss.
4. **Randomized schedule test:** 10 distinct randomized producer/consumer schedules;
   linearizability checker passes all.
5. `./gradlew chaosTest` exits 0 on all-pass, non-zero on any failure.

## Out of scope
- Full Jepsen integration (custom checker is sufficient for this project's scope).
- Cross-partition linearizability (per-partition is the model).
