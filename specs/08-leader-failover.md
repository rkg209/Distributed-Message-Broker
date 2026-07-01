---
id: "08"
title: Leader Failover & Epoch Fencing
status: todo
phase: 4
depends_on: ["07"]
requirements: [FR-44, FR-45, NFR-1, NFR-4, NFR-12, NFR-13, INV-1, INV-4]
invariants: [INV-1, INV-4]
---

# Spec 08 · Leader Failover & Epoch Fencing

## What
Make the system survive partition-leader death: detect the crash, elect a new
Raft leader, fence the old leader with a leader epoch, redirect
producers/consumers to the new leader, and resume consumers from the committed
offset with zero loss.

## Why
This is the headline correctness claim: "zero committed-message loss across
~1,000 induced leader crashes." Every invariant (INV-1 through INV-5) must hold
through a failover. Without fencing (INV-4), a stale leader could commit entries
after a new leader is elected, causing split-brain.

## What to build

### Epoch fencing (`broker/`)
- `PartitionManager` tracks `currentLeaderEpoch` per partition (= current Raft term)
- Any `AppendEntries` or client PUBLISH from a broker with epoch < `currentLeaderEpoch`
  is rejected with `STALE_LEADER_EPOCH`
- `RequestHandler` returns `NOT_LEADER` error when the local broker is not the Raft
  leader for the requested partition

### Client redirect (`client/`)
- `ProducerClient` / `ConsumerClient` — on `NOT_LEADER` error, call
  `MetadataClient.refresh()` and retry to the new leader
- `MetadataClient` — invalidates cache and re-queries on `NOT_LEADER`

### Failover observability
- Broker logs: `"Elected leader for partition=X term=Y"`, `"Stepping down term=Z>Y"`,
  `"Fencing stale leader request epoch=Z currentEpoch=Y"`
- `MetadataService` updates partition→leader map on Raft leadership change

## Acceptance criteria
1. **Basic failover (INV-1):** producer publishes a continuous stream to a 3-broker
   cluster. Kill the leader mid-stream. Within `2 × electionTimeout`, a new leader is
   elected. Consumer receives every committed message with no gap. Zero loss.
2. **Fencing (INV-4):** partition the old leader from the network BEFORE killing it;
   new leader is elected; old leader attempts a write → rejected with `STALE_LEADER_EPOCH`;
   no duplicate commits. Verify via broker logs and log inspection.
3. **Consumer resume:** consumer has committed offset 500 before the leader dies.
   After failover, consumer re-polls from 500 and receives 500 onward with no duplicate.
4. **Restart + rejoin:** the killed broker restarts; it receives `AppendEntries` from the
   new leader with term > its own; it transitions to FOLLOWER and syncs its log.
5. **Repeated failovers:** kill leader 10 times in succession; zero committed-message
   loss in the consumer's view after each failover.
6. `/raft-review` passes before marking done.
7. `./gradlew test` GREEN.

## Out of scope
- Idempotent producer (retry dedup — Spec 09).
- Backpressure (Spec 10).
- The 1,000-crash headline test (Spec 11 — this spec proves the mechanism works for 10).
