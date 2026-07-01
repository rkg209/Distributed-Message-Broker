---
id: "09"
title: Idempotent Producer & Delivery Semantics
status: todo
phase: 5
depends_on: ["08"]
requirements: [FR-19, FR-20, FR-21, FR-22, FR-25, FR-26, NFR-2, NFR-3, INV-3]
invariants: [INV-3]
---

# Spec 09 · Idempotent Producer & Delivery Semantics

## What
Add producer-id + per-partition sequence numbers so that producer retries on
network error never create duplicate records. Pair with explicit consumer offset
commits (durable, surviving restarts) for effectively-once delivery semantics.

## Why
Without idempotence, the natural producer retry on a network hiccup inserts
duplicates — breaking INV-3 and the "0 duplication" claim. This spec closes
that gap, completing the "effectively-once" delivery story (idempotent producer
+ at-least-once consumer with explicit offset commits).

## What to build

### Broker-side deduplication (`broker/`)
- `IdempotencyStore` per partition: `ConcurrentHashMap<(producerId, partitionId), lastCommittedSeq>`
  — rebuilt from the log on recovery by scanning `producerId` and `seqNo` fields
- In `PartitionReplica.append()`:
  - `seq == lastCommitted + 1` → proceed
  - `seq <= lastCommitted` → return cached ack (duplicate, no re-append)
  - `seq > lastCommitted + 1` → return `SEQUENCE_GAP` error
- `LogRecord` format extended with `producerId` (8B) and `seqNo` (8B) fields

### Client-side (`client/`)
- `ProducerClient` — assigns `producerId` on first connect (generated client-side as UUID);
  maintains `Map<(topic, partition), long> nextSeq`; increments on each send; retries
  with the same `(producerId, seqNo)` on network error

### Offset commit durability (`broker/`)
- `OffsetStore` already exists (Spec 04); verify it survives kill -9 and restarts correctly
  (end-to-end test from Spec 04 re-verified here under failover conditions)

## Acceptance criteria
1. **Dedup test:** producer sends message with `(P, partition=0, seq=42)`; network drops
   the response; producer retries with same seq. Broker log contains exactly ONE record
   for seq=42. Consumer reads exactly one copy.
2. **Sequence gap test:** producer sends seq=44 after seq=42 (skipping 43); broker returns
   `SEQUENCE_GAP`; producer receives an error and does NOT silently lose the message.
3. **Recovery test:** broker is killed after committing seq=42. On restart, `IdempotencyStore`
   is rebuilt from the log; a retry of seq=42 is still detected as a duplicate.
4. **Consumer offset commit durability:** consumer commits offset 300; broker is killed;
   on restart consumer resumes from offset 300 with no re-delivery of 0–299.
5. **Chaos + dedup:** 100 messages produced with random network errors injected after
   each send (50% drop rate simulated). Consumer receives exactly 100 messages.
6. `./gradlew test` GREEN.

## Out of scope
- Full cross-partition transactions (explicitly excluded, CON-5).
- Backpressure (Spec 10).
