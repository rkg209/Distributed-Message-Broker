---
id: "04"
title: Partitions & Consumer Groups (Static)
status: todo
phase: 3
depends_on: ["03"]
requirements: [FR-7, FR-8, FR-9, FR-27, FR-28, FR-29, NFR-2, NFR-3]
---

# Spec 04 · Partitions & Consumer Groups (Static)

## What
Extend the broker to support multiple partitions per topic with key-based
routing, and add a consumer-group abstraction with durable committed-offset
storage and static (non-rebalancing) partition assignment.

## Why
Partitioning is the unit of parallelism and ordering in the system. Consumer
groups with durable offsets are required before the chaos harness can verify
that consumers resume correctly after crashes. This spec completes the single-
broker feature set before distribution is added.

## What to build

### Partitioning (`broker/`)
- `PartitionRouter` — given a message key and partition count, returns the target
  partition ID (default: `murmur2(key) % numPartitions`; null key → round-robin)
- Update `PartitionManager` to manage N partitions per topic
- Update `MetadataService` to return partition count and partition→leader per topic

### Consumer groups (`broker/`)
- `ConsumerGroupManager` — stores committed offset per `(group-id, topic, partition)`
- `OffsetStore` — durable append-only file per group; recovers on restart
- Handle `COMMIT_OFFSET_REQ` / `COMMIT_OFFSET_RESP` in `RequestHandler`
- Static assignment: partition-to-consumer mapping configured at startup (not dynamic)

### Client (`client/`)
- `ProducerClient` — accept optional routing key; resolve target partition via router
- `ConsumerClient` — accept group-id; send `COMMIT_OFFSET_REQ`; on connect, fetch last
  committed offset from broker and resume from there

## Acceptance criteria
1. **Key routing test:** 10,000 messages produced with 10 distinct keys to a 4-partition
   topic. All messages with the same key land in the same partition and are in order.
2. **Per-partition ordering test:** two producers writing to different partitions in
   parallel; per-partition order is preserved; cross-partition order is not guaranteed.
3. **Consumer group offset durability test:** consumer in group G reads 500 of 1,000
   messages and commits offset 500. Process is restarted. Consumer resumes from offset 500
   — no re-read of 0–499, no gap.
4. **Independent groups test:** two groups (G1, G2) consuming the same topic maintain
   independent committed offsets; G1 commit does not affect G2.
5. `./gradlew test` GREEN with all above scenarios.

## Out of scope
- Dynamic rebalancing (stretch, Spec 13).
- Replication (Spec 07).
- Any Raft integration.
