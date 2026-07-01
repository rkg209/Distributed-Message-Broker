---
id: "07"
title: Replicated Partitions via Raft (Multi-Raft)
status: todo
phase: 4
depends_on: ["06"]
requirements: [FR-41, FR-42, FR-43, FR-44, NFR-1, NFR-5, INV-1, INV-2, INV-5]
invariants: [INV-1, INV-2, INV-5]
---

# Spec 07 · Replicated Partitions via Raft (Multi-Raft)

## What
Wire the `raft/` module into the `broker/` so each partition is backed by its
own Raft group, with the durable `PartitionLog` (Spec 03) as the replicated
state machine. A publish is acknowledged only after majority replication.
Consumers only read committed offsets.

## Why
This spec earns the core distributed-systems claim. INV-1 and INV-5 become
provable: committed data survives any single broker failure because majority
replication happened before the ack was sent.

## What to build

### `broker/` changes
- `PartitionReplica` — implements `StateMachine`; owns one `RaftNode` + one
  `PartitionLog`; `apply(raftIndex, command)` appends the decoded record to the log
- `RaftTransport` implementation using `PeerConnection` (from Spec 05) and the
  `protocol/` codec
- `PartitionManager` — initializes one `PartitionReplica` (and thus one `RaftNode`)
  per partition this broker participates in
- Publish path: `RequestHandler.handlePublish()` calls `PartitionReplica.append()`
  which calls `RaftNode.propose()` — **blocks until committed**
- Poll path: reads only up to `commitIndex` (INV-5)

### `raft/` changes
- `RaftLogStore` production implementation backed by `PartitionLog`
- `RaftTransport` implementation using the broker's `PeerConnection`

## Acceptance criteria
1. **Replication test (3 brokers, RF=3):** produce 1,000 messages; all three
   brokers' `PartitionLog`s contain the same 1,000 entries in the same order
   after the run.
2. **Follower kill test:** kill follower broker-3; production continues uninterrupted
   (majority = broker-1 + broker-2); consumer reads all messages correctly.
3. **Uncommitted-read test (INV-5):** a follower's partition log is inspected
   directly while broker-2 is partitioned (so broker-1+broker-3 form majority);
   a consumer poll to broker-2 returns at most the last committed offset, not
   any entries broker-2 received but the majority did not.
4. **End-to-end ack test (INV-1):** publish returns only AFTER majority has the
   entry (verified by inspecting broker-2 log before ack arrives at client).
5. All Spec 02 end-to-end tests still pass (with real replication now active).
6. `/raft-review` passes before marking done.
7. `./gradlew test` GREEN.

## Out of scope
- Leader failover / epoch fencing (Spec 08).
- Idempotent producer (Spec 09).
