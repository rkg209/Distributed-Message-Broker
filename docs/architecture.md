# Architecture — Distributed Message Broker

> This document is the interview reference. It covers every locked design decision
> and the rationale behind it. Read this before a technical interview.

---

## System Overview

A Kafka-style broker built from scratch in Java 21. Producers publish messages to
named topics; the broker persists each message to an append-only disk log, replicates
it to a majority of peer brokers using Raft consensus, and acknowledges the write only
after that majority is reached. Consumers poll partitions by offset and receive committed
messages in strict append order.

**The correctness guarantee:** zero committed-message loss and zero duplication under
arbitrary leader crashes — proven by the chaos harness, not asserted.

---

## Module Map

```
broker/ → raft/, log/, protocol/
client/ → protocol/
chaos/  → client/
bench/  → client/
raft/   → (standalone, no broker deps)
log/    → (standalone, no broker deps)
protocol/ → (standalone, value types + codec only)
```

No module may introduce a dependency that violates this map.

---

## Architecture Diagram

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

---

## Locked Design Decisions

### 1. Replication: Raft, one group per partition

**Choice:** Each partition is its own Raft group (multi-Raft).

**Why:** Makes "no loss, no dupes, no split-brain" true by construction from one
provably-correct algorithm. Cleanest correctness story.

**Interview note:** Real Kafka uses an ISR + controller model, not Raft per partition.
Describe this as "Kafka-style broker with Raft-replicated partitions", never as
"implementing Kafka internals."

### 2. Network protocol: custom length-prefixed binary over TCP

**Choice:** `[4-byte length][1-byte type][payload]`; all fields big-endian; one virtual
thread per connection.

**Why:** Earns "network protocol design" honestly and exercises Java 21 virtual threads.
Blocking IO on virtual threads is far simpler to get correct than NIO selectors while
scaling similarly. No gRPC, no Netty, no Protobuf.

### 3. Delivery semantics: idempotent producer + at-least-once consumer

**Choice:** Producer-id + per-partition sequence numbers, dedupe-on-append, explicit
consumer offset commits.

**Why:** Delivers a defensible "effectively-once" guarantee. Full cross-partition
transactions are explicitly out of scope — they would dwarf the project without adding
proportional interview signal.

### 4. Consumer groups: static assignment in core, dynamic rebalancing as a stretch spec

**Choice:** Partition-to-consumer mapping configured at startup is the core delivery
model. Dynamic rebalancing shipped as Spec 13 on top of it: `GroupCoordinator` tracks
group membership and triggers a rebalance on join/leave/session-timeout, `RangeAssignor`
computes the new partition assignment, and consumers coordinate over
`JOIN_GROUP`/`GROUP_HEARTBEAT`/`LEAVE_GROUP` RPCs.

**Why:** Keeps the durability/replication centerpiece unblocked first. Rebalancing was
scoped as a stretch spec specifically so its scheduling and API-negotiation edge cases
couldn't threaten the correctness centerpiece.

### 5. Storage: segment files + fsync + sparse mmap index

**Choice:** Append-only `.log` + `.index` files; retention by size/time; no compaction.

**Why:** Standard, defensible durability design. Compaction is a large add with little
extra signal.

### 6. Cluster membership: static Docker Compose config

**Choice:** Fixed broker list; no gossip protocol.

**Why:** Removes a whole class of complexity without weakening the distributed-systems
story. Dynamic membership is irrelevant to the correctness headline.

### 7. Build & tooling: Gradle Kotlin DSL, JUnit 5, JMH, Testcontainers, Docker Compose

**Why:** Mainstream Java tooling that reviewers recognize.

### 8. Consumer model: pull-based

**Choice:** Consumers poll the broker for records at an offset they control
(`POLL_REQ`/`POLL_RESP`), rather than the broker pushing records to consumers.

**Why:** A pull-based consumer sets its own pace and never needs the broker to buffer
per-consumer backlog for a slow reader — the broker just serves whatever offset is
asked for, out of the same durable log every consumer reads from. It's also what makes
replaying from an arbitrary committed offset (crash recovery, group rebalance, a
consumer restarting from its last commit) a normal read path rather than a special
case.

---

## The Five Invariants

Every architectural decision exists to preserve these. Every invariant has at least
one automated test that tries to violate it.

| # | Invariant |
|---|-----------|
| INV-1 | A committed write is never lost, even across leader crashes. |
| INV-2 | Per-partition ordering is preserved for a given producer. |
| INV-3 | No duplicate delivery from producer retries (idempotent producer deduplicates on append). |
| INV-4 | Two leaders for the same partition can never both commit (leader epoch fences stale leaders). |
| INV-5 | Consumers only read committed (majority-replicated) data. |

---

## Wire Format

```
┌──────────────┬───────────┬──────────────────────────────────┐
│ length (4B)  │ type (1B) │ payload (length - 1 bytes)       │
└──────────────┴───────────┴──────────────────────────────────┘
```

| Type byte | Name                | Direction      |
|-----------|---------------------|----------------|
| 0x01      | PUBLISH_REQ         | Client→Broker  |
| 0x02      | PUBLISH_RESP        | Broker→Client  |
| 0x03      | POLL_REQ            | Client→Broker  |
| 0x04      | POLL_RESP           | Broker→Client  |
| 0x05      | COMMIT_OFFSET_REQ   | Client→Broker  |
| 0x06      | COMMIT_OFFSET_RESP  | Broker→Client  |
| 0x07      | METADATA_REQ        | Client→Broker  |
| 0x08      | METADATA_RESP       | Broker→Client  |
| 0x09      | FETCH_OFFSET_REQ    | Client→Broker  |
| 0x0A      | FETCH_OFFSET_RESP   | Broker→Client  |
| 0x10      | APPEND_ENTRIES_REQ  | Broker→Broker  |
| 0x11      | APPEND_ENTRIES_RESP | Broker→Broker  |
| 0x12      | REQUEST_VOTE_REQ    | Broker→Broker  |
| 0x13      | REQUEST_VOTE_RESP   | Broker→Broker  |
| 0x14      | HEARTBEAT_REQ       | Broker→Broker  |
| 0x15      | HEARTBEAT_RESP      | Broker→Broker  |
| 0x20      | JOIN_GROUP_REQ      | Client→Broker  |
| 0x21      | JOIN_GROUP_RESP     | Broker→Client  |
| 0x22      | GROUP_HEARTBEAT_REQ | Client→Broker  |
| 0x23      | GROUP_HEARTBEAT_RESP| Broker→Client  |
| 0x24      | LEAVE_GROUP_REQ     | Client→Broker  |
| 0x25      | LEAVE_GROUP_RESP    | Broker→Client  |
| 0xFF      | ERROR_RESP          | Broker→Client  |

---

## Interview Q&A

**Q: How does a write survive a broker crash?**
A: The producer's PUBLISH_REQ is sent to the partition leader. The leader calls
`RaftNode.propose()`, which blocks until a majority of the Raft group has appended
the entry to their logs. Only then does the leader apply the entry to the `PartitionLog`
and return a PUBLISH_RESP. Since a majority has the data, even if the leader crashes
immediately after, the next elected leader will have the entry and will commit it.

**Q: How does leader election work? How is split-brain prevented?**
A: Each partition runs its own Raft group. When the leader stops sending heartbeats,
followers' election timers fire (randomized 150–300ms). The candidate sends
`RequestVote` RPCs with its current term and last log entry. A follower grants a vote
only if the candidate's log is at least as up-to-date as its own. The winner is the
first candidate to receive votes from a majority.

Split-brain is prevented by leader epochs (= Raft terms). The new leader's term is
strictly higher than the old leader's term. Any `AppendEntries` or client write from
the old leader will be rejected because its term is stale. Two leaders can never
simultaneously have a majority of the same term.

**Q: What does "effectively-once" mean here?**
A: It means: idempotent producer (retries never create duplicate records in the log)
+ at-least-once consumer (explicit offset commits; consumer retries at worst re-deliver
already-committed records, not new ones). It does NOT mean full cross-partition
transactions — that is explicitly out of scope.

**Q: How does backpressure work?**
A: Each partition has a bounded `ArrayBlockingQueue` for in-flight publishes. A fast
producer's virtual thread blocks when the queue is full (natural backpressure from
blocking IO on a virtual thread — no separate throttle mechanism needed). The producer
client backs off exponentially on `BROKER_BUSY` responses.

**Q: What is the cost of RF=3 vs RF=1?**
A: Measured in `docs/results.md`. The RF=3 overhead is the throughput delta from
requiring two network round trips (leader → two followers) before committing, plus
the latency added by the slowest of the two followers on the critical path.

**Q: How does rebalancing work, and what does a consumer do mid-rebalance?**
A: `GroupCoordinator` tracks each group's membership and triggers a rebalance on
`JOIN_GROUP`, `LEAVE_GROUP`, or a missed `GROUP_HEARTBEAT` past
`BROKER_GROUP_SESSION_TIMEOUT_MS`. When a rebalance starts, `RangeAssignor` recomputes
a contiguous partition range per member from the current membership list and the
coordinator hands each member its new assignment on its next heartbeat response. A
consumer mid-rebalance keeps consuming its *old* assignment until it receives the new
one — there is no "stop the world" barrier — so at most it processes a few extra
records from a partition it's about to give up, which is safe because commits are
idempotent per (group, topic, partition) offset, not because rebalancing is exactly
synchronized.

**Q: How does the idempotent producer tell a duplicate from a sequence gap?**
A: `IdempotencyStore.check(producerId, seqNo)` compares the incoming `seqNo` against
the last *committed* sequence for that producer (tracked per-partition, applied
identically on every replica so a new leader has correct state instantly): exactly
`lastSeq + 1` is a fresh append, `<= lastSeq` is a duplicate (the last one is
answered with its cached offset so a client retrying a lost response gets the same
offset back; older ones can't be re-answered), and anything higher is a gap — the
client skipped a sequence number, which is a client bug, not a retry, and is rejected
with `CODE_SEQUENCE_GAP` rather than silently accepted.

**Q: What does the chaos harness actually inject, and what does each checker prove?**
A: `FaultInjector` drives four real fault types against the Docker Compose cluster:
`killLeader` (SIGKILL the current partition leader), `restart`, `partitionNetwork`
(iptables DROP rules between two brokers), and `slowDisk` (recreates a broker's
container with a real `BROKER_FSYNC_DELAY_MS`, since `tc` shapes network queues, not
disk I/O). Every producer/consumer event is captured by `HistoryRecorder` and checked
afterward: `LossChecker` proves INV-1 (every acked publish was received),
`DuplicationChecker` proves INV-3 (no producer message occupies two offsets),
`LinearizabilityChecker` proves INV-2 (a single valid total order explains what every
consumer saw), and `DivergenceChecker` proves INV-5 (every replica's log agrees on
committed data) by reading each replica's log directly.

**Q: How do segments, the sparse index, and retention fit together?**
A: Each partition's log is a sequence of `{base-offset:020d}.log` segment files, each
paired with a `{base-offset:020d}.index` file mapping offset → byte position at a
configurable stride rather than every record (sparse), so a read seeks to the nearest
indexed entry and linear-scans a bounded distance from there. Every append can be
followed by an `fsync` (`FsyncPolicy`) before the write is acknowledged as committed.
Retention deletes whole old segments once they cross a size or time threshold —
there is no log compaction, so a topic can't be used as a compacted key-value change
log, only as a bounded-retention event stream.

**Q: Why Raft per partition instead of Kafka's ISR + controller model?**
A: One correctness story instead of two. ISR (in-sync replica set) plus a separate
controller for leader election is Kafka's design and works, but it's effectively a
hand-rolled, more ad-hoc consensus protocol layered under a cluster-wide controller
election of its own. Raft is a single, provably-correct algorithm that gives leader
election, log replication, and split-brain prevention (via terms/epochs) for free,
per partition, with no separate controller election needed. That's why this project
is framed as "a Kafka-style broker with Raft-replicated partitions" and never as
"an implementation of Kafka's ISR/controller architecture" — the wire protocol and
partition model are Kafka-like, but the replication core is a different, simpler
algorithm chosen for the correctness guarantee, not a re-implementation of Kafka
internals.
