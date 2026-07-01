# Distributed Message Broker — Architecture Document

**Version:** 1.0  
**Status:** Implementation-Ready Baseline  
**Source specs:** `project.md`, `requirements.md`, `project-summary.md`

---

## Table of Contents

1. [System Overview](#1-system-overview)
2. [Component Architecture](#2-component-architecture)
3. [Data Flow](#3-data-flow)
4. [Service Boundaries](#4-service-boundaries)
5. [Deployment Architecture](#5-deployment-architecture)
6. [Scaling Strategy](#6-scaling-strategy)
7. [Security Architecture](#7-security-architecture)
8. [Technology Stack](#8-technology-stack)

---

## 1. System Overview

### 1.1 Purpose

The Distributed Message Broker is a durable, replicated, ordered message queue. Producers publish messages to named topics; the broker persists each message to an append-only disk log, replicates it to a majority of peer brokers using Raft consensus, and acknowledges the write only after that majority is reached. Consumers poll partitions by offset and receive committed messages in strict append order.

The system's correctness guarantee — zero committed-message loss and zero duplication under arbitrary leader crashes — is the primary deliverable. Performance (≥200K msgs/sec at RF=3, p99 ≤8ms) is a secondary deliverable. Both are machine-verified, not asserted.

### 1.2 Scope Boundary

**In scope:**
- Custom binary TCP protocol for all client↔broker and broker↔broker communication
- Append-only segment-file log with fsync and sparse memory-mapped offset index
- Raft consensus, one group per partition (multi-Raft)
- Idempotent producer (producer-id + per-partition sequence numbers)
- At-least-once consumer with explicit offset commits (effectively-once when combined with idempotent producer)
- Static cluster membership via Docker Compose
- Static consumer-group partition assignment
- Retention by size/time
- Chaos harness, linearizability checker, JMH benchmarks

**Explicitly out of scope:**
- Cross-partition transactions / read-committed isolation
- Log compaction
- Dynamic cluster membership / gossip
- Dynamic consumer-group rebalancing (stretch only)
- NIO selectors, async IO frameworks, gRPC, Netty, Aeron
- Apache Kafka ISR/controller replication model

### 1.3 Correctness Invariants

These five invariants are the system's contract. Every architectural decision below is made in service of them.

| INV | Statement |
|-----|-----------|
| INV-1 | A committed write is never lost, even across leader crashes. |
| INV-2 | Per-partition ordering is preserved for a given producer. |
| INV-3 | No duplicate delivery results from producer retries (idempotent producer deduplicates on append). |
| INV-4 | Two leaders for the same partition can never both commit (leader epoch fences stale leaders). |
| INV-5 | Consumers only read committed (majority-replicated) data. |

---

## 2. Component Architecture

### 2.1 Module Map

The system is organized as a Gradle multi-module project. Each module has a single, non-overlapping responsibility. Dependency arrows point from dependent to dependency; no cycles are permitted.

```
┌─────────────────────────────────────────────────────────────────────┐
│                        broker/                                       │
│  BrokerServer · PartitionManager · ReplicationCoordinator           │
│  ConsumerGroupManager · OffsetStore · MetadataService               │
│  BackpressureController                                              │
│         │                    │                    │                  │
│         ▼                    ▼                    ▼                  │
│      raft/               log/               protocol/               │
│  RaftNode            SegmentLog          MessageCodec               │
│  RaftLog             LogSegment          FrameDecoder               │
│  ElectionTimer       OffsetIndex         FrameEncoder               │
│  ReplicationPipeline SegmentManager      RequestTypes               │
│  PersistentState     RetentionPolicy     ResponseTypes              │
│         │                    │                    │                  │
│         └──────────┬─────────┘                    │                  │
│                    ▼                              ▼                  │
│               (shared)                       client/                │
│           common types,                  ProducerClient             │
│           config, logging                ConsumerClient             │
│                                          MetadataClient             │
└─────────────────────────────────────────────────────────────────────┘

chaos/                          bench/
ChaosOrchestrator               JmhBenchmarks
FaultInjector                   LoadGenerator
LinearizabilityChecker          ResultsWriter
HistoryRecorder
```

### 2.2 Module Responsibilities

#### `protocol/` — Wire Protocol & Codec

Owns the binary framing format and all serialization/deserialization. Nothing outside this module constructs raw bytes for the wire.

**Key classes:**
- `Frame` — immutable value type: `[4-byte length][1-byte type][variable payload]`
- `MessageType` — enum of all request/response types (see §2.3)
- `FrameDecoder` — reads from a `java.io.InputStream`, blocks until a complete frame arrives, returns a typed `Request` or `Response`
- `FrameEncoder` — writes a typed object to a `java.io.OutputStream` as a length-prefixed frame
- `MessageCodec` — per-type serialization logic; uses a compact hand-written binary format (no JSON, no Protobuf — custom binary is the portfolio signal)
- `ProtocolException` — thrown on malformed frames; never swallowed

**Wire format (all fields big-endian):**
```
┌──────────────┬───────────┬──────────────────────────────────┐
│ length (4B)  │ type (1B) │ payload (length - 1 bytes)       │
└──────────────┴───────────┴──────────────────────────────────┘
```
`length` is the byte count of `type + payload`. A frame with `length=1` has an empty payload. Maximum frame size is configurable (default 16 MB).

**Message types:**

| Type byte | Name | Direction | Purpose |
|-----------|------|-----------|---------|
| 0x01 | `PUBLISH_REQ` | Client→Broker | Publish one record |
| 0x02 | `PUBLISH_RESP` | Broker→Client | Ack with committed offset, or error |
| 0x03 | `POLL_REQ` | Client→Broker | Fetch records from offset |
| 0x04 | `POLL_RESP` | Broker→Client | Batch of committed records |
| 0x05 | `COMMIT_OFFSET_REQ` | Client→Broker | Commit consumer group offset |
| 0x06 | `COMMIT_OFFSET_RESP` | Broker→Client | Ack offset commit |
| 0x07 | `METADATA_REQ` | Client→Broker | Query partition→leader map |
| 0x08 | `METADATA_RESP` | Broker→Client | Current partition→leader map |
| 0x10 | `APPEND_ENTRIES_REQ` | Broker→Broker | Raft AppendEntries RPC |
| 0x11 | `APPEND_ENTRIES_RESP` | Broker→Broker | Raft AppendEntries response |
| 0x12 | `REQUEST_VOTE_REQ` | Broker→Broker | Raft RequestVote RPC |
| 0x13 | `REQUEST_VOTE_RESP` | Broker→Broker | Raft RequestVote response |
| 0x14 | `HEARTBEAT_REQ` | Broker→Broker | Raft heartbeat (empty AppendEntries) |
| 0x15 | `HEARTBEAT_RESP` | Broker→Broker | Heartbeat ack |
| 0xFF | `ERROR_RESP` | Broker→Client | Error with code and message |

#### `log/` — Append-Only Log

Owns all disk I/O for a single partition's message log. Completely independent of Raft and broker logic; takes a directory path and exposes a pure append/read interface.

**Key classes:**
- `PartitionLog` — top-level facade; owns a list of `LogSegment`s and the active `OffsetIndex`
- `LogSegment` — a single `.log` file; append-only writes via `FileChannel`; fsync on configurable policy
- `OffsetIndex` — sparse index mapping logical offset → file position; backed by a `MappedByteBuffer` over a `.index` file; one entry per N bytes (configurable, default 4096 bytes)
- `SegmentManager` — rolls segments when the active segment exceeds the size limit; enforces retention by deleting old segments
- `LogRecord` — immutable value type: `[8-byte offset][8-byte timestamp][4-byte key-length][key bytes][4-byte value-length][value bytes][4-byte CRC32]`
- `LogRecovery` — on startup, scans the last segment from the last valid index entry forward, rebuilds the in-memory state, truncates any partial trailing write

**Fsync policy (configurable, default: `EVERY_WRITE`):**

| Policy | Behavior | Durability | Throughput |
|--------|----------|------------|------------|
| `EVERY_WRITE` | `fsync` after every append | Survives OS crash | Lowest |
| `PERIODIC` | `fsync` every N ms | Survives process crash | Medium |
| `OS_MANAGED` | No explicit fsync | No crash guarantee | Highest |

For the correctness headline, `EVERY_WRITE` is the default. The benchmark suite measures all three.

**Segment file naming:** `{base-offset:020d}.log` and `{base-offset:020d}.index` — the filename encodes the first offset in the segment, enabling O(log n) segment lookup by offset.

**Recovery algorithm:**
```
1. List all .log files in the partition directory, sort by base offset.
2. For each segment except the last: verify it is complete (CRC check on last record).
3. For the last segment: load its .index file; seek to the last indexed position;
   scan forward record-by-record, verifying CRC; stop at the first corrupt/partial record.
4. Truncate the .log file at the last valid record boundary.
5. Rebuild the in-memory nextOffset = last valid offset + 1.
```

#### `raft/` — Raft Consensus Module

A standalone, broker-agnostic Raft implementation. The broker plugs in a `StateMachine` interface; the Raft module drives it. This module has no dependency on `log/` or `broker/`; it uses its own durable log for Raft entries (which in the broker's case wraps `log/`).

**Key classes:**
- `RaftNode` — the central state machine; owns the `RaftRole` (FOLLOWER / CANDIDATE / LEADER), current term, voted-for, commit index, last applied
- `RaftLog` — the Raft entry log; entries are `(term, index, command-bytes)`; backed by a durable append-only store (in the broker, this is `PartitionLog`)
- `PersistentState` — durably stores `(currentTerm, votedFor)` before any RPC response; uses a single-record file with fsync
- `ElectionTimer` — randomized timeout (150–300ms default, configurable); fires `startElection()` on the `RaftNode`
- `ReplicationPipeline` — per-follower goroutine (virtual thread) that tracks `nextIndex[follower]` and `matchIndex[follower]`, sends `AppendEntries`, and processes responses
- `CommitAdvancer` — after each `AppendEntries` response, checks if `matchIndex` for a majority of nodes ≥ some index N with `log[N].term == currentTerm`; if so, advances `commitIndex`
- `StateMachine` — interface the broker implements: `apply(long index, byte[] command) → byte[]`
- `RaftRpcClient` — sends `AppendEntries` and `RequestVote` RPCs over the broker's TCP connections; uses the `protocol/` codec

**Raft state transitions:**
```
         timeout / no heartbeat
FOLLOWER ──────────────────────► CANDIDATE ──── wins majority ──► LEADER
    ▲                                │                               │
    │         discovers higher term  │                               │
    └────────────────────────────────┘◄──────────────────────────────┘
                                          discovers higher term
```

**Leader epoch (fencing):**
- Every Raft term is the leader epoch. The term is included in every `AppendEntries` and `RequestVote` RPC.
- A broker receiving an RPC with a term > its own immediately transitions to FOLLOWER and updates its term.
- A broker receiving an RPC with a term < its own rejects it.
- The broker's `PartitionManager` tracks the current leader epoch. Any write attempt from a broker with a stale epoch is rejected with `STALE_LEADER_EPOCH` error.
- This is INV-4: two leaders cannot both commit because the second leader's term is strictly higher, and the first leader's writes will be rejected by followers who have already seen the higher term.

**Commit safety (INV-1):**
- An entry is committed only when `matchIndex[i] >= N` for a majority of nodes, and `log[N].term == currentTerm`.
- The "only entries from the current term" rule (Raft §5.4.2) prevents a new leader from committing stale entries from a previous term without first committing an entry from its own term.
- Committed entries are never overwritten; a new leader only truncates uncommitted suffix entries.

#### `broker/` — Broker Server

The top-level server process. Wires together `protocol/`, `log/`, and `raft/` into a running broker.

**Key classes:**
- `BrokerServer` — main entry point; reads config, starts the TCP listener, initializes all subsystems
- `ConnectionAcceptor` — accepts TCP connections in a loop; spawns one virtual thread per connection via `Executors.newVirtualThreadPerTaskExecutor()`
- `RequestHandler` — per-connection handler; reads frames via `FrameDecoder`, dispatches to the appropriate handler method, writes responses via `FrameEncoder`
- `PartitionManager` — owns the set of `PartitionReplica` objects for this broker; routes publish/poll to the correct replica; enforces leader-only writes
- `PartitionReplica` — one per partition this broker participates in; holds a `RaftNode` and a `PartitionLog`; implements `StateMachine` (Raft applies committed entries to the log)
- `MetadataService` — maintains the `partition → (leader broker-id, all replica broker-ids)` map; updated by Raft leadership changes; answers `METADATA_REQ`
- `ConsumerGroupManager` — stores committed offsets per `(group-id, topic, partition)`; backed by a durable offset log (a special internal partition)
- `OffsetStore` — durable storage for consumer group offsets; a simple append-only file per group, recovered on restart
- `BackpressureController` — per-partition bounded `ArrayBlockingQueue` for in-flight publish requests; blocks the producer's virtual thread when full (natural backpressure via blocking IO)
- `BrokerConfig` — loaded from environment variables / config file; all timeouts, sizes, and policies are externalized

**Publish path (leader broker):**
```
RequestHandler.handlePublish()
  → BackpressureController.acquire()          // blocks if queue full
  → PartitionManager.getLeaderReplica()       // throws if not leader
  → PartitionReplica.append(record)
      → idempotency check (producer-id, seq)  // INV-3
      → RaftNode.propose(serialized record)   // blocks until committed
      → returns committed offset
  → BackpressureController.release()
  → send PUBLISH_RESP with offset
```

**Poll path (leader broker):**
```
RequestHandler.handlePoll()
  → PartitionManager.getLeaderReplica()
  → PartitionReplica.read(offset, maxBytes)
      → PartitionLog.read(offset, maxBytes)   // only up to commitIndex (INV-5)
  → send POLL_RESP with record batch
```

#### `client/` — Client Library

The Java client library used by producers, consumers, and the chaos/bench harnesses.

**Key classes:**
- `BrokerConnection` — a single TCP connection to one broker; wraps `FrameDecoder` and `FrameEncoder`; one virtual thread for reading responses, one for writing requests; uses a `ConcurrentHashMap<correlationId, CompletableFuture>` for request/response matching
- `MetadataClient` — queries any broker for the current partition→leader map; caches results; refreshes on `NOT_LEADER` error
- `ProducerClient` — assigns producer-id on first connect; maintains per-partition sequence numbers; sends `PUBLISH_REQ`; retries on network error (idempotent retry is safe due to INV-3); routes to the correct leader via `MetadataClient`
- `ConsumerClient` — sends `POLL_REQ` to the partition leader; tracks current offset; sends `COMMIT_OFFSET_REQ` explicitly; routes via `MetadataClient`
- `CorrelationIdGenerator` — thread-safe monotonic counter for matching async requests to responses

**Correlation ID protocol:**
Every request frame includes a 4-byte correlation ID chosen by the client. The broker echoes it in the response. The client uses this to match responses to outstanding requests, enabling pipelining of multiple in-flight requests over a single connection.

#### `chaos/` — Fault-Injection Harness

**Key classes:**
- `ChaosOrchestrator` — top-level test driver; configures fault schedule, runs producers/consumers, injects faults, collects history, runs the checker
- `FaultInjector` — injects faults via Docker API (kill/pause/unpause containers) and `iptables` rules (network partition between specific broker pairs)
- `HistoryRecorder` — thread-safe log of all `(type, key, value, timestamp, outcome)` events from producers and consumers; written to disk for post-hoc analysis
- `LinearizabilityChecker` — implements a Knossos-style WGL (Wing-Gong-Lamport) linearizability check over the recorded history; models the system as a register per partition-key
- `DivergenceChecker` — after a test run, connects to all brokers and compares their committed log contents for each partition; reports any offset where replicas disagree

#### `bench/` — Benchmarks

**Key classes:**
- `PublishThroughputBenchmark` — JMH benchmark; measures sustained msgs/sec at 1KB payload, RF=1 and RF=3
- `PublishLatencyBenchmark` — JMH benchmark; measures publish-to-commit p50/p99 at RF=3
- `LoadGenerator` — standalone load generator (not JMH) for sustained multi-minute runs; used by the chaos harness
- `ResultsWriter` — parses JMH JSON output and appends formatted tables to `docs/results.md`

### 2.3 Internal Interfaces

These are the Java interfaces that define the boundaries between modules. They are the seams at which mocking, testing, and future extension happen.

```java
// log/ module public interface
public interface PartitionLog {
    AppendResult append(LogRecord record);          // returns assigned offset
    List<LogRecord> read(long fromOffset, int maxBytes);
    long nextOffset();
    long firstOffset();
    void flush();                                   // explicit fsync
    void recover();                                 // called on startup
    void close();
}

// raft/ module public interface
public interface StateMachine {
    // Called by RaftNode on the leader after majority replication.
    // Must be deterministic and idempotent.
    ApplyResult apply(long raftIndex, byte[] command);
}

public interface RaftTransport {
    // Called by RaftNode to send RPCs to peers.
    CompletableFuture<AppendEntriesResponse> appendEntries(
        int targetBrokerId, AppendEntriesRequest req);
    CompletableFuture<RequestVoteResponse> requestVote(
        int targetBrokerId, RequestVoteRequest req);
}

// broker/ module — what a PartitionReplica exposes to RequestHandler
public interface PartitionReplica {
    AppendResult append(ProducerRecord record);     // leader only; blocks until committed
    List<LogRecord> read(long fromOffset, int maxBytes); // returns only committed records
    boolean isLeader();
    long leaderEpoch();
    PartitionMetadata metadata();
}
```

---

## 3. Data Flow

### 3.1 Publish Flow (Happy Path, RF=3)

```
Producer Client                 Broker-1 (Leader)              Broker-2 (Follower)    Broker-3 (Follower)
     │                                │                                │                      │
     │── PUBLISH_REQ ────────────────►│                                │                      │
     │   (producerId=P, seq=42,       │                                │                      │
     │    topic=T, partition=0,       │                                │                      │
     │    key=K, payload=bytes)       │                                │                      │
     │                                │                                │                      │
     │                         [idempotency check]                     │                      │
     │                         (P, 0, 42) not seen → proceed          │                      │
     │                                │                                │                      │
     │                         [RaftNode.propose(record)]              │                      │
     │                         append to local RaftLog                 │                      │
     │                         (index=N, term=T)                       │                      │
     │                                │                                │                      │
     │                                │── APPEND_ENTRIES_REQ ─────────►│                      │
     │                                │   (term=T, prevLogIndex=N-1,   │                      │
     │                                │    entries=[{N,T,record}],     │                      │
     │                                │    leaderCommit=N-1)           │                      │
     │                                │                                │                      │
     │                                │── APPEND_ENTRIES_REQ ──────────────────────────────►  │
     │                                │   (same)                       │                      │
     │                                │                                │                      │
     │                                │◄─ APPEND_ENTRIES_RESP ─────────│                      │
     │                                │   (success=true,               │                      │
     │                                │    matchIndex=N)               │                      │
     │                                │                                │                      │
     │                         [CommitAdvancer: matchIndex[B2]=N]      │                      │
     │                         majority reached (B1+B2 = 2 of 3)      │                      │
     │                         commitIndex advances to N               │                      │
     │                         StateMachine.apply(N, record)           │                      │
     │                         → PartitionLog.append(record)           │                      │
     │                         → assigns offset=O                      │                      │
     │                                │                                │                      │
     │◄─ PUBLISH_RESP ────────────────│                                │                      │
     │   (offset=O)                   │                                │                      │
     │                                │◄─ APPEND_ENTRIES_RESP ─────────────────────────────── │
     │                                │   (success=true,               │                      │
     │                                │    matchIndex=N)               │                      │
     │                                │                                │                      │
     │                         [next heartbeat carries leaderCommit=N] │                      │
     │                         followers advance their commitIndex      │                      │
```

**Key timing note:** The producer receives its ack after B1 + B2 have the entry (majority). B3's response arrives later and is not on the critical path. The producer's virtual thread blocks inside `RaftNode.propose()` until the majority is reached; this is the source of the publish-to-commit latency.

### 3.2 Poll Flow

```
Consumer Client                 Broker-1 (Leader)
     │                                │
     │── POLL_REQ ───────────────────►│
     │   (group=G, topic=T,           │
     │    partition=0, offset=O,      │
     │    maxBytes=65536)             │
     │                                │
     │                         [PartitionReplica.read(O, 65536)]
     │                         reads from PartitionLog
     │                         up to min(commitIndex, O+maxBytes)
     │                         (INV-5: never past commitIndex)
     │                                │
     │◄─ POLL_RESP ───────────────────│
     │   (records=[r1,r2,...rN],      │
     │    nextOffset=O+N)             │
     │                                │
     │ [process records]              │
     │                                │
     │── COMMIT_OFFSET_REQ ──────────►│
     │   (group=G, topic=T,           │
     │    partition=0, offset=O+N)    │
     │                                │
     │                         [ConsumerGroupManager.commit(G,T,0,O+N)]
     │                         → OffsetStore.append(...)
     │                         → fsync
     │                                │
     │◄─ COMMIT_OFFSET_RESP ──────────│
```

### 3.3 Leader Failover Flow

```
Time ──────────────────────────────────────────────────────────────────────►

Broker-1 (Leader, term=5)    Broker-2 (Follower)    Broker-3 (Follower)
     │                              │                       │
     │  [CRASH / KILL]              │                       │
     ✗                              │                       │
                                    │                       │
                             [ElectionTimer fires           │
                              on B2 after timeout]          │
                                    │                       │
                                    │── REQUEST_VOTE_REQ ──►│
                                    │   (term=6,            │
                                    │    candidateId=B2,    │
                                    │    lastLogIndex=N,    │
                                    │    lastLogTerm=5)     │
                                    │                       │
                                    │◄─ REQUEST_VOTE_RESP ──│
                                    │   (term=6,            │
                                    │    voteGranted=true)  │
                                    │                       │
                             [B2 wins majority (B2+B3)]     │
                             B2 becomes LEADER, term=6      │
                             leaderEpoch=6                  │
                                    │                       │
                             [B2 sends heartbeat            │
                              with term=6 to B3]            │
                                    │── HEARTBEAT_REQ ─────►│
                                    │   (term=6)            │
                                    │                       │
                             [B2 sends no-op entry          │
                              to commit any pending         │
                              entries from term=5]          │
                                    │                       │
Producer Client                     │                       │
     │── PUBLISH_REQ ──────────────►│ (redirected by        │
     │   (to B1, gets NOT_LEADER)   │  MetadataClient       │
     │   MetadataClient refreshes   │  after refresh)       │
     │   → discovers B2 is leader   │                       │
     │── PUBLISH_REQ ──────────────►│                       │
     │                              │                       │

[If B1 restarts later:]
B1 receives APPEND_ENTRIES from B2 with term=6 > B1's term=5
B1 transitions to FOLLOWER, term=6
B1 truncates any uncommitted suffix entries
B1 rejoins as follower for term=6
```

**Fencing guarantee (INV-4):** If B1 somehow did not crash but was merely partitioned, when the partition heals, B1 receives a message with term=6. B1 immediately steps down. Any write B1 attempted during the partition was either (a) not committed (no majority), so it is safely discarded, or (b) committed, in which case B2 also has it (majority requirement). There is no scenario where B1 commits something B2 does not have.

### 3.4 Idempotent Producer Deduplication Flow

```
Producer Client                 Broker-1 (Leader)
     │                                │
     │── PUBLISH_REQ ───────────────►│
     │   (producerId=P, seq=42, ...)  │
     │                                │
     │                         [RaftNode.propose() starts]
     │                         [network hiccup — no response]
     │                                │
     │   [timeout, retry]             │
     │── PUBLISH_REQ ───────────────►│  ← same (P, seq=42)
     │   (producerId=P, seq=42, ...)  │
     │                                │
     │                         [idempotency check]
     │                         (P, partition=0, seq=42) already committed
     │                         → return original offset, no second append
     │                                │
     │◄─ PUBLISH_RESP ────────────────│
     │   (offset=O)  ← same offset    │
```

**Idempotency store:** The broker maintains an in-memory `ConcurrentHashMap<(producerId, partitionId), lastCommittedSeq>` per partition, rebuilt from the log on recovery. If `incomingSeq == lastCommittedSeq + 1`, proceed. If `incomingSeq <= lastCommittedSeq`, it is a duplicate — return the cached ack. If `incomingSeq > lastCommittedSeq + 1`, it is an out-of-order sequence gap — return `SEQUENCE_GAP` error (producer must not skip sequence numbers).

### 3.5 Metadata Discovery and Redirect Flow

```
Client                    Any Broker (B1)           Broker-2 (Leader for P0)
  │                             │                           │
  │── METADATA_REQ ────────────►│                           │
  │   (topic=T)                 │                           │
  │                       [MetadataService.query(T)]        │
  │◄─ METADATA_RESP ────────────│                           │
  │   (partition=0 → B2:9092,   │                           │
  │    partition=1 → B3:9092)   │                           │
  │                             │                           │
  │── PUBLISH_REQ ──────────────────────────────────────────►│
  │   (topic=T, partition=0)    │                           │
  │◄─ PUBLISH_RESP ─────────────────────────────────────────│
  │   (offset=O)                │                           │
  │                             │                           │
  │  [B2 crashes; B3 becomes leader for P0]                 │
  │                             │                           │
  │── PUBLISH_REQ ──────────────────────────────────────────►│ (B2, now dead)
  │   (no response / connection refused)                    │
  │                             │                           │
  │── METADATA_REQ ────────────►│                           │
  │