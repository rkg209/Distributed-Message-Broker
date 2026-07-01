# Distributed Message Broker — System Design

**Version:** 1.0  
**Source:** `planning/02-architecture.md`  
**Status:** Implementation Reference

---

## Table of Contents

1. [Modules](#1-modules)
2. [Services](#2-services)
3. [Internal Workflows](#3-internal-workflows)
4. [Event Flows](#4-event-flows)
5. [State Transitions](#5-state-transitions)
6. [Design Patterns](#6-design-patterns)
7. [Integration Points](#7-integration-points)

---

## 1. Modules

The system is a Gradle multi-module Java project. Each module owns a single vertical slice of responsibility. No cycles exist in the dependency graph. The sections below describe every module's purpose, public surface, internal structure, and the invariants it owns.

### 1.1 Dependency Graph

```
bench/      chaos/
  │            │
  └────┬────────┘
       │
    client/
       │
    broker/ ──────────────────────────────────────────────────────┐
       │                    │                    │                 │
     raft/               log/               protocol/          (shared)
       │                    │                    │             common types
       └──────────┬──────────┘                   │             config
                  │                              │             logging
               (shared) ◄─────────────────────────
```

**Rule:** arrows point from dependent to dependency. `protocol/` and `(shared)` are leaves. `log/` and `raft/` depend only on `(shared)`. `broker/` depends on all three. `client/` depends on `protocol/` and `(shared)`. `chaos/` and `bench/` depend on `client/` and `broker/` (for configuration constants and test utilities only — never for runtime coupling).

---

### 1.2 `protocol/` — Wire Protocol and Codec

**Owns:** binary framing, all serialization and deserialization, the canonical list of message types. Nothing outside this module constructs raw bytes for the wire.

#### Package layout

```
protocol/
  src/main/java/broker/protocol/
    Frame.java
    MessageType.java
    FrameDecoder.java
    FrameEncoder.java
    MessageCodec.java
    ProtocolException.java
    requests/
      PublishRequest.java
      PollRequest.java
      CommitOffsetRequest.java
      MetadataRequest.java
      AppendEntriesRequest.java
      RequestVoteRequest.java
      HeartbeatRequest.java
    responses/
      PublishResponse.java
      PollResponse.java
      CommitOffsetResponse.java
      MetadataResponse.java
      AppendEntriesResponse.java
      RequestVoteResponse.java
      HeartbeatResponse.java
      ErrorResponse.java
```

#### Key classes in detail

**`Frame`** — immutable value type. Fields: `int length`, `MessageType type`, `byte[] payload`. Constructed only by `FrameDecoder`; consumed only by `MessageCodec`. Never escapes the codec layer as raw bytes.

**`MessageType`** — enum with one constant per wire type byte (0x01–0x15, 0xFF). Each constant carries its byte value and a direction annotation (`CLIENT_TO_BROKER`, `BROKER_TO_CLIENT`, `BROKER_TO_BROKER`) used by `FrameDecoder` to reject frames arriving on the wrong connection class.

**`FrameDecoder`** — stateful, not thread-safe. One instance per TCP connection. Reads from a `java.io.InputStream`. Algorithm:
1. Read 4 bytes → `length`.
2. Validate `length` ≤ `maxFrameBytes` (default 16 MB); throw `ProtocolException` otherwise.
3. Read `length` bytes into a heap `byte[]`.
4. Extract `type` byte; look up `MessageType`; throw `ProtocolException` on unknown type.
5. Delegate to `MessageCodec.decode(type, payload)` → typed request or response object.

**`FrameEncoder`** — stateless, thread-safe. Writes to a `java.io.OutputStream`. Accepts any typed request or response, delegates to `MessageCodec.encode()`, prepends the 4-byte length, writes atomically (single `write` call on a pre-allocated `byte[]`).

**`MessageCodec`** — one static `encode` and one static `decode` method per `MessageType`, dispatched via a `switch` expression. Uses hand-written big-endian binary format: fixed-width integer fields first, then length-prefixed variable fields. No reflection, no JSON, no Protobuf. This is the portfolio signal for low-level systems competence.

**`ProtocolException`** — unchecked. Thrown on: unknown type byte, frame exceeding max size, truncated payload, CRC mismatch on record payloads. The connection handler catches this and closes the connection; it is never swallowed.

#### Wire format (all fields big-endian)

```
┌──────────────┬───────────┬──────────────────────────────────┐
│ length (4B)  │ type (1B) │ payload (length - 1 bytes)       │
└──────────────┴───────────┴──────────────────────────────────┘
```

`length` counts `type + payload`. A frame with `length=1` has an empty payload.

#### `PublishRequest` payload layout (example of codec discipline)

```
┌──────────────────┬──────────────────┬──────────────┬──────────────────┐
│ correlationId 4B │ producerId    8B  │ partitionId  │ sequenceNumber   │
│                  │                  │ 4B           │ 8B               │
├──────────────────┴──────────────────┴──────────────┴──────────────────┤
│ topicLength 2B │ topic bytes        │ keyLength 4B │ key bytes        │
├────────────────┴────────────────────┴──────────────┴──────────────────┤
│ valueLength 4B │ value bytes                                          │
└────────────────┴──────────────────────────────────────────────────────┘
```

Every request carries a `correlationId` (4 bytes, chosen by the client) that the broker echoes verbatim in the response. This enables pipelining over a single TCP connection.

---

### 1.3 `log/` — Append-Only Partition Log

**Owns:** all disk I/O for a single partition's message data. Completely independent of Raft and broker logic. Takes a directory path; exposes a pure append/read interface.

#### Package layout

```
log/
  src/main/java/broker/log/
    PartitionLog.java          (interface)
    PartitionLogImpl.java
    LogSegment.java
    OffsetIndex.java
    SegmentManager.java
    LogRecord.java
    LogRecovery.java
    FsyncPolicy.java
    RetentionPolicy.java
    AppendResult.java
```

#### Key classes in detail

**`LogRecord`** — immutable value type. On-disk layout:

```
┌──────────┬───────────┬────────────┬───────────┬─────────────┬──────────┬──────────┐
│ offset   │ timestamp │ keyLength  │ key bytes │ valueLength │ value    │ CRC32    │
│ 8B       │ 8B        │ 4B         │ variable  │ 4B          │ variable │ 4B       │
└──────────┴───────────┴────────────┴───────────┴─────────────┴──────────┴──────────┘
```

CRC32 covers all preceding bytes in the record. Verified on every read and during recovery.

**`LogSegment`** — wraps a single `.log` file. Maintains a `FileChannel` opened in append mode. `append(LogRecord)` serializes the record, writes via `FileChannel.write(ByteBuffer)`, and calls `FileChannel.force(false)` according to the active `FsyncPolicy`. Tracks `baseOffset` (encoded in the filename) and `currentSize`. Read path: `read(long offset, int maxBytes)` seeks to the file position via the `OffsetIndex`, then scans forward deserializing records until `maxBytes` is consumed.

**`OffsetIndex`** — sparse index: one entry per `indexIntervalBytes` (default 4096) of log data written. Each entry is `(logicalOffset: 8B, filePosition: 4B)` — 12 bytes per entry. Backed by a `MappedByteBuffer` over a `.index` file. On append, if `bytesSinceLastIndex >= indexIntervalBytes`, writes a new entry. On lookup, binary-searches the mapped buffer for the largest entry ≤ target offset, returns the file position.

**`SegmentManager`** — owns the ordered list of `LogSegment` objects for one partition. On `append`, checks if the active segment exceeds `maxSegmentBytes` (default 1 GB); if so, closes the active segment, creates a new one with `baseOffset = nextOffset`, and appends to the new segment. On `read`, binary-searches segment list by `baseOffset` to find the correct segment in O(log n). Enforces retention: periodically scans segments older than `retentionMs` or beyond `retentionBytes` and deletes the `.log` and `.index` files for the oldest segments.

**`LogRecovery`** — called once at startup by `PartitionLogImpl.recover()`. Algorithm:

```
1. List all .log files in the partition directory; sort ascending by base offset.
2. For each segment except the last:
     a. Open the segment.
     b. Verify the CRC32 of the last record.
     c. If corrupt: this is a catastrophic mid-segment corruption; log and halt.
3. For the last (active) segment:
     a. Load its .index file into OffsetIndex.
     b. Seek to the file position of the last index entry.
     c. Scan forward record-by-record, verifying CRC32 on each.
     d. Stop at the first record whose CRC32 fails or whose bytes are incomplete.
     e. Truncate the .log file at the last valid record boundary using
        FileChannel.truncate(position).
4. Set nextOffset = lastValidOffset + 1.
5. Rebuild the in-memory OffsetIndex for the active segment from the truncation point.
```

**`FsyncPolicy`** — enum: `EVERY_WRITE`, `PERIODIC`, `OS_MANAGED`. Injected into `LogSegment` at construction. `PERIODIC` uses a dedicated virtual thread that calls `FileChannel.force(false)` every `fsyncIntervalMs` milliseconds.

**`PartitionLog` interface** — the public contract:

```java
public interface PartitionLog {
    AppendResult append(LogRecord record);
    List<LogRecord> read(long fromOffset, int maxBytes);
    long nextOffset();
    long firstOffset();
    void flush();        // explicit fsync regardless of policy
    void recover();      // called once at startup
    void close();
}
```

`AppendResult` carries the assigned `offset` (long) and the `filePosition` (long) at which the record was written.

---

### 1.4 `raft/` — Raft Consensus Module

**Owns:** leader election, log replication, commit advancement, and durable Raft state. Broker-agnostic: it drives a `StateMachine` interface and uses a `RaftTransport` interface. Has no dependency on `log/` or `broker/` at the source level; the broker wires them together at runtime.

#### Package layout

```
raft/
  src/main/java/broker/raft/
    RaftNode.java
    RaftRole.java              (enum: FOLLOWER, CANDIDATE, LEADER)
    RaftLog.java               (interface)
    RaftLogImpl.java
    RaftEntry.java
    PersistentState.java
    ElectionTimer.java
    ReplicationPipeline.java
    CommitAdvancer.java
    StateMachine.java          (interface)
    RaftTransport.java         (interface)
    RaftConfig.java
    RaftException.java
```

#### Key classes in detail

**`RaftNode`** — the central state machine. Fields:

```java
volatile RaftRole role;           // FOLLOWER | CANDIDATE | LEADER
volatile long currentTerm;        // persisted before any RPC response
volatile int  votedFor;           // persisted; -1 = none
volatile long commitIndex;        // highest committed entry index
volatile long lastApplied;        // highest applied entry index
int           localId;            // this broker's ID
int[]         peerIds;            // all other broker IDs in the Raft group
```

All term and votedFor mutations go through `PersistentState.persist(term, votedFor)` before taking effect. `RaftNode` is the only class that mutates role; all other classes call methods on `RaftNode` and receive callbacks.

**`RaftLog`** — interface over the durable entry log:

```java
public interface RaftLog {
    void append(RaftEntry entry);
    RaftEntry get(long index);
    long lastIndex();
    long lastTerm();
    void truncateFrom(long index);   // removes entries [index, lastIndex]
    void flush();
}
```

`RaftEntry` is `(long term, long index, byte[] command)`. In the broker, `RaftLogImpl` wraps `PartitionLog` — each `LogRecord` stores one Raft entry, with the Raft term and index encoded in the key field.

**`PersistentState`** — owns a single-record file `raft-state.bin` in the partition directory. Layout: `[currentTerm: 8B][votedFor: 4B][CRC32: 4B]`. Written with `FileChannel.write` + `FileChannel.force(true)` (metadata flush) before returning. Read on startup; if CRC fails, the node refuses to start (operator must intervene).

**`ElectionTimer`** — runs on a dedicated virtual thread. Maintains a `ScheduledFuture` that fires after a randomized timeout in `[electionTimeoutMinMs, electionTimeoutMaxMs]` (default 150–300 ms). Reset by `RaftNode` on every valid `AppendEntries` or `Heartbeat` received. On fire: calls `RaftNode.startElection()`.

**`ReplicationPipeline`** — one instance per follower, created when this node becomes leader. Runs on a dedicated virtual thread. Tracks:

```java
long nextIndex;    // next entry index to send to this follower
long matchIndex;   // highest entry index known to be replicated on this follower
```

Loop:
1. If `nextIndex <= leader.lastIndex()`: build `AppendEntriesRequest` with entries `[nextIndex, min(nextIndex + maxBatchEntries, lastIndex)]`.
2. Send via `RaftTransport.appendEntries(followerId, req)`.
3. On success response: update `matchIndex = response.matchIndex`; update `nextIndex = matchIndex + 1`; notify `CommitAdvancer`.
4. On failure response with `response.term > currentTerm`: call `RaftNode.stepDown(response.term)`.
5. On failure response with `response.success = false` (log inconsistency): decrement `nextIndex` by 1 (or use the `conflictIndex` hint from the response for faster backtracking); retry.
6. If no entries to send and `heartbeatIntervalMs` has elapsed: send a heartbeat (empty `AppendEntries`).

**`CommitAdvancer`** — called by each `ReplicationPipeline` after updating `matchIndex`. Algorithm:

```
For N from leader.lastIndex() down to commitIndex + 1:
    if log[N].term == currentTerm
    and count(matchIndex[i] >= N) + 1 >= majority:   // +1 for leader itself
        commitIndex = N
        apply entries (lastApplied+1 .. commitIndex) to StateMachine
        break
```

The `log[N].term == currentTerm` check is the Raft §5.4.2 safety rule: a leader never commits entries from a previous term by counting replicas; it only commits them indirectly by committing a current-term entry that follows them.

**`StateMachine` interface:**

```java
public interface StateMachine {
    ApplyResult apply(long raftIndex, byte[] command);
}
```

`ApplyResult` carries the result bytes that `RaftNode.propose()` returns to the caller (the broker's `RequestHandler`). Must be deterministic and idempotent.

**`RaftTransport` interface:**

```java
public interface RaftTransport {
    CompletableFuture<AppendEntriesResponse> appendEntries(
        int targetBrokerId, AppendEntriesRequest req);
    CompletableFuture<RequestVoteResponse> requestVote(
        int targetBrokerId, RequestVoteRequest req);
}
```

Implemented in `broker/` by `RaftRpcClient`, which uses the existing `BrokerConnection` pool and `protocol/` codec. The `raft/` module never touches sockets directly.

---

### 1.5 `broker/` — Broker Server

**Owns:** the running server process. Wires `protocol/`, `log/`, and `raft/` together. Owns the TCP listener, connection lifecycle, request dispatch, partition management, metadata, consumer group offset tracking, and backpressure.

#### Package layout

```
broker/
  src/main/java/broker/server/
    BrokerServer.java
    BrokerConfig.java
    ConnectionAcceptor.java
    RequestHandler.java
    PartitionManager.java
    PartitionReplica.java          (interface)
    PartitionReplicaImpl.java
    MetadataService.java
    ConsumerGroupManager.java
    OffsetStore.java
    BackpressureController.java
    IdempotencyStore.java
    RaftRpcClient.java             (implements RaftTransport)
    BrokerPeerRegistry.java
```

#### Key classes in detail

**`BrokerServer`** — main entry point. `main()` reads `BrokerConfig` from environment variables and a config file, then:
1. Creates the `PartitionManager` (which creates `PartitionReplicaImpl` instances, each with a `PartitionLogImpl` and a `RaftNode`).
2. Creates the `MetadataService`.
3. Creates the `ConsumerGroupManager`.
4. Creates the `BackpressureController`.
5. Starts the `ConnectionAcceptor` on the configured TCP port.
6. Registers a JVM shutdown hook that calls `shutdown()` on all subsystems in reverse order.

**`ConnectionAcceptor`** — runs on a single virtual thread. Calls `ServerSocket.accept()` in a loop. For each accepted `Socket`, submits a `RequestHandler` task to `Executors.newVirtualThreadPerTaskExecutor()`. The executor is unbounded in thread count (virtual threads are cheap) but `BackpressureController` limits in-flight work.

**`RequestHandler`** — per-connection, runs on one virtual thread for its lifetime. Owns one `FrameDecoder` and one `FrameEncoder` for the connection. Main loop:

```
while (connection is open):
    frame = frameDecoder.readFrame()       // blocks on IO
    switch (frame.type):
        PUBLISH_REQ      → handlePublish(frame)
        POLL_REQ         → handlePoll(frame)
        COMMIT_OFFSET_REQ → handleCommitOffset(frame)
        METADATA_REQ     → handleMetadata(frame)
        APPEND_ENTRIES_REQ → handleAppendEntries(frame)   // broker-to-broker
        REQUEST_VOTE_REQ → handleRequestVote(frame)       // broker-to-broker
        HEARTBEAT_REQ    → handleHeartbeat(frame)         // broker-to-broker
        default          → send ERROR_RESP, close connection
    on ProtocolException → send ERROR_RESP, close connection
    on IOException       → close connection silently
```

**`PartitionManager`** — owns a `Map<Integer, PartitionReplicaImpl>` keyed by partition ID. On startup, reads the partition assignment from `BrokerConfig` and initializes one `PartitionReplicaImpl` per assigned partition. `getLeaderReplica(partitionId)` returns the replica only if `replica.isLeader()` is true; otherwise throws `NotLeaderException`, which `RequestHandler` converts to an `ERROR_RESP` with code `NOT_LEADER` and the current known leader's address.

**`PartitionReplicaImpl`** — implements both `PartitionReplica` and `StateMachine`. Holds:
- `RaftNode raftNode` — the Raft consensus engine for this partition.
- `PartitionLogImpl partitionLog` — the durable message log.
- `IdempotencyStore idempotencyStore` — per-partition deduplication state.

`append(ProducerRecord record)`:
1. Check `idempotencyStore.check(record.producerId, record.sequenceNumber)` — see §3.4.
2. Serialize the record to `byte[]`.
3. Call `raftNode.propose(bytes)` — blocks the calling virtual thread until the entry is committed by a majority.
4. `RaftNode` calls back `StateMachine.apply(raftIndex, bytes)` on the leader after commit.
5. `apply()` calls `partitionLog.append(logRecord)` and returns the assigned offset.
6. `propose()` returns the `ApplyResult` containing the offset.
7. Return `AppendResult(offset)` to `RequestHandler`.

`read(long fromOffset, int maxBytes)`:
1. Clamp `fromOffset` to `[partitionLog.firstOffset(), raftNode.commitIndex()]` — enforces INV-5.
2. Delegate to `partitionLog.read(fromOffset, maxBytes)`.

**`MetadataService`** — maintains a `ConcurrentHashMap<String, List<PartitionInfo>>` mapping topic name to partition metadata. `PartitionInfo` contains `(partitionId, leaderId, replicaIds, leaderEpoch)`. Updated by `PartitionReplicaImpl` when `RaftNode` fires a leadership-change callback. Answers `METADATA_REQ` by serializing the current map into a `MetadataResponse`.

**`ConsumerGroupManager`** — owns a `ConcurrentHashMap<GroupPartitionKey, Long>` for in-memory offset tracking, backed by `OffsetStore` for durability. `GroupPartitionKey` is `(groupId, topic, partitionId)`. `commit(key, offset)` appends to `OffsetStore` then updates the in-memory map. `getOffset(key)` returns the in-memory value (or 0 if never committed). Recovered from `OffsetStore` on startup by replaying the append log.

**`OffsetStore`** — a simple append-only file per consumer group. Each record: `[partitionId: 4B][offset: 8B][timestamp: 8B][CRC32: 4B]`. Fsynced after every commit. Recovered by scanning all records and keeping the last offset per partition.

**`BackpressureController`** — one `ArrayBlockingQueue<Permit>` per partition, capacity = `maxInFlightPerPartition` (default 256). `acquire(partitionId)` calls `queue.put(PERMIT)` — blocks the producer's virtual thread if the queue is full. `release(partitionId)` calls `queue.take()`. This is natural backpressure: the virtual thread blocks cheaply, and the OS does not spin.

**`IdempotencyStore`** — per-partition in-memory `ConcurrentHashMap<Long, Long>` mapping `producerId → lastCommittedSeq`. Rebuilt from the partition log on recovery by scanning all `LogRecord`s and extracting `(producerId, sequenceNumber)` from the record key field. Rules:
- `incomingSeq == lastCommittedSeq + 1` → proceed (new write).
- `incomingSeq <= lastCommittedSeq` → duplicate; return cached ack offset.
- `incomingSeq > lastCommittedSeq + 1` → sequence gap; return `SEQUENCE_GAP` error.

**`RaftRpcClient`** — implements `RaftTransport`. Maintains a `Map<Integer, BrokerConnection>` of persistent TCP connections to peer brokers (one connection per peer, reconnected on failure). `appendEntries(targetId, req)` encodes the request via `FrameEncoder`, sends it over the peer's `BrokerConnection`, and returns a `CompletableFuture<AppendEntriesResponse>` resolved when the correlation-ID-matched response arrives.

---

### 1.6 `client/` — Client Library

**Owns:** the Java client API used by producers, consumers, and the test harnesses.

#### Package layout

```
client/
  src/main/java/broker/client/
    BrokerConnection.java
    MetadataClient.java
    ProducerClient.java
    ConsumerClient.java
    CorrelationIdGenerator.java
    ClientConfig.java
    ClientException.java
    NotLeaderException.java
    RetryPolicy.java
```

#### Key classes in detail

**`BrokerConnection`** — one TCP connection to one broker. Owns:
- A `FrameDecoder` on the socket's `InputStream`.
- A `FrameEncoder` on the socket's `OutputStream`.
- A `ConcurrentHashMap<Integer, CompletableFuture<Object>> pendingRequests` keyed by correlation ID.
- A reader virtual thread: loops calling `frameDecoder.readFrame()`, looks up the correlation ID in `pendingRequests`, completes the future.
- A writer virtual thread: drains a `LinkedBlockingQueue<PendingWrite>` and calls `frameEncoder.write()`.

This design allows multiple in-flight requests over a single connection (pipelining) without per-request thread overhead.

**`MetadataClient`** — wraps a `BrokerConnection` to any broker. Caches the `Map<TopicPartition, BrokerAddress>` leader map. `getLeader(topic, partition)` returns from cache if present; otherwise sends `METADATA_REQ` and updates the cache. On receiving `NOT_LEADER` error from any operation, invalidates the cache entry and refreshes before retrying.

**`ProducerClient`** — stateful per-topic-partition. On first use, obtains a `producerId` (a UUID-derived long, assigned locally — no server round-trip needed for the MVP). Maintains `Map<TopicPartition, Long> nextSequence`. `publish(topic, partition, key, value)`:
1. Get `seq = nextSequence.getOrDefault(tp, 0L)`.
2. Build `PublishRequest(producerId, seq, topic, partition, key, value, correlationId)`.
3. Get leader connection via `MetadataClient`.
4. Send request; await `CompletableFuture<PublishResponse>`.
5. On success: increment `nextSequence[tp]`; return offset.
6. On `NOT_LEADER`: refresh metadata; retry (idempotent because same `seq`).
7. On network error: retry with same `seq` (idempotent).
8. On `SEQUENCE_GAP`: programming error; throw `ClientException` (do not retry).

**`ConsumerClient`** — stateful per consumer group and partition. Tracks `Map<TopicPartition, Long> currentOffset`. `poll(topic, partition, maxBytes)`:
1. Get leader via `MetadataClient`.
2. Send `PollRequest(groupId, topic, partition, currentOffset, maxBytes)`.
3. Await `PollResponse`; update `currentOffset = response.nextOffset`.
4. Return list of `LogRecord`.

`commitOffset(topic, partition)`:
1. Send `CommitOffsetRequest(groupId, topic, partition, currentOffset)`.
2. Await `CommitOffsetResponse`.

---

### 1.7 `chaos/` — Fault-Injection Harness

```
chaos/
  src/main/java/broker/chaos/
    ChaosOrchestrator.java
    FaultInjector.java
    HistoryRecorder.java
    LinearizabilityChecker.java
    DivergenceChecker.java
    FaultSchedule.java
    HistoryEvent.java
```

**`ChaosOrchestrator`** — top-level test driver. Configures a `FaultSchedule` (list of `(time, fault-type, target)` tuples), starts background `ProducerClient` and `ConsumerClient` threads, runs the schedule, waits for quiescence, then runs `LinearizabilityChecker` and `DivergenceChecker`. Fails the test if either checker reports a violation.

**`FaultInjector`** — executes faults via the Docker Java SDK (`docker kill`, `docker pause`, `docker unpause`) and via `ProcessBuilder` to run `iptables` commands inside containers (network partition between specific broker pairs). Faults are reversible; the schedule includes both injection and healing events.

**`HistoryRecorder`** — thread-safe. Records `HistoryEvent(type=INVOKE|RETURN, operation=PUBLISH|POLL, key, value, offset, timestamp, outcome=OK|FAIL)`. Written to a JSON-lines file for post-hoc analysis. The linearizability checker reads this file.

**`LinearizabilityChecker`** — implements the WGL (Wing-Gong-Lamport) algorithm. Models each partition as a register (or an append-only log, depending on the model). Checks that the recorded history of invocations and returns is consistent with some sequential execution of the operations. A violation means the system delivered a result that no correct sequential execution could produce — i.e., a correctness bug.

**`DivergenceChecker`** — after a test run, connects to all brokers and issues `POLL_REQ` from offset 0 on each partition. Compares the returned record sequences byte-by-byte across all replicas. Any disagreement at the same offset is a replication divergence — a violation of INV-1.

---

### 1.8 `bench/` — Benchmarks

```
bench/
  src/main/java/broker/bench/
    PublishThroughputBenchmark.java
    PublishLatencyBenchmark.java
    LoadGenerator.java
    ResultsWriter.java
```

JMH benchmarks use `@State(Scope.Benchmark)` with a `ProducerClient` warmed up in `@Setup`. `LoadGenerator` is a standalone multi-threaded driver used by `ChaosOrchestrator` for sustained load during fault injection. `ResultsWriter` parses JMH JSON output and appends formatted Markdown tables to `docs/results.md`.

---

## 2. Services

A "service" here means a long-running logical unit within the broker process. Each service runs on one or more virtual threads and has a defined lifecycle (start, running, shutdown).

### 2.1 Service Map

```
BrokerServer process
├── ConnectionAcceptor          (1 virtual thread)
├── RequestHandler pool         (1 virtual thread per active connection)
├── RaftNode [per partition]
│   ├── ElectionTimer           (1 virtual thread per partition)
│   └── ReplicationPipeline     (1 virtual thread per follower per partition, leader only)
├── FsyncScheduler              (1 virtual thread, if FsyncPolicy=PERIODIC)
├── RetentionEnforcer           (1 virtual thread, runs periodically)
└── MetadataService             (stateless, called inline)
```

### 2.2 `ConnectionAcceptor`

**Thread model:** single virtual thread.  
**Lifecycle:** started by `BrokerServer.start()`; stopped by closing the `ServerSocket` (causes `accept