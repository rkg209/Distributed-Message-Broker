# database-design.md

**Version:** 1.0
**Source:** `planning/02-architecture.md`, `planning/03-system-design.md`
**Status:** Implementation Reference

---

## Table of Contents

1. [Design Philosophy](#1-design-philosophy)
2. [Storage Taxonomy](#2-storage-taxonomy)
3. [Entity Catalog](#3-entity-catalog)
4. [Schema Definitions](#4-schema-definitions)
5. [Relationships and Cardinality](#5-relationships-and-cardinality)
6. [Data Ownership by Module](#6-data-ownership-by-module)
7. [Multi-Tenancy Model](#7-multi-tenancy-model)
8. [Durability and Recovery Contracts](#8-durability-and-recovery-contracts)
9. [Retention and Lifecycle](#9-retention-and-lifecycle)
10. [Consistency Guarantees per Store](#10-consistency-guarantees-per-store)

---

## 1. Design Philosophy

This system does **not** use a general-purpose relational database (PostgreSQL, MySQL, SQLite) anywhere in its runtime path. Every persistent store is a purpose-built, append-only binary file whose schema is defined by the hand-written codec in `protocol/`. This is a deliberate architectural choice: the correctness and performance properties of the system depend on knowing exactly what bytes hit disk, when `fsync` is called, and in what order. A general-purpose database would introduce an opaque durability layer that cannot be reasoned about at the invariant level.

The consequence is that this document describes a **logical schema** — the entities, their fields, their relationships, and the binary layouts that encode them — rather than DDL for a SQL engine. Each section maps logical entities to the physical file format that stores them.

Three principles govern every storage decision:

1. **Append-only by default.** No record is ever overwritten in place. Mutation is expressed as a new record that supersedes an older one. This makes crash recovery a scan-to-last-valid-record operation rather than a redo/undo log problem.

2. **fsync before ack.** No acknowledgement is sent to a client or a Raft peer until the relevant bytes have been flushed to the OS page cache and confirmed durable by `FileChannel.force()`. The specific policy is configurable, but the default (`EVERY_WRITE`) is the conservative one.

3. **One writer per file.** Every file has exactly one logical writer at any time. There are no concurrent appenders to the same segment file. This eliminates the need for file-level locking and makes the append position a simple monotonic counter.

---

## 2. Storage Taxonomy

The system maintains five distinct categories of persistent state. Each has different durability requirements, access patterns, and ownership boundaries.

| Store ID | Name | Owner Module | File Pattern | Access Pattern | Fsync Policy |
|----------|------|-------------|--------------|----------------|--------------|
| S1 | Partition Message Log | `log/` | `{partitionDir}/{baseOffset:020d}.log` | Append-only write; sequential read by offset | Configurable; default `EVERY_WRITE` |
| S2 | Offset Index | `log/` | `{partitionDir}/{baseOffset:020d}.index` | Sparse append on write; binary-search read | Written with log segment; `MappedByteBuffer` |
| S3 | Raft Persistent State | `raft/` | `{partitionDir}/raft-state.bin` | Single-record overwrite on term/vote change | `force(true)` (metadata flush) always |
| S4 | Consumer Group Offset Log | `broker/` | `{offsetDir}/{groupId}.offsets` | Append-only write; full scan on recovery | `EVERY_WRITE` |
| S5 | Broker Metadata (runtime) | `broker/` | In-memory only | Read-heavy; written on leadership change | N/A — derived from S3 on recovery |

**S5 is not persisted independently.** The partition→leader mapping is reconstructed at startup from the Raft state (S3) and the current election outcome. It is never written to disk as a separate file because it is a derived, volatile view of the Raft consensus state.

---

## 3. Entity Catalog

The following logical entities exist in the system. Each entity is described by its identity, its fields, its owning store, and the invariants that govern it.

### 3.1 Topic

A named logical channel. Topics are not persisted as first-class records; they are implied by the existence of partition directories. A topic exists if and only if at least one partition directory for it exists on at least one broker.

| Field | Type | Notes |
|-------|------|-------|
| `name` | UTF-8 string, max 255 bytes | Unique across the cluster; used as a directory name component |
| `partitionCount` | int | Determined at cluster configuration time; encoded in `BrokerConfig` |

**Identity:** `name`
**Owning store:** `BrokerConfig` (static configuration file, not a runtime store)
**Persistence:** configuration only; no runtime record

---

### 3.2 Partition

A totally-ordered, append-only sub-stream of a topic. The unit of Raft consensus, log storage, and consumer assignment.

| Field | Type | Notes |
|-------|------|-------|
| `topicName` | string | Foreign key to Topic |
| `partitionId` | int | Zero-based index within the topic |
| `replicaSet` | int[] | Ordered list of broker IDs that hold a replica; from `BrokerConfig` |
| `directoryPath` | string | Absolute path on the local broker filesystem |

**Identity:** `(topicName, partitionId)`
**Owning store:** `BrokerConfig` (static); runtime state in `PartitionManager` (in-memory)
**Persistence:** directory existence on disk; no explicit record

---

### 3.3 LogRecord

The atomic unit of message storage. One `LogRecord` corresponds to one producer message that has been committed by Raft majority and applied to the partition log.

| Field | Type | Size | Notes |
|-------|------|------|-------|
| `offset` | long | 8 B | Partition-scoped, monotonically increasing from 0; assigned by `PartitionLogImpl.apply()` |
| `timestamp` | long | 8 B | Wall-clock milliseconds at the time the leader applied the entry; set by `PartitionReplicaImpl` |
| `keyLength` | int | 4 B | Byte length of `key`; may be 0 |
| `key` | byte[] | variable | Producer-supplied record key; used for routing and idempotency tracking |
| `valueLength` | int | 4 B | Byte length of `value` |
| `value` | byte[] | variable | Producer-supplied message payload |
| `crc32` | int | 4 B | CRC32 over all preceding bytes in this record; verified on every read |

**Identity:** `(topicName, partitionId, offset)` — globally unique
**Owning store:** S1 (Partition Message Log)
**Immutability:** once written, a `LogRecord` is never modified. Retention deletes whole segment files, never individual records.

**On-disk layout (big-endian):**

```
┌──────────┬───────────┬────────────┬───────────┬─────────────┬──────────┬──────────┐
│ offset   │ timestamp │ keyLength  │ key bytes │ valueLength │ value    │ CRC32    │
│ 8B       │ 8B        │ 4B         │ variable  │ 4B          │ variable │ 4B       │
└──────────┴───────────┴────────────┴───────────┴─────────────┴──────────┴──────────┘
```

Total minimum size (empty key, empty value): 32 bytes.

---

### 3.4 OffsetIndexEntry

A sparse index entry mapping a logical offset to a byte position within a `.log` file. Not a user-visible entity; purely an internal acceleration structure.

| Field | Type | Size | Notes |
|-------|------|------|-------|
| `logicalOffset` | long | 8 B | The `LogRecord.offset` of the indexed record |
| `filePosition` | int | 4 B | Byte offset from the start of the `.log` file |

**Identity:** `(segmentBaseOffset, logicalOffset)` — one entry per `indexIntervalBytes` of log data
**Owning store:** S2 (Offset Index)
**Note:** `filePosition` is stored as a 4-byte int (not 8-byte long) because segment files are capped at 1 GB (`maxSegmentBytes`), making 32-bit positions sufficient. This matches the Kafka index format for the same reason.

**On-disk layout:**

```
┌──────────────────┬──────────────────┐
│ logicalOffset 8B │ filePosition  4B │  (12 bytes per entry, fixed-width)
└──────────────────┴──────────────────┘
```

The index file is a flat array of these 12-byte entries, memory-mapped via `MappedByteBuffer`. Binary search over the array finds the largest `logicalOffset ≤ targetOffset` in O(log n) comparisons.

---

### 3.5 RaftEntry

A Raft log entry. Wraps a serialized `LogRecord` (or, for the no-op entry a new leader appends on election, an empty command) with Raft metadata.

| Field | Type | Size | Notes |
|-------|------|------|-------|
| `term` | long | 8 B | The Raft term in which this entry was created |
| `index` | long | 8 B | The Raft log index (1-based, monotonically increasing per partition) |
| `commandLength` | int | 4 B | Byte length of `command` |
| `command` | byte[] | variable | Serialized `LogRecord` bytes, or empty for no-op |

**Identity:** `(partitionId, index)` — globally unique per partition
**Owning store:** S1 (Partition Message Log) — `RaftLogImpl` encodes the `term` and `index` into the `key` field of the `LogRecord`, so the Raft log and the message log share the same physical file. See §6.1 for the encoding detail.
**Relationship to LogRecord:** one-to-one after commit. Before commit, a `RaftEntry` exists in the Raft log but has no corresponding committed `LogRecord` offset. The `StateMachine.apply()` call is the moment a `RaftEntry` becomes a `LogRecord` with an assigned offset.

---

### 3.6 RaftPersistentState

The two fields Raft requires to be durable before any RPC response: `currentTerm` and `votedFor`. Stored as a single fixed-size record with a CRC.

| Field | Type | Size | Notes |
|-------|------|------|-------|
| `currentTerm` | long | 8 B | Monotonically increasing; never decreases |
| `votedFor` | int | 4 B | Broker ID of the candidate this node voted for in `currentTerm`; -1 if none |
| `crc32` | int | 4 B | CRC32 over `currentTerm` and `votedFor` |

**Identity:** one record per partition per broker (the file is partition-scoped)
**Owning store:** S3 (Raft Persistent State)
**Write protocol:** the file is written in full on every change (16 bytes total), followed by `FileChannel.force(true)`. There is no append; the file is always exactly 16 bytes. On startup, if the CRC fails, the broker refuses to start for that partition — operator intervention is required because the node cannot safely determine its term or vote.

**On-disk layout:**

```
┌──────────────────┬──────────────┬──────────┐
│ currentTerm   8B │ votedFor  4B │ CRC32 4B │  (16 bytes total, fixed)
└──────────────────┴──────────────┴──────────┘
```

---

### 3.7 ConsumerGroupOffsetRecord

A durable record of a consumer group's committed read position for one partition. The offset log for a group is append-only; the current offset for a `(group, topic, partition)` triple is the value from the most recent record for that triple.

| Field | Type | Size | Notes |
|-------|------|------|-------|
| `partitionId` | int | 4 B | The partition whose offset is being committed |
| `topicLength` | short | 2 B | Byte length of `topicName` |
| `topicName` | byte[] | variable | UTF-8 encoded topic name |
| `committedOffset` | long | 8 B | The next offset the consumer will read (i.e., last processed offset + 1) |
| `timestamp` | long | 8 B | Wall-clock milliseconds at commit time |
| `crc32` | int | 4 B | CRC32 over all preceding bytes in this record |

**Identity:** `(groupId, topicName, partitionId)` — the file is group-scoped, so `groupId` is implicit in the file path
**Owning store:** S4 (Consumer Group Offset Log)
**Recovery:** on startup, `OffsetStore` scans the entire file sequentially, keeping the last valid `committedOffset` per `(topicName, partitionId)` key. Invalid records (CRC failure) terminate the scan; the file is truncated at the last valid record boundary, matching the same recovery pattern used by `LogRecovery`.

**On-disk layout:**

```
┌──────────────┬─────────────┬────────────────┬─────────────────┬───────────┬──────────┐
│ partitionId  │ topicLength │ topicName bytes │ committedOffset │ timestamp │ CRC32    │
│ 4B           │ 2B          │ variable        │ 8B              │ 8B        │ 4B       │
└──────────────┴─────────────┴────────────────┴─────────────────┴───────────┴──────────┘
```

---

### 3.8 ProducerSession

The in-memory deduplication state for one producer on one partition. Not persisted as a standalone file; reconstructed from the partition log on recovery.

| Field | Type | Notes |
|-------|------|-------|
| `producerId` | long | Assigned by the client library at startup (UUID-derived); unique per producer process |
| `partitionId` | int | The partition this session tracks |
| `lastCommittedSeq` | long | The sequence number of the most recently committed message from this producer on this partition |
| `lastCommittedOffset` | long | The log offset assigned to that message; returned as the ack for duplicate requests |

**Identity:** `(producerId, partitionId)`
**Owning store:** in-memory `IdempotencyStore` (a `ConcurrentHashMap`)
**Persistence:** derived from S1. On recovery, `IdempotencyStore` scans the partition log and extracts `(producerId, sequenceNumber)` from the `key` field of each `LogRecord`. The `key` field encoding is described in §6.1.

---

### 3.9 PartitionLeaderRecord (runtime only)

The current leader and epoch for a partition, as known to this broker. Entirely in-memory; derived from Raft state.

| Field | Type | Notes |
|-------|------|-------|
| `topicName` | string | |
| `partitionId` | int | |
| `leaderId` | int | Broker ID of the current Raft leader; -1 if unknown |
| `leaderEpoch` | long | The Raft term of the current leader; used for fencing (INV-4) |
| `replicaIds` | int[] | All broker IDs in the replica set |

**Identity:** `(topicName, partitionId)`
**Owning store:** `MetadataService` in-memory map
**Persistence:** none. Reconstructed from `RaftNode` state after startup and election.

---

## 4. Schema Definitions

This section presents the complete logical schema as entity definitions with field types, constraints, and the binary encoding rules that govern them. Think of this as the "CREATE TABLE" equivalent for a system that stores everything in binary files.

### 4.1 Encoding Conventions

All multi-byte integer fields are **big-endian** (network byte order). This is consistent throughout every file format in the system. String fields are always preceded by a length prefix; there are no null-terminated strings. Boolean fields are encoded as a single byte: `0x00` = false, `0x01` = true.

| Logical Type | Physical Encoding | Size |
|-------------|-------------------|------|
| `long` | signed 64-bit big-endian | 8 B |
| `int` | signed 32-bit big-endian | 4 B |
| `short` | signed 16-bit big-endian | 2 B |
| `byte` | unsigned 8-bit | 1 B |
| `boolean` | `byte` (0x00 / 0x01) | 1 B |
| `string` | `short` length + UTF-8 bytes | 2 B + n B |
| `long-string` | `int` length + UTF-8 bytes | 4 B + n B |
| `bytes` | `int` length + raw bytes | 4 B + n B |
| `crc32` | unsigned 32-bit big-endian | 4 B |

### 4.2 LogRecord (S1)

```
LogRecord {
    offset:        long        // constraint: offset >= 0; strictly increasing within segment
    timestamp:     long        // constraint: timestamp > 0 (epoch ms)
    keyLength:     int         // constraint: keyLength >= 0
    key:           byte[keyLength]
    valueLength:   int         // constraint: valueLength >= 0
    value:         byte[valueLength]
    crc32:         int         // covers bytes [0, recordStart + 28 + keyLength + 4 + valueLength)
}
```

**Constraints:**
- `offset` must equal `previousOffset + 1` within a segment (enforced by `PartitionLogImpl`; violation causes `LogRecovery` to truncate at the gap).
- `crc32` is verified on every read; a mismatch causes `LogRecovery` to truncate at that record.
- Maximum record size is bounded by the frame size limit (default 16 MB) minus frame overhead.

### 4.3 OffsetIndexEntry (S2)

```
OffsetIndexEntry {
    logicalOffset:  long   // constraint: strictly increasing within file
    filePosition:   int    // constraint: filePosition >= 0; < maxSegmentBytes (1 GB)
}
```

**Constraints:**
- Entries are written in strictly ascending `logicalOffset` order; binary search depends on this.
- `filePosition` fits in 32 bits because `maxSegmentBytes` ≤ 2^30 (1 GB).

### 4.4 RaftPersistentState (S3)

```
RaftPersistentState {
    currentTerm:  long   // constraint: currentTerm >= 0; never decreases across writes
    votedFor:     int    // constraint: votedFor == -1 OR votedFor is a valid broker ID
    crc32:        int    // covers [currentTerm, votedFor]
}
```

**Constraints:**
- The file is always exactly 16 bytes. Any other size on startup is treated as corruption.
- `currentTerm` must be ≥ the value from the previous write; a decrease indicates file corruption.

### 4.5 ConsumerGroupOffsetRecord (S4)

```
ConsumerGroupOffsetRecord {
    partitionId:      int
    topicLength:      short    // constraint: topicLength > 0; topicLength <= 255
    topicName:        byte[topicLength]
    committedOffset:  long     // constraint: committedOffset >= 0
    timestamp:        long     // constraint: timestamp > 0
    crc32:            int      // covers all preceding bytes in this record
}
```

**Constraints:**
- `committedOffset` must be ≤ the `commitIndex` of the partition's Raft log at the time of commit (enforced by `ConsumerGroupManager` before writing).
- Records with CRC failures are treated as the end of valid data; the file is truncated there on recovery.

### 4.6 Key Field Encoding for Idempotency and Raft

The `key` field of a `LogRecord` carries two embedded sub-fields that serve the idempotency store and the Raft log implementation. This multiplexing avoids a separate file for Raft metadata.

```
LogRecord.key encoding (when written by PartitionReplicaImpl):
┌──────────────┬──────────────────┬──────────────────────────┐
│ producerId   │ sequenceNumber   │ userKey bytes (optional) │
│ 8B           │ 8B               │ variable                 │
└──────────────┴──────────────────┴──────────────────────────┘

LogRecord.value encoding (when written by RaftLogImpl for Raft entries):
┌──────────┬───────────┬──────────────────────────────────┐
│ raftTerm │ raftIndex │ originalCommand bytes            │
│ 8B       │ 8B        │ variable                         │
└──────────┴───────────┴──────────────────────────────────┘
```

**Rationale:** Storing `raftTerm` and `raftIndex` in the `value` prefix means `RaftLogImpl` can reconstruct the full Raft log by scanning `LogRecord`s without a separate Raft log file. `IdempotencyStore` reads `producerId` and `sequenceNumber` from the `key` prefix during recovery without needing a separate index file.

This is a deliberate denormalization: the `LogRecord` is the single source of truth for both the message content and the Raft/idempotency metadata associated with it.

---

## 5. Relationships and Cardinality

```
Topic ──────────────────────────── has many ──── Partition
                                                     │
                                    ┌────────────────┼────────────────┐
                                    │                │                │
                               has many         has one          has many
                                    │                │                │
                               LogRecord    RaftPersistentState  ConsumerGroupOffsetRecord
                                    │            (per broker)         │
                               has one                           belongs to
                                    │                            ConsumerGroup
                             OffsetIndexEntry
                             (sparse; one per
                              indexIntervalBytes)
```

### 5.1 Cardinality Table

| Relationship | Cardinality | Notes |
|-------------|-------------|-------|
| Topic → Partition | 1 : N | `partitionCount` is fixed at configuration time |
| Partition → LogRecord | 1 : N (ordered) | Ordered by `offset`; offset is the primary key |
| Partition → LogSegment | 1 : N (ordered) | Segments are rolled at `maxSegmentBytes`; ordered by `baseOffset` |
| LogSegment → LogRecord | 1 : N (ordered) | All records in a segment share the same `.log` file |
| LogSegment → OffsetIndexEntry | 1 : N (sparse) | One entry per `indexIntervalBytes` of log data |
| Partition → RaftPersistentState | 1 : 1 per broker | Each broker stores its own Raft state for each partition it replicates |
| ConsumerGroup → ConsumerGroupOffsetRecord | 1 : N | One record per commit; last record per `(topic, partition)` is current |
| ConsumerGroup → Partition | M : N | A group may consume multiple partitions; a partition may be consumed by multiple groups |
| ProducerSession → Partition | M : 1 | Many producers may write to one partition; each has its own `(producerId, partitionId)` session |
| RaftEntry → LogRecord | 1 : 1 (after commit) | Before commit, a `RaftEntry` has no corresponding `LogRecord` offset |

### 5.2 Foreign Key Semantics

Because there is no relational engine enforcing referential integrity, the system enforces these relationships through code invariants:

- **`LogRecord.offset` continuity:** `PartitionLogImpl` maintains `nextOffset` in memory and assigns it atomically. `LogRecovery` verifies continuity on startup and truncates at the first gap.
- **`ConsumerGroupOffsetRecord.committedOffset` validity:** `ConsumerGroupManager.commit()` checks that `committedOffset ≤ raftNode.commitIndex()` before writing. A consumer cannot commit an offset for a message that has not been committed by Raft.
- **`RaftEntry.index` continuity:** `RaftLogImpl.append()` verifies that `entry.index == lastIndex() + 1`. `RaftNode` calls `truncateFrom()` before appending conflicting entries from a new leader, maintaining a contiguous log.

---

## 6. Data Ownership by Module

Each module owns exactly the stores it writes. No module reads from a store it does not own without going through the owning module's public interface. This is the storage equivalent of the module dependency rule.

### 6.1 `log/` Module

**Owns:** S1 (Partition Message Log), S2 (Offset Index)

**Writes:**
- `LogSegment.append()` writes `LogRecord` bytes to the active `.log` file.
- `OffsetIndex` writes `OffsetIndexEntry` records to the `.index` file when the index interval threshold is crossed.
- `LogRecovery.recover()` may truncate the active `.log` file (write via `FileChannel.truncate()`).
- `SegmentManager` deletes `.log` and `.index` files for expired segments.

**Reads:**
- `LogSegment.read()` reads `LogRecord` bytes from `.log` files.
- `OffsetIndex.lookup()` binary-searches the memory-mapped `.index` file.

**Does not own:** S3, S4. The `log/` module has no knowledge of Raft terms, consumer groups, or producer IDs at the storage level. Those concerns are encoded into the `key` and `value` bytes by the caller (`PartitionReplicaImpl` in `broker/`), but `log/` treats them as opaque byte arrays.

**Interface contract:**

```java
// The only way any other module reads or writes S1/S2
public interface PartitionLog {
    AppendResult append(LogRecord record);
    List<LogRecord> read(long fromOffset, int maxBytes);
    long nextOffset();
    long firstOffset();
    void flush();
    void recover();
    void close();
}
```

### 6.2 `raft/` Module

**Owns:** S3 (Raft Persistent State); uses S1 indirectly via `RaftLog` interface

**Writes:**
- `PersistentState.persist(term, votedFor)` writes and fsyncs `raft-state.bin`.
- `RaftLogImpl.append(entry)` calls `PartitionLog.append()` — but this call goes through the `PartitionLog` interface, so `raft/` does not directly own the file. The `raft/` module owns the logical Raft log; the physical storage is delegated to `log/`.
- `RaftLogImpl.truncateFrom(index)` calls a truncation method on `PartitionLog` — this is the one case where the log is not purely append-only. Truncation only removes uncommitted entries (those beyond `commitIndex`), so it never violates INV-1.

**Reads:**
- `PersistentState` reads `raft-state.bin` on startup.
- `RaftLogImpl.get(index)` reads `LogRecord`s via `PartitionLog.read()`.

**Does not own:** S1 directly (goes through interface), S2, S4.

**Key ownership note on S1/S3 co-location:** Both the Raft log (S1, via `RaftLogImpl`) and the Raft persistent state (S3) live in the same partition directory. This is intentional: a partition's entire durable state is self-contained in one directory, making backup, restore, and disaster recovery a directory-copy operation.

### 6.3 `broker/` Module

**Owns:** S4 (Consumer Group Offset Log); S5 (runtime metadata, in-memory only)

**Writes:**
- `OffsetStore.append()` writes `ConsumerGroupOffsetRecord` bytes to `{groupId}.offsets`.
- `MetadataService` updates its in-memory `PartitionLeaderRecord` map on leadership changes.
- `IdempotencyStore` updates its in-memory `ConcurrentHashMap` on each committed write.

**Reads:**
- `OffsetStore` reads `{groupId}.offsets` on startup for recovery.
- `PartitionManager` reads `PartitionLog` via the `PartitionLog` interface (owned by `log/`).
- `RaftNode` state is read via `RaftNode` public methods (owned by `raft/`).

**Does not own:** S1, S2, S3. The broker module is the integrator; it wires the other modules together but does not bypass their interfaces to touch their files.

**`PartitionReplicaImpl` as the integration point:** This class is where the three stores converge. It implements `StateMachine` (called by `raft/`), calls `PartitionLog` (owned by `log/`), and updates `IdempotencyStore` (owned by `broker/`). It is the only class in the system that has references to all three storage concerns simultaneously.

### 6.4 `client/` Module

**Owns:** no persistent storage.

The client library is entirely stateless with respect to disk. `ProducerClient` maintains `nextSequence` in memory; if the client process restarts, it re-reads the last committed sequence from the broker by sending a `METADATA_REQ` extended with a producer-state query. (For the MVP, the client simply starts sequence numbers at 0 and relies on the broker's idempotency store to detect duplicates from a previous session if the `producerId` is reused — which it will not be, since `producerId` is UUID-derived and unique per process instance.)

### 6.5 `chaos/` and `bench/` Modules

**Own:** no persistent storage in the broker's data path.

`HistoryRecorder` writes a JSON-lines event log to a test output directory. This is test infrastructure, not production data. `ResultsWriter` appends to `docs/results.md`. Neither file is part of the broker's durability contract.

---

## 7. Multi-Tenancy Model

### 7.1 Tenant Definition

In this system, the unit of logical isolation is the **consumer group**. A consumer group is the closest analogue to a "tenant" in a traditional multi-tenant system: it has its own committed offset state, its own read position, and its own durable offset log file.

Topics and partitions are **shared infrastructure** — there is no per-tenant topic namespace, no per-tenant partition, and no per-tenant Raft group. All consumer groups reading the same partition share the same physical log files.

### 7.2 Isolation Boundaries

| Resource | Isolation Level | Mechanism |
|----------|----------------|-----------|
| Message log (S1) | Shared | All consumers read the same `.log` files; isolation is by offset, not by file |
| Offset state (S4) | Per consumer group | Each group has its own `{groupId}.offsets` file; groups cannot read or write each other's offsets |
| Raft state (S3) | Per partition per broker | Not tenant-visible; internal to the broker |
| In-memory offset map | Per consumer group | `ConsumerGroupManager` keys its map by `(group