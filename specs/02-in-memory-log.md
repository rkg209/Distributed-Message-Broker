---
id: "02"
title: In-Memory Log & Single-Topic Publish/Consume (Thin Slice)
status: todo
phase: 1
depends_on: ["00", "01"]
requirements: [FR-7, FR-8, FR-16, FR-17, FR-23, FR-24]
---

# Spec 02 · In-Memory Log & Single-Topic Publish/Consume (Thin Slice)

## What
Wire up a complete end-to-end publish/consume flow using an **in-memory log**
(no disk persistence yet). A producer publishes N messages over the real wire
protocol; a consumer polls and receives exactly those N messages in order.
This is the **first working slice** of the system.

## Why
A thin end-to-end slice proves the protocol, the topic/partition abstraction,
and the client library all connect correctly before introducing disk I/O or
distribution. It also establishes the interfaces that durable storage and Raft
will plug into later.

## What to build

### `log/` module
- `PartitionLog` interface (see architecture.md §2.3) — `append`, `read`, `nextOffset`, `firstOffset`
- `InMemoryPartitionLog` — implements `PartitionLog` using a thread-safe in-memory list
- `LogRecord` — immutable value type: offset, timestamp, key bytes, value bytes

### `broker/` module
- `TopicRegistry` — maps `(topic, partitionId)` → `PartitionLog`
- `PartitionManager` — creates/looks up partitions; routes publish/poll to the right log
- Full `RequestHandler.handlePublish()` and `RequestHandler.handlePoll()` against
  the in-memory log

### `client/` module
- `ProducerClient` — sends PUBLISH_REQ; receives PUBLISH_RESP with committed offset
- `ConsumerClient` — sends POLL_REQ with offset; receives POLL_RESP batch; tracks current offset
- `MetadataClient` — sends METADATA_REQ; caches partition→broker map; stubs single-broker for now

## Acceptance criteria
1. **End-to-end test:** a producer publishes 1,000 messages to `test-topic/partition-0`.
   A consumer polls from offset 0 and receives all 1,000 messages in order over the real TCP wire.
2. Messages have correct monotonically increasing offsets (0, 1, 2, …, 999).
3. A second consumer polling from offset 500 receives exactly messages 500–999.
4. A poll at an offset beyond the last message returns an empty batch (not an error).
5. The broker is a single instance (no replication yet); the "committed" offset is simply the
   append offset (replication is Spec 07).
6. `./gradlew test` is GREEN with the above scenarios covered as JUnit 5 integration tests.

## Interfaces locked by this spec (do not change in later specs without justification)
```java
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

## Out of scope
- Disk persistence (Spec 03).
- Multiple partitions / consumer groups (Spec 04).
- Replication (Spec 07).
- Idempotent producer (Spec 09).
- Backpressure (Spec 10).
