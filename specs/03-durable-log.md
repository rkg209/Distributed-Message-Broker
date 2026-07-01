---
id: "03"
title: Durable Append-Only Log
status: todo
phase: 2
depends_on: ["02"]
requirements: [FR-11, FR-12, FR-13, FR-14, FR-15, FR-16, NFR-1, NFR-11]
---

# Spec 03 · Durable Append-Only Log

## What
Replace the in-memory log with a disk-backed append-only log: rolling segment
files with a configurable fsync policy, a sparse memory-mapped offset index for
O(log n) lookups, crash-safe recovery on restart, and retention by size/time.

## Why
Durability is the first tier of the correctness headline. Without `fsync`-backed
persistence, acknowledged writes can be lost on a process crash. This spec makes
INV-1 possible at the single-broker level before replication is added.

## What to build (all in `log/`)

### Storage format
- **Segment files:** `{base-offset:020d}.log` — append-only binary records
- **Index files:**   `{base-offset:020d}.index` — sparse offset→file-position entries
- **LogRecord on disk:** `[8B offset][8B timestamp][4B key-len][key][4B val-len][value][4B CRC32]`

### Key classes
- `DiskPartitionLog` — production implementation of `PartitionLog` interface
- `LogSegment` — one `.log` file; appends via `FileChannel`; fsync on policy
- `OffsetIndex` — sparse index over a `.index` file via `MappedByteBuffer`; one entry per 4096 bytes (configurable)
- `SegmentManager` — rolls segments at size limit; deletes old segments per retention policy
- `LogRecovery` — on startup: verify complete segments, scan last segment from last index entry, truncate partial trailing write, rebuild `nextOffset`

### Fsync policies (configurable via `BrokerConfig`)
| Policy       | Behavior                    |
|--------------|-----------------------------|
| EVERY_WRITE  | fsync after every append    |
| PERIODIC     | fsync every N ms            |
| OS_MANAGED   | no explicit fsync           |

Default: `EVERY_WRITE` (correctness headline requires this).

## Acceptance criteria
1. **Crash-recovery test:** broker writes 10,000 messages with `EVERY_WRITE` policy.
   Process is killed (SIGKILL). On restart, `LogRecovery` reconstructs all acknowledged
   offsets exactly — zero loss, zero duplication, no corruption.
2. **Partial-write test:** a `.log` file is truncated mid-record (simulating a partial
   write). `LogRecovery` truncates cleanly to the last valid record; no exception thrown.
3. **Segment roll test:** with a small segment size limit (e.g. 1 MB), producing enough
   messages rolls multiple segments; all are readable end-to-end after recovery.
4. **Index lookup test:** `read(offset, maxBytes)` performs at most O(log n) segment
   lookups (verified by counting seeks, not wall-clock time).
5. **Retention test:** with a 2-segment size limit, old segments are deleted; offsets
   below `firstOffset()` return an appropriate error.
6. `./gradlew test` GREEN with all above scenarios as JUnit 5 tests.
7. End-to-end integration test from Spec 02 still passes with `DiskPartitionLog` substituted.

## Out of scope
- Log compaction (explicitly excluded, CON-6).
- Replication (Spec 07).
- Any Raft integration.
