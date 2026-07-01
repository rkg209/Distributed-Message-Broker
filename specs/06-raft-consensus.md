---
id: "06"
title: Raft Consensus Core (Standalone Module)
status: todo
phase: 4
depends_on: ["05"]
requirements: [FR-35, FR-36, FR-37, FR-38, FR-39, FR-40, NFR-4, INV-4]
invariants: [INV-1, INV-4]
---

# Spec 06 · Raft Consensus Core (Standalone Module)

## What
Implement a from-scratch, standalone Raft consensus module in `raft/` that is
completely independent of broker-specific logic. The module exposes a
`StateMachine` interface; the broker plugs in later. Test it in isolation.

## Why
Raft is the correctness foundation of the entire system. Building and testing
it in isolation (no broker, no network) keeps the implementation focused and
the tests fast. A correct standalone Raft module earns INV-1 and INV-4.

This spec is the most complex single spec. Do not rush it.

## What to build (all in `raft/`)

### Core classes
- `RaftNode` — central state machine; owns `RaftRole` (FOLLOWER/CANDIDATE/LEADER),
  `currentTerm`, `votedFor`, `commitIndex`, `lastApplied`
- `RaftLog` — entry log; entries are `(term, index, commandBytes)`; backed by a
  `RaftLogStore` interface (in-memory for tests, disk-backed in production via `log/`)
- `PersistentState` — durably stores `(currentTerm, votedFor)` before any RPC response;
  single-record file with fsync
- `ElectionTimer` — randomized timeout (150–300ms, configurable); fires `startElection()`
- `ReplicationPipeline` — per-follower virtual thread tracking `nextIndex[peer]` and
  `matchIndex[peer]`; sends `AppendEntries`; processes responses
- `CommitAdvancer` — after each `AppendEntries` response, advances `commitIndex` when
  majority `matchIndex >= N` and `log[N].term == currentTerm` (§5.4 safety rule)

### Interfaces
```java
public interface StateMachine {
    ApplyResult apply(long raftIndex, byte[] command);
}

public interface RaftTransport {
    CompletableFuture<AppendEntriesResponse> appendEntries(
        int targetBrokerId, AppendEntriesRequest req);
    CompletableFuture<RequestVoteResponse> requestVote(
        int targetBrokerId, RequestVoteRequest req);
}

public interface RaftLogStore {
    void append(RaftEntry entry);
    RaftEntry get(long index);
    long lastIndex();
    long lastTerm();
    void truncateFrom(long index);
}
```

### RPC types (serialized via `protocol/`)
- `AppendEntriesRequest` / `AppendEntriesResponse`
- `RequestVoteRequest` / `RequestVoteResponse`

## Safety properties to verify in tests
1. **Election safety:** at most one leader per term (verified by mock transport that
   tracks how many nodes believe they are leader per term).
2. **Log matching:** if two logs have an entry with the same (term, index), all entries
   before it are identical (verified by comparing logs after a series of appends).
3. **Leader completeness:** a new leader has all committed entries (verified by checking
   log contents after re-election).
4. **State machine safety:** every applied entry at a given index is the same on all nodes.
5. **No commit without current term:** a leader does not commit entries from a previous
   term without first committing a no-op from its own term.

## Acceptance criteria
1. **Leader election:** a 3-node Raft cluster (in-process, mock transport) elects exactly
   one leader within 2× election timeout.
2. **Log replication:** the leader proposes 100 commands; all committed entries appear
   on all three nodes in the same order.
3. **Leader crash re-election:** the leader's virtual thread is interrupted; within
   2× election timeout a new leader is elected and replication continues. No committed
   entry is lost.
4. **Term fencing:** a node that was partitioned (missed 10 proposals) reconnects; it
   receives the missing entries via AppendEntries and catches up correctly.
5. **PersistentState:** after simulated crash (`currentTerm`, `votedFor` written to disk),
   node restores state and does not vote twice in the same term.
6. `/raft-review` (distributed-systems-reviewer subagent) passes before this spec is marked done.
7. `./gradlew test` GREEN with all above as JUnit 5 tests (deterministic via mock transport).

## Out of scope
- Integration with broker or PartitionLog (Spec 07).
- Cluster membership changes.
- Log snapshots / install-snapshot RPC.
