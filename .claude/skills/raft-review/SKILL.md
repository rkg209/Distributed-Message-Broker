---
name: raft-review
description: Audit consensus/replication code against the five correctness invariants. Run before marking any spec that touches raft/ or broker replication as done.
---

Trigger a focused correctness review of the consensus and replication code by the
`distributed-systems-reviewer` subagent.

## What the subagent reviews
The changed files in `raft/` and the replication/failover code in `broker/`
(specifically `PartitionReplica`, `ReplicationCoordinator`, `BackpressureController`,
and any epoch-fencing code).

## The five invariants to check
1. **INV-1** — No committed write is ever lost: check that `commitIndex` only advances
   after a majority has the entry, and that a new leader never drops committed entries.
2. **INV-2** — Per-partition ordering: check that the `PartitionLog` append is called
   only in Raft `apply()` order (no out-of-order applies).
3. **INV-3** — No producer-retry duplicates: check the idempotency store is rebuilt
   correctly on recovery and that duplicate detection is applied before `RaftNode.propose()`.
4. **INV-4** — No two committing leaders: check that leader epoch is incremented on
   every new election, that stale-epoch writes are rejected before append, and that
   no path bypasses the epoch check.
5. **INV-5** — Consumers read only committed data: check that `PartitionReplica.read()`
   is capped at `commitIndex` with no off-by-one.

## What to pass to the subagent
The diff of changed files since the last spec was marked done (or the full file list
if reviewing a new spec). Provide file paths, not just filenames.

## Output
The subagent returns a structured report:
- For each invariant: PASS / POTENTIAL VIOLATION / VIOLATION with a description
- For each violation or potential violation: the exact code path, the scenario that
  triggers it, and a suggested fix

If any VIOLATION is found, the spec is NOT done until fixed and re-reviewed.
