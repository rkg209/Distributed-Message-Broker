---
id: "13"
title: Dynamic Consumer-Group Rebalancing (STRETCH)
status: stretch
phase: 7
depends_on: ["12"]
requirements: [FR-46]
---

# Spec 13 · Dynamic Consumer-Group Rebalancing (STRETCH)

> **STRETCH SPEC** — Do not start until the headline chaos test (Spec 11) and
> benchmarks (Spec 12) are complete and results are recorded. This spec must
> NOT gate the correctness deliverable.

## What
Add a group coordinator that automatically reassigns partitions among consumers
in a group when members join or leave, with a rebalance protocol that avoids
double-consumption and message gaps.

## Why
Dynamic rebalancing is what makes consumer groups scale elastically. While not
required for the correctness headline, it rounds out the Kafka-style feature set
and is an impressive addition if time permits.

## What to build

### `broker/`
- `GroupCoordinator` — tracks group membership; detects consumer leave/join via
  heartbeat timeout; triggers rebalance
- `RebalanceProtocol` — assigns partitions to consumers (range or round-robin);
  sends `REBALANCE` notification to all group members; waits for all members to
  acknowledge before activating the new assignment
- `REBALANCE_REQ` / `REBALANCE_RESP` message types (add to `protocol/`)

### `client/`
- `ConsumerClient` — handles `REBALANCE` notification: pauses consumption, commits
  current offset, acknowledges rebalance, resumes on new partition assignment

## Acceptance criteria
1. Consumer A and consumer B in group G each own 2 partitions of a 4-partition topic.
   Consumer C joins; group rebalances to A=2, B=1, C=1. No message is delivered twice
   or skipped during rebalance.
2. Consumer B leaves. Group rebalances to A=2, C=2. Same correctness guarantee.
3. Rebalance completes within 2× `rebalanceTimeout`.
4. `./gradlew test` GREEN with the above as integration tests.

## Out of scope
- Incremental / cooperative rebalancing (simple eager rebalance is sufficient).
