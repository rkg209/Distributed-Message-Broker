# project-summary.md

---

# Distributed Message Broker (mini-Kafka) — Executive Summary

## What It Is

A message broker built from scratch in Java 21 that durably stores messages, replicates them across multiple servers, and survives server crashes without losing or duplicating a single confirmed message. Think of it as a stripped-down, fully transparent version of Apache Kafka — every component hand-built and every correctness claim machine-verified.

The system accepts messages from producers, writes them to an append-only disk log, replicates each write to a majority of servers using the Raft consensus algorithm, and serves those messages to consumers in strict order. When a server dies mid-stream, the cluster elects a new leader automatically and consumers resume exactly where they left off — no gaps, no duplicates.

---

## Who It Is For

**Hiring managers and technical interviewers** evaluating senior or staff-level Java/distributed-systems engineers. This project exists to answer the question *"can this person actually build and reason about distributed systems?"* with a reproducible proof rather than a verbal claim.

**The target audience for the artifact itself** is anyone who can clone a repository and run one command. The correctness guarantee — zero message loss and zero duplication across roughly one thousand induced server crashes — is not asserted; it is demonstrated live.

---

## What It Proves

Most portfolio projects demonstrate that a candidate can use a framework. This one demonstrates that a candidate can reason about, implement, and *verify* the hard parts of distributed systems that frameworks hide:

- **Durability.** Messages written to disk survive process crashes. Segment files, controlled fsync, and a memory-mapped offset index mean recovery on restart is deterministic and complete.

- **Consensus under failure.** Each partition runs its own Raft group. A write is only confirmed to the producer after a majority of replicas have it. A server can die at any moment and no confirmed write disappears.

- **Split-brain prevention.** Leader epochs fence stale leaders. Two servers can never simultaneously believe they are the authoritative leader and both commit conflicting data.

- **Idempotent delivery.** Producer retries carry sequence numbers; the broker deduplicates on append. Combined with explicit consumer offset commits, the system delivers effectively-once semantics without the complexity of full distributed transactions.

- **Measured performance.** Sustained throughput of approximately 200,000 messages per second at one-kilobyte payloads with replication factor three, with publish-to-commit p99 latency around eight milliseconds. The cost of the durability guarantee — the throughput delta between RF=1 and RF=3 — is measured and reported, not estimated.

---

## Why It Matters as a Portfolio Piece

Distributed systems correctness is notoriously difficult to claim and easy to fake. The distinguishing feature of this project is its **chaos harness**: an automated test that kills leaders, injects network partitions, and stresses disk I/O across millions of messages, then checks that every confirmed write is present exactly once in every consumer's view. The results are recorded in a public results file that any reviewer can reproduce.

The project is built spec-driven — every feature starts as a written specification with explicit acceptance criteria, proceeds through a reviewed implementation plan, and is considered done only when the tests pass and the numbers are recorded. This mirrors the engineering process at companies that take distributed systems seriously, and gives the builder a concrete, defensible answer to every standard interview question about the design.

---

## The One-Command Demo

```bash
docker compose up          # start a 3-broker cluster
# kill one broker mid-publish
docker compose stop broker-2
# consumer receives every message, in order, with no gaps
```

That demo, backed by the chaos results table, is the deliverable. Everything else in the project exists to make it true.