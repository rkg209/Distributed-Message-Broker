# Distributed Message Broker — Claude Code Constitution

## What this is
A from-scratch, Raft-replicated, durable message broker (mini-Kafka) in Java 21.
Headline guarantee: zero committed-message loss and zero duplication under crashes,
PROVEN by the chaos harness. Correctness is the deliverable.

## Locked decisions (do NOT re-litigate — see distributed-message-broker.md Part B)
- Replication: Raft, ONE Raft group per partition (multi-Raft).
- Protocol: custom length-prefixed binary over TCP, virtual-thread-per-connection.
- Delivery: idempotent producer (producer-id + per-partition seq) + at-least-once
  consumer with explicit offset commits. NOT full cross-partition transactions.
- Storage: append-only segments + fsync + sparse mmap offset index; retention by
  size/time only; NO log compaction.
- Membership: static config (Docker Compose); lightweight controller tracks leadership.
- Consumer groups: static assignment in core; rebalancing is a stretch spec only.
- Build: Gradle Kotlin DSL, JUnit 5, JMH, Testcontainers, Docker Compose.
- Framing: describe as "Kafka-style broker with Raft-replicated partitions", NOT as
  an implementation of Apache Kafka's ISR/controller architecture.

## Invariants that must ALWAYS hold (assert these in tests)
1. (INV-1) A committed write is never lost, even across leader crashes.
2. (INV-2) Per-partition ordering is preserved for a given producer.
3. (INV-3) No duplicate delivery from producer retries (idempotent producer dedupes on append).
4. (INV-4) Two leaders for the same partition can never both commit (leader epoch fences stale leaders).
5. (INV-5) Consumers only read committed (majority-replicated) data.

## SDD workflow (follow for every feature)
1. Specs live in /specs, numbered 00–14, in dependency order. Build them IN ORDER.
2. For a spec: /spec-plan first (produce a plan, get it reviewed), THEN /implement-spec.
3. Test-first where practical. A spec is DONE only when: code compiles, unit +
   property tests pass, and (where applicable) chaos/bench targets produce results
   recorded in docs/results.md.
4. Any replication/consensus change MUST be reviewed by the distributed-systems-reviewer
   subagent (/raft-review) before being considered done.
5. Mark a spec complete by adding `status: done` to its frontmatter and updating
   docs/results.md if it has measurable outputs.

## Current spec in progress
<!-- Update this line when starting a new spec -->
Spec 01 — Wire Protocol & Network Layer

## Build / test / run commands
- Build:            ./gradlew build
- Unit tests:       ./gradlew test
- Format:           ./gradlew spotlessApply
- Chaos (headline): ./gradlew chaosTest -Pcrashes=1000
- Benchmarks:       ./gradlew jmh
- Cluster up/down:  docker compose -f docker/docker-compose.yml up / down

## Module map (do NOT add cross-module dependencies without explicit justification)
```
broker/ → raft/, log/, protocol/
client/ → protocol/
chaos/  → client/
bench/  → client/
raft/   → (standalone, no broker deps)
log/    → (standalone, no broker deps)
protocol/ → (standalone, value types + codec only)
```

## Conventions
- Java 21. Use virtual threads for connection handling (Thread.ofVirtual() / newVirtualThreadPerTaskExecutor()).
- No silent failure: durability/replication errors propagate, never swallowed.
- Every public guarantee (INV-1 through INV-5) has a test that tries to break it.
- All configuration (timeouts, sizes, policies) externalized — never hardcoded.
- Segment files named {base-offset:020d}.log and {base-offset:020d}.index.
- Wire format: [4-byte length][1-byte type][payload]; all multi-byte fields big-endian.
- No JSON, no Protobuf, no gRPC, no Netty — custom binary codec is the portfolio signal.

## Git commit policy
- Never add a `Co-Authored-By: Claude` (or any Anthropic-related) trailer to commit
  messages. Commit messages should list only human authors.
