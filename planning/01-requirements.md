# requirements.md — Distributed Message Broker (mini-Kafka)

**Document version:** 1.0  
**Source:** `project.md` (raw specification)  
**Status:** Baseline — do not modify without updating version and change log

---

## 1. Business Goals

| ID | Goal |
|----|------|
| BG-1 | Demonstrate production-grade distributed-systems engineering as the anchor portfolio project for a senior Java/backend engineering role. |
| BG-2 | Prove, not merely claim, zero committed-message loss and zero duplication under approximately 1,000 induced leader crashes over approximately 10 million messages, via an automated chaos harness. |
| BG-3 | Produce a publicly reproducible correctness artifact: a stranger can clone the repository, run one command, kill a broker mid-publish, and observe the no-loss guarantee hold. |
| BG-4 | Establish measured performance baselines (throughput, latency, cost-of-durability delta) that can be cited verbatim on a resume and defended in a technical interview. |
| BG-5 | Validate a spec-driven development workflow (SDD with Claude Code) that can be reused across other portfolio projects. |

---

## 2. Stakeholders

| ID | Stakeholder | Interest |
|----|-------------|----------|
| SH-1 | **Portfolio owner / engineer** | Builds and maintains the system; primary author of all specs, code, and results. |
| SH-2 | **Recruiting audience (recruiters, hiring managers)** | Reads the resume bullet and README headline; needs a credible, scannable claim backed by a green CI badge and a results table. |
| SH-3 | **Technical interviewers** | Probes the design decisions, invariants, and measured results; needs defensible answers to questions about durability, replication, failover, ordering, and backpressure. |
| SH-4 | **Open-source / peer reviewers** | May clone the repository and inspect code quality, test coverage, and the chaos harness; needs a clean, navigable codebase with a one-command demo. |
| SH-5 | **Claude Code (AI development assistant)** | Consumes specs and the project constitution (`CLAUDE.md`) to produce plans, implementations, and tests; needs unambiguous, numbered requirements and locked design decisions. |

---

## 3. Users / Personas

| ID | Persona | Description | Primary Interactions |
|----|---------|-------------|----------------------|
| U-1 | **Producer client** | An application (or test harness) that publishes messages to a named topic. Expects durable, ordered, deduplicated acknowledgement. | `publish(topic, key, payload)` → acknowledged offset |
| U-2 | **Consumer client** | An application (or test harness) that polls a topic partition for ordered messages and commits its read offset. Expects to receive every committed message exactly once per consumer group. | `poll(topic, partition, offset)` → ordered records; `commitOffset(group, partition, offset)` |
| U-3 | **Cluster operator** | Configures and operates the multi-broker cluster via Docker Compose and static configuration files. Monitors broker health and partition leadership. | Edit static config; `docker compose up/down`; inspect logs and `docs/results.md` |
| U-4 | **Chaos harness** | Automated test process that injects leader kills, network partitions, and disk faults, then verifies correctness invariants. | `./gradlew chaosTest -Pcrashes=1000` |
| U-5 | **Benchmark harness** | JMH-based load generator that measures throughput and latency at varying replication factors and message sizes. | `./gradlew jmh` → `docs/results.md` |

---

## 4. Functional Requirements

### 4.1 Wire Protocol & Network Layer

| ID | Requirement |
|----|-------------|
| FR-1 | The system SHALL implement a custom length-prefixed binary request/response protocol over TCP for all client-to-broker and broker-to-broker communication. |
| FR-2 | The broker SHALL accept one virtual thread per TCP connection, using Java 21 virtual threads for all connection handling. |
| FR-3 | The protocol SHALL define explicit message types for at minimum: `Publish`, `Poll`, `CommitOffset`, `Metadata`, `AppendEntries` (Raft), and `RequestVote` (Raft). |
| FR-4 | The protocol framing SHALL include a length prefix sufficient to delimit variable-length payloads. |
| FR-5 | The server SHALL reject malformed frames with a well-defined error response and SHALL NOT crash or silently discard them. |
| FR-6 | A client library SHALL be provided that encodes and decodes the binary protocol, enabling producers and consumers to communicate with the broker without manual frame construction. |

### 4.2 Topic & Partition Model

| ID | Requirement |
|----|-------------|
| FR-7 | The system SHALL support named **topics**, each subdivided into one or more **partitions**. |
| FR-8 | Each partition SHALL be the unit of ordering: messages within a single partition SHALL be delivered to consumers in the order they were appended. |
| FR-9 | The system SHALL route messages with the same key to the same partition, preserving per-key ordering within a topic. |
| FR-10 | Each partition SHALL be independently replicated across brokers as its own Raft group (multi-Raft). |

### 4.3 Append-Only Log & Durability

| ID | Requirement |
|----|-------------|
| FR-11 | Each partition SHALL be persisted to disk as a sequence of rolling **segment files** in an append-only format. |
| FR-12 | The broker SHALL apply an **fsync** policy to segment files such that an acknowledged write survives a process crash or OS restart. |
| FR-13 | Each partition log SHALL maintain a **sparse memory-mapped offset index** enabling O(log n) lookup of a record by offset. |
| FR-14 | On broker restart, the log module SHALL recover all previously acknowledged offsets correctly from the segment files and index, without data loss or corruption. |
| FR-15 | The log SHALL enforce **retention by size and/or time**: segments exceeding the configured size or age SHALL be deleted. Log compaction is explicitly out of scope. |
| FR-16 | Every record appended to the log SHALL be assigned a **monotonically increasing offset** within its partition. |

### 4.4 Publish (Producer) API

| ID | Requirement |
|----|-------------|
| FR-17 | A producer SHALL be able to publish a message to a named topic with an optional routing key and an arbitrary byte-array payload. |
| FR-18 | The broker SHALL return an acknowledgement to the producer only after the write has been committed (i.e., replicated to a majority of the partition's Raft group). |
| FR-19 | Each producer instance SHALL be assigned a unique **producer-id** by the broker or generated client-side. |
| FR-20 | The producer SHALL attach a **per-partition sequence number** to every published message. |
| FR-21 | The broker SHALL deduplicate messages on append: if a message with the same `(producer-id, partition, sequence-number)` tuple has already been committed, the broker SHALL return the original acknowledgement without appending a duplicate record. |
| FR-22 | The producer client SHALL support **retry** of unacknowledged publishes; combined with FR-21, retries SHALL NOT produce duplicate records in the log. |

### 4.5 Poll (Consumer) API

| ID | Requirement |
|----|-------------|
| FR-23 | A consumer SHALL be able to poll a specific `(topic, partition, offset)` and receive an ordered batch of committed records starting at that offset. |
| FR-24 | The broker SHALL return **only committed records** (those replicated to a majority) in response to a poll request; uncommitted records SHALL NOT be visible to consumers. |
| FR-25 | A consumer SHALL be able to explicitly commit its read offset for a `(consumer-group, topic, partition)` tuple. |
| FR-26 | Committed consumer offsets SHALL be durably stored so that a consumer restart resumes from the last committed offset without re-reading already-processed messages. |

### 4.6 Consumer Groups

| ID | Requirement |
|----|-------------|
| FR-27 | The system SHALL support **consumer groups**: multiple consumer instances sharing a group-id collectively consume a topic, each assigned a disjoint subset of partitions. |
| FR-28 | In the core implementation, partition assignment to consumers within a group SHALL be **static** (configured at startup). Dynamic rebalancing is a stretch requirement (FR-46). |
| FR-29 | Each consumer group SHALL maintain an independent committed offset per partition, so multiple groups can consume the same topic independently. |

### 4.7 Cluster Membership & Metadata

| ID | Requirement |
|----|-------------|
| FR-30 | Cluster membership SHALL be defined by **static configuration** (a fixed broker list provided via Docker Compose environment or configuration files). Dynamic membership discovery is out of scope. |
| FR-31 | A lightweight **controller/metadata role** SHALL track the current `partition → leader broker` assignment and make it queryable by clients and brokers. |
| FR-32 | Brokers SHALL detect a peer broker failure via **heartbeat** monitoring within a configurable timeout. |
| FR-33 | The controller SHALL update the partition→leader map when a leader changes (due to election or failure). |
| FR-34 | Clients SHALL be able to query the metadata service to discover which broker is the current leader for a given partition, and SHALL redirect requests accordingly after a leadership change. |

### 4.8 Raft Consensus Module

| ID | Requirement |
|----|-------------|
| FR-35 | The system SHALL include a **standalone, reusable Raft consensus module** (`raft/`) that is independent of broker-specific logic. |
| FR-36 | The Raft module SHALL implement: persistent state (current term, voted-for, log), **leader election** (`RequestVote` RPC), **log replication** (`AppendEntries` RPC), and **commit-index advancement** on majority acknowledgement. |
| FR-37 | The Raft module SHALL persist its state (term, voted-for, log entries) to durable storage before responding to any RPC, so that a crashed node can rejoin without violating safety. |
| FR-38 | A Raft group SHALL elect a stable leader when no leader is present, and SHALL re-elect after a leader failure, within a configurable election timeout. |
| FR-39 | A log entry SHALL be considered **committed** only after a majority of the Raft group has appended it to their local logs. |
| FR-40 | The Raft module SHALL be testable in isolation: a standalone Raft cluster of N nodes SHALL elect a leader, replicate a command log, commit on majority, and re-elect after a leader kill — all without broker code. |

### 4.9 Replicated Partitions & Leader Failover

| ID | Requirement |
|----|-------------|
| FR-41 | Each partition SHALL be backed by its own Raft group, with the durable append-only log (FR-11 through FR-16) serving as the replicated state machine. |
| FR-42 | A publish request SHALL be acknowledged to the producer only after the Raft group for that partition has committed the entry (majority replication). |
| FR-43 | Killing a **follower** broker in a partition's Raft group SHALL not affect the availability of that partition, provided a majority of the group remains alive. |
| FR-44 | On **partition-leader death**, the remaining brokers in the Raft group SHALL elect a new leader and resume serving publish and poll requests. |
| FR-45 | Each Raft leader election SHALL increment a **leader epoch**. A broker operating with a stale epoch SHALL be **fenced**: its writes SHALL be rejected and it SHALL NOT be able to commit entries after fencing. |

### 4.10 Backpressure & Flow Control

| ID | Requirement |
|----|-------------|
| FR-47 | The broker SHALL use **bounded internal queues** for in-flight messages; when queues are full, the broker SHALL apply backpressure to producers rather than unboundedly buffering. |
| FR-48 | A **slow or stalled consumer** SHALL NOT exhaust broker memory, crash the broker, or degrade throughput for other consumers or producers. |
| FR-49 | The producer client SHALL support **throttling** when the broker signals backpressure, backing off rather than flooding the broker. |

### 4.11 Chaos / Fault-Injection Harness

| ID | Requirement |
|----|-------------|
| FR-50 | The system SHALL include an automated **fault-injection harness** (`chaos/`) capable of injecting: leader process kills, network partitions between brokers, and slow/full-disk conditions. |
| FR-51 | The harness SHALL execute a **headline correctness test**: approximately 1,000 induced leader crashes over approximately 10 million messages, verifying zero message loss and zero duplication. |
| FR-52 | The harness SHALL verify **no data divergence** across N injected network partitions (no split-brain). |
| FR-53 | The harness SHALL include a **linearizability / consistency checker** that validates operation histories across randomized schedules. |
| FR-54 | All chaos test results SHALL be recorded in `docs/results.md` with pass/fail counts and the exact parameters used. |

### 4.12 Benchmarks

| ID | Requirement |
|----|-------------|
| FR-55 | The system SHALL include **JMH benchmarks** (`bench/`) measuring: sustained throughput (messages/sec) at 1 KB payloads for RF=1 and RF=3, and publish-to-commit latency (p50, p99) at RF=3. |
| FR-56 | Benchmark results SHALL be recorded in `docs/results.md` as throughput/latency tables, including the **RF=1 vs RF=3 cost-of-durability delta**. |
| FR-57 | The benchmark suite SHALL include a **load generator** capable of sustaining the target throughput for a configurable duration. |

### 4.13 Stretch Requirements (post-headline only)

| ID | Requirement |
|----|-------------|
| FR-46 | *(Stretch)* The system MAY implement **dynamic consumer-group rebalancing**: a group coordinator that reassigns partitions when consumers join or leave, with no double-consumption and no gap. |
| FR-58 | *(Stretch)* The system MAY be extended with a **plugin packaging** of the `.claude/` SDD workflow for reuse across other portfolio projects. |

---

## 5. Non-Functional Requirements

### 5.1 Correctness & Safety

| ID | Requirement | Target |
|----|-------------|--------|
| NFR-1 | **No committed-message loss:** a write acknowledged by the broker SHALL survive any single broker crash, any leader failover, and any network partition that does not permanently eliminate a majority of the Raft group. | 0 lost messages across the headline chaos test (FR-51). |
| NFR-2 | **No duplication from producer retries:** the idempotent producer (FR-20, FR-21) SHALL ensure that retrying an unacknowledged publish never produces a duplicate record in the log. | 0 duplicate records across the headline chaos test. |
| NFR-3 | **Per-partition ordering:** records within a single partition SHALL be delivered to consumers in append order, regardless of leader changes. | Verified by the chaos harness and linearizability checker (FR-53). |
| NFR-4 | **No split-brain:** at most one broker SHALL act as the committed leader for a given partition at any time; a stale leader SHALL be fenced before any new leader can commit. | 0 split-brain events across N injected network partitions (FR-52). |
| NFR-5 | **Consumers read only committed data:** a consumer poll SHALL never return a record that has not been committed by a majority of the partition's Raft group. | Enforced at the broker read path; verified by the chaos harness. |
| NFR-6 | **Linearizability:** the system's operation history under randomized schedules SHALL pass the linearizability checker (FR-53). | Checker passes across all randomized schedules in the test suite. |

### 5.2 Performance

| ID | Requirement | Target |
|----|-------------|--------|
| NFR-7 | **Throughput (RF=3):** the broker cluster SHALL sustain at least approximately 200,000 messages/sec at 1 KB payloads with replication factor 3. | ≥ 200K msgs/sec (measured; replace with actual). |
| NFR-8 | **Publish-to-commit latency (RF=3):** the p99 end-to-end latency from producer publish to committed acknowledgement SHALL be at most approximately 8 ms at RF=3 under the target throughput. | p99 ≤ ~8 ms (measured; replace with actual). |
| NFR-9 | **Cost-of-durability delta:** the throughput difference between RF=1 and RF=3 SHALL be measured and reported. | Quantified in `docs/results.md`; no pass/fail threshold, but must be reported. |
| NFR-10 | **Bounded memory under slow consumers:** broker heap usage SHALL remain bounded when a consumer is stalled indefinitely, with no OutOfMemoryError. | No OOM observed in the backpressure test (FR-48). |

### 5.3 Reliability & Availability

| ID | Requirement | Target |
|----|-------------|--------|
| NFR-11 | **Crash recovery:** a broker that restarts after a crash SHALL recover its log state correctly and rejoin its Raft groups without data loss or log divergence. | Recovery verified by restart tests in the chaos harness. |
| NFR-12 | **Majority-fault tolerance:** a partition with replication factor RF SHALL remain available as long as a majority (⌊RF/2⌋ + 1) of its Raft group is alive. | Verified for RF=3 (tolerates 1 failure) in the chaos harness. |
| NFR-13 | **Failover time:** after a leader crash, a new leader SHALL be elected and the partition SHALL resume serving requests within a configurable election timeout. | Configurable; default target ≤ a few seconds in a LAN environment. |

### 5.4 Maintainability & Code Quality

| ID | Requirement |
|----|-------------|
| NFR-14 | The codebase SHALL be organized into the module structure defined in FR-35 and the repository layout (`protocol`, `log`, `raft`, `broker`, `client`, `chaos`, `bench`), with no cross-module dependency violations. |
| NFR-15 | Consensus, log, protocol, and broker concerns SHALL be kept in separate modules with well-defined interfaces; no module SHALL silently swallow durability or replication errors. |
| NFR-16 | All public guarantees (NFR-1 through NFR-6) SHALL have at least one automated test that attempts to violate them. |
| NFR-17 | Code formatting SHALL be enforced by Spotless (applied automatically via the `PostToolUse` hook on `.java` file edits). |
| NFR-18 | Every spec SHALL be considered done only when: the code compiles, unit and property tests pass, and (where applicable) chaos/benchmark results are recorded in `docs/results.md`. |

### 5.5 Observability & Documentation

| ID | Requirement |
|----|-------------|
| NFR-19 | The system SHALL emit structured logs sufficient to diagnose leader elections, failovers, and replication errors in a running cluster. |
| NFR-20 | `docs/results.md` SHALL be kept current with the latest chaos test and benchmark results, and SHALL be the single source of truth for all headline numbers. |
| NFR-21 | `docs/architecture.md` SHALL document all locked design decisions (Part B of `project.md`) and serve as the interview reference document. |
| NFR-22 | The `README.md` SHALL include: an architecture diagram, the headline results table, and a one-command demo (`docker compose up` + kill-a-broker script). |

---

## 6. Constraints

| ID | Constraint |
|----|------------|
| CON-1 | **Language:** Java 21 only. No other JVM languages in production modules. |
| CON-2 | **Replication algorithm:** Raft only, one Raft group per partition. The ISR/controller model used by Apache Kafka is explicitly excluded. |
| CON-3 | **Network protocol:** custom length-prefixed binary over TCP only. No use of existing messaging frameworks (e.g., gRPC, Netty, Aeron) for the broker wire protocol. |
| CON-4 | **IO model:** virtual-thread-per-connection blocking IO only. NIO selectors and async IO frameworks are excluded from the connection-handling path. |
| CON-5 | **Delivery semantics scope:** idempotent producer + at-least-once consumer with explicit offset commits. Full cross-partition transactions and read-committed isolation are explicitly out of scope. |
| CON-6 | **Storage:** append-only segment files + fsync + sparse memory-mapped offset index. Log compaction is explicitly out of scope. |
| CON-7 | **Cluster membership:** static configuration via Docker Compose. Dynamic membership discovery and gossip protocols are out of scope. |
| CON-8 | **Consumer group assignment:** static partition assignment in the core implementation. Dynamic rebalancing is a stretch requirement (FR-46) and SHALL NOT gate the headline correctness deliverable. |
| CON-9 | **Build system:** Gradle with Kotlin DSL only. Maven and other build tools are excluded. |
| CON-10 | **Spec ordering:** specs SHALL be implemented in the order defined in Part E of `project.md` (Specs 00–14). No spec SHALL be started before all its declared predecessors are complete. |
| CON-11 | **No silent failure:** durability and replication errors SHALL propagate to callers; they SHALL NOT be swallowed, logged-and-ignored, or converted to silent no-ops. |
| CON-12 | **Framing of the project:** the system SHALL be described as "a Kafka-style broker with Raft-replicated partitions," never as an implementation of Apache Kafka's internal ISR/controller architecture. |

---

## 7. Technologies

### 7.1 Explicitly Specified

| Category | Technology | Version / Notes |
|----------|------------|-----------------|
| Language | Java | 21 (virtual threads required) |
| Build | Gradle | Kotlin DSL |
| Unit / integration testing | JUnit | 5 |
| Benchmarking | JMH | (Java Microbenchmark Harness) |
| Container-based integration testing | Testcontainers | Latest stable |
| Multi-broker cluster orchestration | Docker Compose | Static broker configuration |
| CI | GitHub Actions | Build + test on every push |
| Code formatting | Spotless | Enforced via Gradle plugin + hook |
| AI development assistant | Claude Code | SDD workflow; skills, subagents, hooks as defined in Part D |
| Version control / issue tracking | GitHub | GitHub MCP for issue/PR workflow |

### 7.2 Implied by Design Decisions

| Category | Technology / Approach | Implied By |
|----------|-----------------------|------------|
| Consensus | Custom from-scratch Raft implementation | CON-2, FR-35–FR-40 |
| Wire protocol | Custom length-prefixed binary codec | CON-3, FR-1–FR-6 |
| Storage | Segment files + `FileChannel` fsync + `MappedByteBuffer` index | FR-11–FR-15 |
| Connection handling | `Thread.ofVirtual()` / `Executors.newVirtualThreadPerTaskExecutor()` | CON-4, FR-2 |
| Property-based testing | A property/fuzz testing library (e.g., jqwik or QuickTheories) | NFR-16, FR-53 |
| Linearizability checking | Custom checker or Knossos-style history verifier | FR-53 |
| Fault injection | Custom chaos harness in `chaos/` module | FR-50–FR-54 |
| Logging | Structured logging framework (e.g., SLF4J + Logback) | NFR-19 |

---

## 8. Deployment Requirements

| ID | Requirement |
|----|-------------|
| DR-1 | The system SHALL be deployable as a **multi-broker cluster** using a single `docker compose -f docker/docker-compose.yml up` command, with no manual configuration steps beyond editing the static broker list. |
| DR-2 | The Docker Compose configuration SHALL define at minimum **3 broker instances** to support RF=3 replication and demonstrate majority-fault tolerance. |
| DR-3 | Each broker SHALL be packaged as a **Docker image** built from a `Dockerfile` in the `docker/` directory. |
| DR-4 | The cluster SHALL be fully torn down and all state removed by `docker compose -f docker/docker-compose.yml down`. |
| DR-5 | The chaos test (`./gradlew chaosTest -Pcrashes=1000`) SHALL be executable against a running Docker Compose cluster without manual intervention. |
| DR-6 | The benchmark suite (`./gradlew jmh`) SHALL be executable against a running Docker Compose cluster and SHALL write results to `docs/results.md` automatically. |
| DR-7 | A **demo script** SHALL be provided that: starts the cluster, begins a producer publishing a continuous stream, kills one broker mid-stream, and demonstrates that the consumer receives every message in order with zero loss — reproducible by a stranger from the README alone. |
| DR-8 | GitHub Actions CI SHALL run `./gradlew build` and `./gradlew test` on every push to the main branch, and the CI badge SHALL be visible in the README. |
| DR-9 | All Gradle tasks required for the standard workflow SHALL be documented in the README: `build`, `test`, `spotlessApply`, `chaosTest`, `jmh`, and the Docker Compose `up`/`down` commands. |
| DR-10 | Broker configuration (broker IDs, addresses, partition assignments, replication factors, retention policy, fsync policy, election timeouts) SHALL be externalized to configuration files or environment variables, not hardcoded. |

---

## Appendix A — Invariants (referenced by NFR-16 and CLAUDE.md)

These five invariants SHALL each have at least one automated test that attempts to violate them. They are the correctness deliverable of the project.

| INV | Statement |
|-----|-----------|
| INV-1 | A committed write is never lost, even across leader crashes. |
| INV-2 | Per-partition ordering is preserved for a given producer. |
| INV-3 | No duplicate delivery results from producer retries (idempotent producer deduplicates on append). |
| INV-4 | Two leaders for the same partition can never both commit (leader epoch fences stale leaders). |
| INV-5 | Consumers only read committed (majority-replicated) data. |

---

## Appendix B — Out-of-Scope Items (explicit exclusions)

The following are **deliberately excluded** and SHALL be treated as out of scope unless a stretch spec is explicitly activated after the headline correctness deliverable is complete.

| Item | Rationale |
|------|-----------|
| Full cross-partition transactions / read-committed isolation | Large scope addition with low incremental portfolio signal; would gate the headline. |
| Log compaction | Large implementation with little additional correctness signal beyond retention. |
| Dynamic cluster membership / gossip | Removes a whole class of complexity without weakening the distributed-systems story. |
| Dynamic consumer-group rebalancing | Stretch only (FR-46); must not gate the headline chaos test. |
| NIO selectors / async IO frameworks | Virtual-thread blocking IO is simpler, correct, and sufficient at this scale. |
| gRPC, Netty, Aeron, or other messaging frameworks | Custom protocol is the explicit portfolio signal (CON-3). |
| Apache Kafka ISR/controller replication model | Raft per partition is the chosen model (CON-2). |