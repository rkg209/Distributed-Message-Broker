# Building a Distributed Message Broker From Scratch — Project Journal

This is the full story of how I designed, built, tested, and validated **mini-kafka**: a
from-scratch, Raft-replicated, durable message broker written in Java 21. I'm writing this
document mainly for myself — so that six months from now, or in an interview, I can explain
not just what I built but *why* every decision looks the way it does, what broke along the
way, and how I know the thing actually works instead of just believing it does. I've tried to
keep this honest: the numbers below are what I measured on my own laptop, not what I hoped for.

If you just want the headline results and a quick start, `README.md` has the short version.
This document is the long version.

---

## 1. Why I Built This

I wanted a portfolio project that could survive a real technical interview instead of folding
under the second follow-up question. Most side projects I'd built before this one demonstrated
that I could wire up a framework — a REST API here, a CRUD app there. None of them demonstrated
that I could reason about the actually hard parts of backend engineering: what happens when a
server dies in the middle of a write, how you prove that two machines never disagree about who's
in charge, how you guarantee a message gets delivered exactly once when the network itself is
unreliable.

Apache Kafka is the canonical example of a system that solves these problems, and I use it at
work, but using Kafka doesn't prove I understand Kafka. So I set myself a harder goal: build a
message broker that has Kafka's basic shape — topics, partitions, producers, consumer groups,
durable logs — but replicate it with Raft, from scratch, and then don't just claim it's correct.
*Prove* it's correct, with a chaos harness that induces real crashes and checks the result
against real invariants. That became the whole point of the project. The throughput numbers are
almost secondary; the headline claim is "zero committed-message loss and zero duplication under
crashes, and here's the test that tries to break that claim and fails."

I'm not trying to reimplement Apache Kafka's internals (its ISR/controller replication model in
particular is its own complicated thing), and I say so explicitly everywhere in this project's
docs — I describe this as a "Kafka-style broker with Raft-replicated partitions." Raft is a
single, well-understood, provably-correct consensus algorithm, and building one Raft group per
partition gets me the same guarantees Kafka's ISR+controller design gets, through a conceptually
simpler and more defensible path for a solo project.

---

## 2. How I Planned It

I didn't start writing Java on day one. I've been burned before by starting a distributed
systems project with vague ambitions and ending up with an untested pile of half-features six
weeks in, so this time I forced myself to write everything down before touching code.

### 2.1 The master design document

The very first artifact I wrote was a single file, `distributed-message-broker.md`, before any
code existed. It's split into three parts:

- **Part A**, the resume/interview framing: the exact, falsifiable claims I wanted to be able to
  make at the end — zero loss / zero duplication across roughly a thousand induced leader
  crashes over about ten million messages, ~200,000 msgs/sec at 1KB payloads with replication
  factor 3, p99 publish-to-commit latency around 8ms, and a measured (not guessed) throughput
  cost of running at RF=3 instead of RF=1.
- **Part B**, the locked architectural decisions — the ones I did not want to re-litigate three
  weeks in when I was tired and tempted to take a shortcut. Raft per partition. A custom binary
  wire protocol, not JSON or Protobuf or gRPC. Idempotent producer + at-least-once consumer, not
  full distributed transactions. Append-only segment storage with no compaction. Static cluster
  membership via Docker Compose, no gossip protocol. All of these are choices that trade away
  scope I didn't think would add much interview signal, in exchange for keeping the core
  correctness story clean.
- **Part C**, the ordered backlog: fifteen specs, numbered 00 through 14, each one building
  strictly on the last.

### 2.2 The planning folder

From that master document I expanded into a full `planning/` folder — an executive summary
(`01-project-summary.md`), a functional requirements doc with numbered FR-/NFR-/CON- items
(`01-requirements.md`), a component architecture doc, a detailed system design doc covering
replication and failover and delivery semantics, a storage/log layout design with an actual
schema file, and an API design doc with an OpenAPI-style spec for the wire-level messages. Was
this overkill for a solo project? Probably. But writing the requirements down with ID numbers
(FR-7, NFR-10, INV-3, and so on) meant that later, when I was implementing spec 10's backpressure
work, I could point at NFR-10 directly instead of re-deriving from scratch what "handle
backpressure" was supposed to mean.

The most important decision that came out of this planning phase wasn't technical at all — it
was the framing decision to never describe this as an Apache Kafka clone, and to always describe
the replication core as "Raft-replicated partitions" instead. That distinction matters in an
interview: claiming to have reimplemented Kafka's actual ISR/controller model would be both
untrue and an invitation to get picked apart on details I didn't build. Being precise about what
I actually built (and why I chose Raft instead of copying Kafka's design) is a more defensible
story and, frankly, a more interesting one to talk through.

### 2.3 Turning the plan into a spec-driven workflow

Rather than working from a giant to-do list, I split the whole roadmap into fifteen individual
spec files under `specs/00-foundations.md` through `specs/14-demo-polish.md`, each with YAML
frontmatter recording an id, a status, a phase number, and its dependency list, plus a `## What`
and `## Why` section and concrete acceptance criteria. Every spec had to be planned before it was
implemented, and nothing could jump ahead of its declared dependencies. Practically, this meant
I never had to hold the entire system in my head at once — spec 07 (replicated partitions) could
just assume spec 06 (the standalone Raft module) was done and tested, because it was.

I set myself one hard rule that I did not break: **any change touching Raft or replication code
had to pass a dedicated correctness review pass before I'd consider the spec done.** Consensus
bugs are exactly the kind of bug that looks fine in a quick read and only shows up three hours
into a chaos run, so I wanted a second, more adversarial pass specifically on that code before
moving on, every single time — not just once at the end.

---

## 3. Architecture, In Brief

The system, once all fifteen specs were done, looks like this:

```
                custom binary protocol over TCP (virtual-thread-per-connection)
Producer ──publish(topic, key, msg)──► Broker = partition leader (Raft leader for that partition)
                                          │  append to local log segment, assign offset
                                          ▼
                                  Raft AppendEntries → follower brokers
                                          │  entry commits once a MAJORITY of the Raft group has it
                                          ▼
                              committed offset advances (only committed data is readable)
                                          ▼
Consumer group ──poll(topic, partition, offset)──► ordered messages ; commit offset after processing

On leader crash:  the partition's Raft followers elect a new leader (higher term),
                  leader epoch fences the old leader, committed entries survive,
                  consumers resume from their committed offset → no loss, no dupes, no split-brain.
```

It's a Gradle multi-module build with a deliberately narrow dependency graph, enforced by
convention rather than a build-time check, but I never violated it:

```
broker/ → raft/, log/, protocol/
client/ → protocol/
chaos/  → client/
bench/  → client/
raft/   → (standalone, no broker deps)
log/    → (standalone, no broker deps)
protocol/ → (standalone, value types + codec only)
```

Keeping `raft/` and `log/` fully standalone (no dependency on `broker/`) turned out to be one of
the better decisions I made early on — it meant I could write pure, fast, non-Docker unit tests
against Raft leader election and log replication in complete isolation from network code, well
before the broker even had a wire protocol wired up to it.

The five invariants that everything else in this project exists to protect:

| # | Invariant |
|---|-----------|
| INV-1 | A committed write is never lost, even across leader crashes. |
| INV-2 | Per-partition ordering is preserved for a given producer. |
| INV-3 | No duplicate delivery from producer retries (idempotent producer dedupes on append). |
| INV-4 | Two leaders for the same partition can never both commit (leader epoch fences stale leaders). |
| INV-5 | Consumers only read committed (majority-replicated) data. |

Every one of these has at least one automated test whose entire purpose is to try to break it,
not just demonstrate the happy path.

The wire protocol is a hand-rolled `[4-byte length][1-byte type][payload]` binary framing,
everything big-endian, with one virtual thread per TCP connection (`Thread.ofVirtual()`,
Java 21). I deliberately ruled out gRPC, Netty, Protobuf, and JSON for this — using someone
else's RPC framework would have quietly taken the actual "network protocol design" work off my
plate, and that was one of the things I most wanted this project to prove I could do. Blocking
I/O on a virtual thread turned out to be far simpler to reason about correctly than an
NIO-selector event loop, while still scaling to hundreds of concurrent connections, which the
concurrency tests confirm.

---

## 4. How I Built It, Spec by Spec

I worked strictly in dependency order. Below is the real build log — what shipped in each spec,
what broke, and how I fixed it. Dates are commit dates.

### Spec 00 — Foundations & Scaffolding (2026-07-01)

The first commit, and the biggest single one at roughly 6,800 lines, but almost none of it is
broker logic — it's the skeleton everything else stands on. I set up the seven-module Gradle
Kotlin DSL build with the dependency graph above, wrote all fifteen spec files up front (so the
entire roadmap existed before a line of real logic did), wired up GitHub Actions CI to run
`build` and `test` on every push, added SLF4J + Logback structured logging across every module,
and wrote a `BrokerConfig` skeleton that reads `BROKER_ID` / `BROKER_HOST` / `BROKER_PORT` from
environment variables — externalizing configuration from the very first commit rather than
retrofitting it later, since I already knew I'd need per-environment config for Docker Compose.

I also set up my own development tooling around the spec-driven workflow at this stage —
templates and checklists for turning a spec into a plan, running the plan test-first, and a
dedicated review pass gated specifically on anything touching Raft or replication before I'd
mark a spec done. That structure is the reason specs 06 through 09 (the consensus-heavy middle
of the project) didn't spiral.

Acceptance was simple and mechanical: `./gradlew build` and `test` green, Spotless (Google Java
Format) enforcing consistent formatting, CI badge live in the README.

One process decision landed right after this spec closed: I added a git commit policy to my
project's constitution file stating that every commit lists human authorship only, with no
AI-co-author trailers of any kind — I wanted the commit history itself to read as my own
engineering log, not as a transcript of a tool session.

### Spec 01 — Wire Protocol & Network Layer (2026-07-02)

The first functional layer, about 1,700 lines, and everything else in the system rides on top of
it. I built the `protocol/` module: the `Frame` type, a `MessageType` enum covering all fifteen
message kinds the system would eventually need (publish, poll, commit-offset, metadata, plus the
broker-to-broker Raft messages — AppendEntries, RequestVote, Heartbeat — and a catch-all
ERROR_RESP), a `FrameEncoder`/`FrameDecoder` pair, a `MessageCodec` doing per-type
serialization/deserialization, request/response value types for every message, and a
`ProtocolException` that is never silently swallowed anywhere in the codebase.

On the broker side: a `ConnectionAcceptor` running a TCP accept loop that spawns one virtual
thread per incoming connection, behind a `RequestHandler` interface (a stub implementation for
now — the real logic arrives with later specs). On the client side: `BrokerConnection`, a single
TCP connection wrapper that matches responses back to requests using correlation IDs, since
multiple requests can be in flight concurrently over one socket.

Testing here set the pattern I kept for the rest of the project: round-trip codec tests for
every single message type, plus a property-based test (`FrameDecoderPropertyTest`) that throws
random byte sequences at the decoder and asserts one thing — it either parses successfully or
throws `ProtocolException`, and it never hangs and never crashes the server. Malformed input has
to become an `ERROR_RESP`, not a stuck connection or a downed process. I also wrote a
concurrency test running a hundred simultaneous virtual-thread connections against the acceptor
to make sure nothing about the connection-handling path assumed single-threaded access.

The first real environment problem showed up here too: my development machine only has JDK 25
installed system-wide, but this is a Java 21 project, and Gradle (plus Spotless) breaks outright
against a JDK 25 toolchain for a project pinned to 21. I resolved it by explicitly exporting
`JAVA_HOME` to a real JDK 21 install before every Gradle invocation — a small thing, but one that
would have quietly wasted a lot of time if I hadn't nailed it down in the first spec instead of
re-discovering it every session.

### Spec 02 — In-Memory Log & Single-Topic Publish/Consume (2026-07-21)

The first true end-to-end slice: a producer publishes N messages over the real wire protocol
built in spec 01, backed by a purely in-memory log (no disk yet), and a consumer polls and gets
back exactly those N messages, in order. This is the spec where "does the whole pipe actually
work" got answered for the first time — protocol codec, connection handling, and an actual log
abstraction all exercised together instead of in isolation.

There's a real gap in dates here (spec 01 closed 2026-07-02, spec 02 didn't close until
2026-07-21) — that stretch was almost entirely the planning-to-execution grind of getting the
in-memory log's semantics exactly right (offset assignment, what "polling past the end" should
do, how a topic with no messages yet should behave) before writing the thin-slice end-to-end test
that ties producer, wire protocol, and consumer together for the first time.

### Spec 03 — Durable Append-Only Log (2026-07-21)

Replaced the in-memory log with the real thing: rolling segment files named with a
twenty-digit zero-padded base offset (`{base-offset:020d}.log` / `.index`), a configurable
`FsyncPolicy` so every append can be followed by an fsync before being acknowledged as
committed, a sparse memory-mapped offset index (mapping offset → byte position at a configurable
stride rather than indexing every single record, trading a small amount of read-side linear scan
for a much smaller index file), crash-safe recovery on broker restart, and retention that deletes
whole old segments once they cross a size or time threshold.

I explicitly decided against log compaction here. Real Kafka supports using a topic as a
compacted key-value change log; this project doesn't, and I say so directly rather than
pretending it's a future feature — it's a large amount of additional complexity for a guarantee
(compacted state topics) that isn't part of my invariant list. Retention here is strictly by
size or age.

Test coverage at this layer is where I first leaned hard into intentionally breaking things:
`PartialWriteRecoveryTest` and `CrashRecoveryTest` simulate a process dying mid-append and verify
that restart recovery doesn't lose already-fsynced data and doesn't choke on a torn final record;
`SegmentRollTest` and `RetentionTest` verify the size/time-based rollover and deletion logic;
`IndexLookupTest` and `OffsetIndexTest` check the sparse index actually finds the right byte
offset for an arbitrary target offset, including offsets that fall between indexed entries.

### Spec 04 — Partitions & Consumer Groups, Static (2026-07-21)

Extended the single-log broker to support multiple partitions per topic with key-based routing
(same key always routes to the same partition, so per-key ordering is preserved), plus a
consumer-group abstraction with durable committed-offset storage and a static, non-rebalancing
partition assignment (dynamic rebalancing was deliberately deferred all the way to the stretch
spec 13, so it couldn't threaten anything in the correctness-critical middle of the project).

`KeyRoutingE2ETest`, `PartitionOrderingTest`, `ConsumerGroupOffsetDurabilityTest`, and
`IndependentGroupsTest` cover, respectively: that a key always lands on the same partition, that
ordering within a partition is preserved end-to-end through the real wire protocol, that
committed offsets survive a restart, and that two independent consumer groups reading the same
topic don't interfere with each other's progress.

### Spec 05 — Cluster Membership & Metadata (2026-07-22)

Static cluster membership: brokers form a cluster from Docker Compose configuration (a fixed
broker list, no gossip protocol — dynamic membership discovery isn't part of the correctness
story I'm trying to tell, so I cut it deliberately), a lightweight controller/metadata role
tracks partition-to-leader assignment, brokers detect peer failures via heartbeats, and clients
can discover cluster metadata and get redirected to the correct partition leader.

`ClusterConfigTest`, `HeartbeatMonitorTest`, `FailureDetectionTest`, and `MetadataE2ETest` cover
config parsing, heartbeat liveness tracking, failure detection timing, and the client-facing
metadata request/response round trip.

### Spec 06 — Raft Consensus Core, standalone module (2026-07-22)

This is the spec I was most careful with. A from-scratch, standalone Raft implementation in
`raft/`, completely independent of any broker-specific logic — it knows nothing about topics,
partitions, or the wire protocol; it only knows about a log of opaque entries, a set of peers,
and the Raft state machine (follower/candidate/leader, terms, log matching, commit index
advancement).

I wrote `LeaderElectionTest` for the basic election path, `TermFencingTest` to prove a stale-term
message from an old leader gets rejected outright, `LogReplicationTest` and `CommitAdvancerTest`
for the append/commit path, `PartitionCatchUpTest` for a follower that's fallen behind catching
back up, `PersistentStateTest` and `FileRaftLogStoreTest` for the on-disk persisted state a node
needs to survive its own restart without violating Raft's safety properties, and — the one I'm
most glad I wrote — `LogMatchingPropertyTest`, a property-based test that generates randomized
sequences of Raft operations and asserts the Log Matching Property holds (if two logs share an
entry at the same index and term, every entry before that index is identical in both logs). This
is the property the entire "no split-brain" story depends on, and it's exactly the kind of thing
that's easy to get subtly wrong in a way a handful of example-based tests won't catch.

This is also where my "any Raft/replication change needs a dedicated correctness pass before I'll
call it done" rule started actually paying for itself — it's much easier to find a term-fencing
bug in a five-file module with no network involved than three specs later when it's tangled up
with the wire protocol and Docker.

### Spec 07 — Replicated Partitions via Raft, Multi-Raft (2026-07-22)

Wired `raft/` into `broker/`: each partition now gets its own independent Raft group, with the
durable `PartitionLog` from spec 03 as the actual state machine Raft entries get applied to. A
publish now goes through `RaftNode.propose()`, which blocks until a majority of that partition's
Raft group has durably appended the entry — only then does the leader apply it to the log and
acknowledge the producer.

`SingleNodeRaftPublishTest` and `ReplicationE2ETest` prove the basic multi-broker replication
path works end-to-end over the real wire protocol (not just inside the `raft/` module's own unit
tests), `AckAfterMajorityTest` specifically proves the publish call doesn't return until a
majority has the entry (not just the leader), `CommittedReadOnlyTest` proves a consumer can never
poll data that hasn't reached the commit index yet (this is INV-5, directly), and
`BrokerRaftTransportTest` covers the broker-to-broker transport layer carrying Raft's own
AppendEntries/RequestVote/Heartbeat messages over the same wire protocol everything else uses.

### Spec 08 — Leader Failover & Epoch Fencing (2026-07-27)

Made the system actually survive a partition leader dying: detect the crash, have the surviving
Raft group elect a new leader with a strictly higher term, fence the old leader (any write or
AppendEntries it tries to send afterward gets rejected because its term is now stale), redirect
clients to the new leader transparently, and have consumers resume exactly from their last
committed offset with no gap and no duplicate.

`LeaderFailoverTest`, `FollowerKillTest`, `LeaderRestartRejoinTest`, and `RepeatedFailoverTest`
exercise the crash-and-recover cycle from different angles (leader dies, a follower dies, the old
leader comes back and has to rejoin without becoming leader again on stale state, and the same
partition survives several failovers in a row without accumulating any corruption).
`EpochFencingTest` is the one that directly attacks INV-4 — it constructs a scenario with two
brokers that both briefly believe they might be leader and asserts that they can never both
commit — and `ConsumerResumeAfterFailoverTest` /
`OffsetDurabilityAcrossFailoverTest` prove a consumer's committed offset and progress genuinely
survive the leader crash rather than silently resetting.

There's a nine-day gap between spec 07 closing (2026-07-22) and spec 08 closing (2026-07-27).
That was the failover and epoch-fencing logic itself — getting the redirect-on-stale-leader path
and the client-side retry/redirect behavior right took real iteration, since this is exactly the
kind of code where an off-by-one in term comparison, done carelessly, silently reintroduces
split-brain instead of loudly failing a test.

### Spec 09 — Idempotent Producer & Delivery Semantics (2026-07-27)

Added producer-id plus per-partition sequence numbers so a producer retry after a network error
(where the original request may or may not have actually landed) never creates a duplicate
record. `IdempotencyStore.check(producerId, seqNo)` compares an incoming sequence number against
the last *committed* sequence for that producer on that partition — tracked identically on every
replica, applied through the same Raft log everything else goes through, so a freshly elected
leader has correct dedup state instantly rather than needing to rebuild it. Exactly
`lastSeq + 1` is accepted as a fresh append; anything `<= lastSeq` is a duplicate and gets
answered with the *cached offset* from the original append (so a client that retried because it
lost the response, not because the write actually failed, gets back the same offset it would have
gotten the first time); anything higher than `lastSeq + 1` is a genuine gap — the client skipped
a sequence number, which is a client bug rather than a retry, and gets rejected with a distinct
`CODE_SEQUENCE_GAP` error instead of being silently accepted as if it were fine.

`IdempotentProduceTest` and `SequenceGapTest` cover the two branches directly,
`IdempotencyRecoveryTest` proves dedup state survives a broker restart, and `ChaosDedupTest` and
`NoOpEntryTest` push the same guarantee through leader-failover scenarios rather than just a
quiet single-broker path.

I paired this with explicit consumer offset commits on the read side, which is what makes the
overall delivery guarantee "effectively-once" rather than genuinely exactly-once: idempotent
producer plus at-least-once consumer. I did not build full cross-partition transactions —
that's explicitly out of scope, and I say so directly rather than letting "effectively-once"
imply more than it delivers.

### Spec 10 — Backpressure & Flow Control (2026-07-27)

Protected the broker against being overwhelmed by a fast producer or stalled by a slow consumer.
Each partition gets a bounded queue (`ArrayBlockingQueue`) for in-flight publishes; once it's
full, a producer's own virtual thread simply blocks on the queue — which, because it's a virtual
thread doing blocking I/O rather than an OS thread, costs almost nothing extra and needed no
separate throttling mechanism layered on top. The client backs off exponentially on
`BROKER_BUSY` responses rather than hammering a saturated partition.

`BackpressureControllerTest`, `BoundedInFlightTest`, and `BrokerBusyTest` cover the admission-gate
logic directly and fast; `SlowConsumerIsolationTest` proves a single stalled consumer group
can't starve other consumers or crash the broker; `BackpressureRecoveryTest` proves the system
actually drains back to normal once the pressure is removed rather than staying wedged.

The one I'm proudest of here is `HeapSoakTest` (tagged `@Tag("soak")`, deliberately excluded from
the normal fast `./gradlew test` run and run separately via `./gradlew :broker:soakTest`): thirty-
two virtual-thread producers hammer a single broker with no consumer draining the partition at
all, for sixty seconds, sampling used heap every two seconds after a forced GC. The measured
result — 7MB max used heap in the first half of the run, 7MB in the second half, flat — is direct
evidence that the admission gate genuinely bounds `RaftNode.pendingProposals` instead of just
slowing growth down. Recorded in `docs/results.md`.

### Spec 11 — Chaos / Fault-Injection Harness & Linearizability Checker (2026-07-28)

This is the project's actual thesis, and the spec I was most nervous about, because it's the one
that either proves everything before it or exposes it. I built a `FaultInjector` in `chaos/` that
drives four real fault types against a live Docker Compose cluster via Testcontainers: killing
the current partition leader outright (`killLeader`), restarting a container, partitioning the
network between two brokers with real iptables DROP rules (`partitionNetwork`/`healNetwork`), and
injecting real, sustained disk slowness on a specific broker via a container recreate with a
`BROKER_FSYNC_DELAY_MS` environment variable (I initially reached for `tc` here since it's the
standard way to shape a Docker network path, but `tc` only shapes network queues, not actual disk
I/O — so a slow-disk fault needs a different mechanism entirely, which is why `slowDisk`
recreates the container with a real injected fsync delay instead).

A `ChaosOrchestrator` runs producers and consumers concurrently against the cluster while
`HistoryRecorder` captures every single producer and consumer event
(`(producerId, seqNo, offset, timestamp, outcome)`), and four checkers run against the recorded
history afterward: `LossChecker` proves every acknowledged publish shows up in some consumer's
received set (INV-1, directly), `DuplicationChecker` proves no producer message occupies two
offsets (INV-3), `LinearizabilityChecker` proves a single valid total order explains everything
every consumer observed (INV-2), and `DivergenceChecker` connects to every replica directly and
proves their committed logs agree (INV-5).

**Getting the harness to actually run against a real Docker daemon for the first time surfaced a
pile of real bugs** that had been quietly hiding behind a green compile the whole time:

- `docker-compose.yml` never actually registered the harness's default "chaos" topic, so it was
  silently falling back to a single unreplicated partition — which would have made the entire
  chaos run meaningless without ever failing loudly.
- The pinned Testcontainers version couldn't talk to the current Docker Desktop API at all;
  upgrading it meant dropping APIs it had removed and reworking how the harness resolves
  containers (by Compose service label instead of the old ambassador-container mechanism, which
  doesn't play well with Compose V2's naming).
- `FaultInjector.slowDisk` shelled out to `docker compose` without pinning a project name, so its
  `--force-recreate` could have silently hit the wrong Compose project instead of the actual
  cluster under test.
- Brokers only ever advertised their container-internal hostname, so nothing running on the
  Docker host — the chaos harness itself, and later the benchmark module — could route to a
  specific partition leader from outside the Compose network. I split this into a client-facing
  `ADVERTISED_LIST` separate from the `BROKER_LIST` used for internal Raft/heartbeat peering.
- `ClusterClient.refresh()` used one global lock shared across every producer/consumer thread in
  a run; under a real failover this serialized redundant metadata refreshes badly enough to blow
  through retry budgets almost immediately. I replaced it with a coalescing refresh so concurrent
  callers share one in-flight refresh instead of queuing behind separate ones.
- `BrokerConnection` had no socket read or connect timeout at all, so a thread stuck reading from
  a container that had just been SIGKILLed could hang indefinitely, no matter how generous the
  retry budget above it was. I added a bounded default timeout and made the redirect path evict a
  connection on `IOException` instead of ever handing back a socket that might have a stale
  buffered response sitting in it.
- The orchestrator's redirect budget was sized for one producer and one consumer failing over
  together, not eleven threads sharing a single `ClusterClient` through a real container
  kill-and-rejoin cycle, so I widened it.
- The disk-slowness scenario's default load was heavy enough against a sustained slow-disk fault
  to blow past the backpressure limits from spec 10 and the checker's own drain window before the
  system could catch up — I capped that specific test's load rather than touching the shared
  chaos defaults everything else depends on.

Two of those fixes — the `ClusterClient` concurrency fix and the `BrokerConnection` timeout —
went through the same dedicated correctness review I required for spec 06, since both sit
directly in the leader-redirect path that INV-4 depends on, even though neither one lives inside
`raft/` itself.

With all of that fixed, all four chaos scenarios pass: the headline crash test, the disk-slowness
test, the network-partition test, and a randomized-schedule test that runs across ten different
random seeds rather than one fixed fault schedule. The result recorded in `docs/results.md` is a
*validated-scale* run — 4 injected leader crashes across 999 messages, 0 loss, 0 duplication,
linearizability PASS, divergence PASS — not yet the full headline scale of ~1,000 crashes over
~10M messages, which remains a follow-up run rather than something spec 11's closure required.
I want to be precise about that distinction rather than rounding it up: the mechanism is proven
correct at the scale I actually ran it, and running it longer is a matter of wall-clock time on a
laptop, not open correctness questions.

### Spec 12 — Benchmarks: JMH + Load Generator (2026-07-28)

Built JMH benchmarks and a standalone load generator (`bench/`) measuring sustained throughput
and publish-to-commit latency at different replication factors and message sizes, with a
`BenchResultsWriter` that merges the JMH JSON output straight into `docs/results.md` rather than
me hand-transcribing numbers (which is exactly the kind of place a number quietly goes stale).

Measured, on a single laptop running a 3-broker Compose cluster with fsync on the commit path (so
these numbers are not meant to be compared against a tuned multi-host production deployment):
1KB payloads sustain 1,668 msgs/sec at RF=1 and 549 msgs/sec at RF=3 — a 67.1% throughput cost
for going from one copy to three, which is the real, measured price of the durability guarantee
this whole project exists to make, not an estimate. Publish-to-commit latency at RF=3 comes in at
p50 7.86ms, p99 61.28ms, p999 63.09ms.

These numbers are well short of the ~200,000 msgs/sec and ~8ms p99 targets I wrote down in the
very first planning document, and I'm not going to pretend otherwise. The honest read: those
targets were written assuming a proper multi-host deployment; what I actually measured is three
broker containers and a benchmark client all competing for the same laptop's CPU, disk, and
network loopback, with fsync literally on the critical path for every single commit. I'd rather
report the real number with that context than quietly move the goalposts. The headline claim of
this project was never the throughput row — it's the correctness row above it, which chaos
testing actually proved rather than merely asserted.

### Spec 13 — Dynamic Consumer-Group Rebalancing, stretch (2026-07-28)

A stretch spec, explicitly gated on specs 11 and 12 being fully done first, precisely so its
scheduling edge cases could never threaten the correctness centerpiece. Added a
`GroupCoordinator` that tracks group membership and triggers a rebalance on a `JOIN_GROUP`,
`LEAVE_GROUP`, or a missed `GROUP_HEARTBEAT` past a configurable session timeout; a
`RangeAssignor` that recomputes a contiguous partition range per member from current membership;
and three new wire messages (`JOIN_GROUP`, `GROUP_HEARTBEAT`, `LEAVE_GROUP`) for consumers to
coordinate over.

The one design detail I want to call out: a consumer mid-rebalance keeps consuming its *old*
assignment right up until it actually receives its new one — there's no stop-the-world barrier.
At most that means a consumer processes a handful of extra records from a partition it's about to
give up. That's safe not because rebalancing is perfectly synchronized (it isn't) but because
offset commits are idempotent per (group, topic, partition) — reprocessing a few already-seen
records at a rebalance boundary is exactly the same at-least-once behavior the system already
guarantees everywhere else, not a new edge case introduced by rebalancing.

`GroupCoordinatorTest` and `RangeAssignorTest` cover the coordinator logic and assignment
computation directly; `DynamicRebalanceE2ETest` runs an actual join/leave cycle over the real
wire protocol and confirms every partition still gets consumed by exactly one live member
throughout.

### Spec 14 — Demo & Polish (2026-07-28)

The last spec: get the project ready for someone else to actually look at. Rewrote the README
with the architecture diagram and the headline results table up top, wrote `scripts/demo.sh` — a
single command that brings the three-broker Compose cluster up, waits for every broker to report
joining the cluster, publishes load across a topic's three partitions, SIGKILLs one broker
partway through the run, lets consumers finish draining, and then verifies zero loss, zero
duplication, and gap-free offsets before tearing the cluster back down and exiting with the
verdict's own exit code — and made sure every Gradle target the project has is actually
documented in the README's target table.

Two infrastructure fixes landed right alongside this spec, both surfaced by actually running the
Docker-based integration job rather than just reading the compose file: the broker's Docker image
was building with the base image's bundled Gradle instead of the project's own pinned Gradle
wrapper version, which silently broke the image build every time the integration job ran it — the
fix was to make the Dockerfile invoke `./gradlew` instead of assuming the base image's Gradle was
close enough. And the CI job's default Testcontainers wait-strategy timeout (60 seconds) turned
out to be shorter than a cold three-image build plus full "wait for every broker to log 'joined
cluster'" cycle takes on a fresh GitHub Actions runner with no build cache and no pulled images —
comfortably over a minute on a clean runner, which is what was actually behind the
`RetryCountExceededException` the integration job kept reporting. I gave the compose cluster a
longer, more realistic startup budget instead of treating the timeout as fixed.

I have not yet run `scripts/demo.sh` end-to-end against a live Docker daemon in the same
environment I wrote it in (that machine doesn't have a Docker daemon available), so I've been
careful not to claim a result I haven't actually observed — `docs/results.md`'s demo row is
explicitly marked "not yet run" rather than backfilled with an invented number, pending running it
for real with Docker available.

---

## 5. How I Tested All of This

Testing wasn't a phase bolted on at the end — it was test-first for essentially every spec, and
the shape of the test suite mirrors the module boundaries deliberately:

- **`protocol/`** — round-trip codec tests per message type, plus a property test that throws
  random bytes at the frame decoder and asserts it never hangs or crashes, only parses or throws
  a typed exception.
- **`log/`** — segment rollover, sparse index lookups, retention, and — the ones that actually
  matter — crash-recovery and partial-write-recovery tests that simulate a process dying
  mid-append and assert restart recovery doesn't lose fsynced data or choke on a torn record.
- **`raft/`** — leader election, term fencing, log replication, commit advancement, follower
  catch-up, persisted state across restart, and one property-based test
  (`LogMatchingPropertyTest`) that generates randomized operation sequences and checks Raft's Log
  Matching Property directly, rather than relying on a handful of hand-picked examples to catch
  a subtle consensus bug.
- **`client/`** — connection handling, retry-on-busy behavior, partition routing, and static
  assignment.
- **`broker/`** (by far the largest suite — over sixty test classes) — every end-to-end scenario
  the specs above describe: replication, failover, repeated failover, epoch fencing, idempotent
  producer dedup and sequence-gap rejection, backpressure and slow-consumer isolation, the
  sixty-second heap soak, dynamic rebalancing, and the static-config integration path.
- **`chaos/`** — unit tests for each individual checker (loss, duplication, linearizability,
  divergence) against hand-constructed histories, plus the actual `chaosTest` source set: four
  scenario classes (headline crash, disk slowness, network partition, randomized schedule across
  ten seeds) that only run against a live Docker daemon and are deliberately kept out of the fast
  `./gradlew test` / CI path, since they're slow and Docker-dependent by nature.
- **`bench/`** — config parsing and results-writer tests; the actual JMH runs themselves are, like
  the chaos suite, deliberately excluded from CI.

CI runs `build`, `test`, and the Docker-based `:broker:integrationTest` job on every push to
main. Chaos runs, JMH benchmarks, and the demo script are all Docker- and wall-clock-heavy by
nature and are deliberately not part of CI — I run them locally and record the results by hand
(well — by `BenchResultsWriter` and the chaos harness's own results-reporting step, not literally
by hand-typing numbers) into `docs/results.md`.

---

## 6. Results — What I Actually Measured

This is the section I care most about being precise in. Everything here is a measured result, not
a target restated as if it were a result.

**Correctness (the headline claim):**

| | Measured |
|---|---|
| Committed-message loss | 0, across 999 messages and 4 injected leader crashes |
| Duplication | 0 |
| Linearizability | PASS |
| Replica divergence | PASS |
| Split-brain events under a network partition | 0 |

**Performance (single laptop, 3-broker Compose cluster, fsync on the commit path, 1KB payloads):**

| Metric | RF=1 | RF=3 |
|---|---|---|
| Throughput | 1,668 msgs/s | 549 msgs/s |
| Publish→commit p50 | — | 7.86 ms |
| Publish→commit p99 | — | 61.28 ms |
| Publish→commit p999 | — | 63.09 ms |

RF=3 costs 67.1% of RF=1's throughput — that's the real, measured price of replicating to a
majority of three instead of writing to one, on this hardware.

**Backpressure heap soak** (single broker, no consumer draining, 32 virtual-thread producers,
60 seconds): 7MB max used heap in the first half of the run, 7MB in the second half — flat,
confirming the admission gate bounds in-flight proposals rather than merely slowing their growth.

**What's still outstanding, and I'm saying so directly rather than rounding it up:** the full
headline chaos scale — roughly 1,000 induced crashes across roughly ten million messages — has
not been run yet; what's recorded above is a validated run at a smaller, laptop-friendly scale
that exercises every checker and every fault type. Running the full-scale version is a follow-up
matter of wall-clock time, not an open correctness question, since the same harness and the same
checkers are what would run it. Similarly, `scripts/demo.sh` has been verified by unit test and a
shell syntax check but not yet run end-to-end against a live Docker daemon, since my primary
development environment doesn't have one available — that's the next thing I'll do the moment
I'm at a machine with Docker running.

---

## 7. What I'd Do Differently

A few honest retrospective notes, since I think this is more useful than pretending the process
was smooth:

- **I underestimated how much of "chaos testing" is actually infrastructure debugging.** The
  single biggest fix commit in this project's history isn't a Raft bug at all — it's the pile of
  Testcontainers version incompatibilities, Docker networking assumptions, and a global lock that
  only showed up once I ran the harness against a real Docker daemon for the first time. Writing
  the chaos harness and getting a chaos harness to actually complete a run turned out to be two
  very different amounts of work, and I'd budget for that gap much earlier next time.
- **Writing the spec files up front, before any code, was worth it.** Being able to point at
  "spec 09 depends on spec 08" and just trust that dependency instead of re-verifying it made the
  middle third of this project (the consensus-heavy specs) much less stressful than it could have
  been.
- **The property-based tests (`FrameDecoderPropertyTest`, `LogMatchingPropertyTest`) found more
  real bugs per hour of effort than any of my example-based tests did.** I'd reach for that tool
  earlier and more often in the next project instead of treating it as a late addition to a
  handful of critical modules.
- **I'd be more skeptical, earlier, of numbers I write down before I've measured anything.** The
  ~200K msgs/sec target in the original planning doc was a reasonable guess for a tuned
  multi-host deployment, but I wrote it down before I'd run a single benchmark, and it ended up
  roughly 350x the number I actually measured on one laptop under real fsync pressure. The gap
  itself isn't embarrassing — durability has a real, measurable cost, and I measured it instead of
  hiding it — but I'd frame a "target" as a hypothesis to test, not a number to defend, the next
  time around.

---

## 8. Where Everything Lives

- `distributed-message-broker.md` — the original master design document (all three parts).
- `planning/` — the expanded requirements, architecture, system design, storage schema, and API
  design docs.
- `specs/00` through `specs/14` — the fifteen specs, each with frontmatter, acceptance criteria,
  and an explicit out-of-scope section, built strictly in order.
- `docs/architecture.md` — the interview-facing reference: every locked decision, the wire
  format table, and a set of interview Q&A I wrote for myself covering the questions I expected
  to actually get asked.
- `docs/results.md` — the single source of truth for every measured number in this project; it's
  meant to only ever contain numbers I've actually observed, never placeholders passed off as
  results.
- `MANUAL_TESTING.md` — step-by-step manual verification instructions for anyone (including
  future me) who wants to stand the cluster up and poke at it by hand instead of just trusting the
  automated suite.
- `README.md` — the short version of all of this, for anyone who just wants to clone the repo and
  run one command.
