# Progress Report — Distributed Message Broker (mini-Kafka)

> Sequential record of how this project evolved: from a single planning document, through
> a full planning folder, to a spec-driven Gradle multi-module codebase with its first two
> specs implemented and tested. Last updated: **2026-07-02**.

---

## Project at a Glance

| | |
|---|---|
| **Goal** | A from-scratch, Raft-replicated, durable message broker in Java 21 — a "mini-Kafka" |
| **Headline guarantee** | Zero committed-message loss and zero duplication under ~1,000 induced crashes, proven by a chaos harness |
| **Methodology** | Spec-Driven Development (SDD): spec → plan → test-first implementation → measured results |
| **Current stage** | Specs 00 and 01 done; **Spec 02 (In-Memory Log & Thin Slice) in progress** |
| **Overall completion** | 2 of 13 core specs done (plus 2 stretch specs), foundation and network layer fully in place |

---

## Stage 0 — Master Design Document (2026-06-30)

The project began as a single source-of-truth document: **`distributed-message-broker.md`** (~28 KB).
It defines the entire project before any code exists, in three parts:

- **Part A — Resume & interview layer.** The measurable claims the project must earn:
  0 loss / 0 duplication across ~1,000 leader crashes over ~10M messages; ~200K msgs/sec
  at 1 KB payloads with replication factor 3; p99 publish→commit latency ~8 ms; and the
  measured throughput cost of RF=3 vs RF=1.
- **Part B — Locked design decisions** (see "Decisions" below) so they are never re-litigated mid-build.
- **Part C — Claude Code setup and the ordered backlog of specs** to be built in dependency order.

## Stage 1 — Planning Folder (2026-06-30)

The master document was expanded into a full **`planning/`** folder of eight artifacts:

| File | Contents |
|------|----------|
| `01-project-summary.md` | Executive summary: what it is, who it's for, what it proves |
| `01-requirements.md` | Functional requirements (FR-1…) and constraints (CON-…) |
| `02-architecture.md` | Module decomposition and component architecture |
| `03-system-design.md` | Detailed system design (replication, failover, delivery semantics) |
| `04-database-design.md` + `04-database-schema.sql` | Storage/log layout design |
| `05-api-design.md` + `05-openapi.yaml` | API surface / wire-level design |

Key framing decision made here: this is a **"Kafka-style broker with Raft-replicated
partitions"** — explicitly *not* a re-implementation of Apache Kafka's ISR/controller
architecture. Correctness (not features) is the deliverable.

## Stage 2 — Spec 00: Foundations & Scaffolding (done 2026-07-01, commit `2b89c31`)

First real commit (~6,800 lines). Turned the planning material into a working, CI-backed skeleton:

**What was built**
- **Gradle multi-module project** (Kotlin DSL, Java 21) with seven modules and an enforced
  dependency direction: `protocol/`, `log/`, `raft/` standalone; `broker/` depends on those
  three; `client/` on `protocol/`; `chaos/` and `bench/` on `client/`.
- **15 spec files** (`specs/00` – `specs/14`) distilled from the planning docs, each with
  frontmatter (`id`, `status`, `phase`, `depends_on`), acceptance criteria, and out-of-scope
  sections — the entire remaining roadmap, written up front in dependency order.
- **GitHub Actions CI** (`.github/workflows/ci.yml`) running build + test on every push.
- **Structured logging** (SLF4J + Logback) wired into all modules; smoke tests per module.
- **`BrokerConfig`** skeleton reading `BROKER_ID` / `BROKER_HOST` / `BROKER_PORT` from the environment.
- **`docs/architecture.md`** (from planning docs) and **`docs/results.md`** — the results
  template with PENDING placeholders and the spec-completion tracker.
- **Claude Code tooling**: `CLAUDE.md` constitution, four subagents (`distributed-systems-reviewer`,
  `chaos-runner`, `benchmark-analyst`, `test-author`) and seven skills (`/spec-plan`,
  `/implement-spec`, `/raft-review`, `/chaos-test`, `/bench`, `/spec-new`, `/update-results`)
  encoding the SDD workflow into the development environment itself.

**Acceptance met:** `./gradlew build` and `test` green, Spotless (Google Java Format) working, CI badge in README.

**Process decision (2026-07-01, commit `af3807d`):** git commit policy added to `CLAUDE.md` —
commit messages list **human authors only**; no AI co-author trailers.

## Stage 3 — Spec 01: Wire Protocol & Network Layer (done 2026-07-02, commit `5803a95`)

First functional layer (~1,700 lines). Everything else in the project rides on this protocol.

**What was built**
- **Wire format:** `[4-byte length][1-byte type][payload]`, big-endian, max frame 16 MB
  (configurable via `ProtocolConfig`). Hand-written binary codec — deliberately no JSON,
  Protobuf, gRPC, or Netty (this is an explicit portfolio signal).
- **`protocol/` module:** `Frame`, `MessageType` (15 message types: PUBLISH, POLL,
  COMMIT_OFFSET, METADATA, plus broker↔broker APPEND_ENTRIES, REQUEST_VOTE, HEARTBEAT,
  and ERROR_RESP), `FrameEncoder`/`FrameDecoder`, `MessageCodec` (per-type serialization),
  request/response value types for every message, and `ProtocolException` (never swallowed).
- **`broker/` module:** `ConnectionAcceptor` — TCP accept loop spawning **one virtual thread
  per connection**; `RequestHandler` interface with a `StubRequestHandler` (real logic
  arrives with later specs).
- **`client/` module:** `BrokerConnection` — single TCP connection with correlation-ID-based
  request/response matching.

**Testing (test-first, per the SDD workflow)**
- Round-trip codec unit tests for every message type (`FrameCodecTest`, `MessageCodecTest`).
- **Property test** (`FrameDecoderPropertyTest`): any random byte sequence fed to the decoder
  either parses or throws `ProtocolException` — never hangs, never crashes the server
  (malformed frames get an ERROR_RESP).
- **Concurrency test** (`ConcurrencyTest`): 100 concurrent virtual-thread connections handled without error.

**Environment issue resolved along the way:** the development machine only has JDK 25
installed, which breaks Gradle/Spotless for this Java 21 project — resolved by exporting
`JAVA_HOME` to a JDK 21 installation before running Gradle.

## Stage 4 — Current: Spec 02 In Progress

**Spec 02 — In-Memory Log & Single-Topic Publish/Consume (Thin Slice)** is the active spec:
the first end-to-end slice where a producer publishes and a consumer polls through the real
protocol against an in-memory log. No implementation committed yet.

---

## Decisions Taken (locked, in chronological order)

1. **Replication = Raft, one Raft group per partition** (multi-Raft) — decided in the master doc, Stage 0.
2. **Custom length-prefixed binary protocol over TCP** with virtual-thread-per-connection; no Netty/gRPC/Protobuf/JSON — Stage 0, implemented in Stage 3.
3. **Delivery semantics = idempotent producer** (producer-id + per-partition sequence) plus at-least-once consumer with explicit offset commits — deliberately *not* full cross-partition transactions.
4. **Storage = append-only segments + fsync + sparse mmap offset index**; retention by size/time only; no log compaction.
5. **Static cluster membership** via Docker Compose config; lightweight controller for leadership tracking; consumer-group rebalancing deferred to a stretch spec.
6. **Framing:** describe as "Kafka-style broker with Raft-replicated partitions," never as an Apache Kafka clone — Stage 1.
7. **SDD workflow enforced by tooling:** specs built strictly in order, `/spec-plan` before `/implement-spec`, and any consensus/replication change must pass `/raft-review` before a spec is marked done — Stage 2.
8. **Human-only commit authorship** policy — Stage 2.
9. **Five invariants (INV-1 … INV-5)** — no committed-write loss, per-producer ordering, no retry duplicates, epoch-fenced single leader, consumers read only committed data — each must have a test that actively tries to break it.

## Issues Encountered & Resolved

| Issue | Stage | Resolution |
|-------|-------|------------|
| JDK mismatch: machine has only JDK 25; Gradle/Spotless fail on this Java 21 project | Spec 00/01 | Export `JAVA_HOME` pointing at JDK 21 before Gradle commands |
| Malformed/adversarial frames could crash or hang the server | Spec 01 | Decoder hardened + property-tested: parse or throw `ProtocolException`; server replies ERROR_RESP and stays up |

## Roadmap — What Remains

| Spec | Title | Status |
|------|-------|--------|
| 00 | Foundations & Scaffolding | ✅ done (2026-07-01) |
| 01 | Wire Protocol & Network Layer | ✅ done (2026-07-02) |
| 02 | In-Memory Log + Thin Slice | 🔨 **in progress** |
| 03 | Durable Append-Only Log | todo |
| 04 | Partitions & Consumer Groups | todo |
| 05 | Cluster Membership & Metadata | todo |
| 06 | Raft Consensus Core | todo |
| 07 | Replicated Partitions via Raft | todo |
| 08 | Leader Failover & Epoch Fencing | todo |
| 09 | Idempotent Producer | todo |
| 10 | Backpressure & Flow Control | todo |
| 11 | Chaos Harness & Linearizability | todo |
| 12 | Benchmarks | todo |
| 13 | Dynamic Rebalancing | stretch |
| 14 | Demo & Polish | stretch |

All chaos and benchmark numbers in `docs/results.md` are still PENDING — they become
measurable only after Specs 11 and 12, which is by design: the headline claims are earned,
not asserted.
