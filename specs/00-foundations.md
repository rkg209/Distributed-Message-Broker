---
id: "00"
title: Foundations & Scaffolding
status: done
phase: 1
depends_on: []
---

# Spec 00 · Foundations & Scaffolding

## What
Initialize the Gradle multi-module project, configure CI, set up logging and
configuration loading, and create empty module skeletons so every subsequent
spec has a stable foundation to build on.

## Why
All later specs depend on a working build, a module structure that enforces
separation of concerns, and CI that catches regressions on every push. Getting
this right early prevents wasted effort reshaping the project later.

## Modules to create (all empty/skeleton initially)
- `protocol/` — wire protocol & codec
- `log/`      — append-only log & index
- `raft/`     — Raft consensus (standalone)
- `broker/`   — broker server (depends on protocol, log, raft)
- `client/`   — producer/consumer client library (depends on protocol)
- `chaos/`    — fault-injection harness (depends on client)
- `bench/`    — JMH benchmarks & load generator (depends on client)

## Acceptance criteria (definition of done)
1. `./gradlew build` is GREEN — all modules compile with zero errors.
2. `./gradlew test` is GREEN — empty test suites pass.
3. `./gradlew spotlessApply` runs without error and enforces Google Java Format.
4. GitHub Actions CI runs `build` + `test` on every push to `main`; badge is
   visible in README.
5. Each module has a `src/main/java` and `src/test/java` directory with a
   placeholder package (`io.minikafka.<module>`).
6. `BrokerConfig` skeleton exists in `broker/` reading at minimum:
   `BROKER_ID`, `BROKER_HOST`, `BROKER_PORT` from environment/config file.
7. Structured logging (SLF4J + Logback) is wired into all modules.
8. `docs/architecture.md` exists (content from planning docs).
9. `docs/results.md` exists with the results table template (empty numbers).

## Out of scope
- Any actual protocol, log, Raft, or broker logic.
- Docker or Testcontainers setup (those come with later specs).
- Any performance or correctness testing.

## Key files expected after this spec
```
build.gradle.kts                    (root, multi-module)
settings.gradle.kts
protocol/build.gradle.kts
log/build.gradle.kts
raft/build.gradle.kts
broker/build.gradle.kts
client/build.gradle.kts
chaos/build.gradle.kts
bench/build.gradle.kts
.github/workflows/ci.yml
docker/                             (empty, placeholder)
docs/architecture.md
docs/results.md
```
