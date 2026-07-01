---
id: "14"
title: Demo & Polish (Deliverable)
status: stretch
phase: 7
depends_on: ["12"]
requirements: [DR-7, DR-8, DR-9, NFR-20, NFR-21, NFR-22]
---

# Spec 14 · Demo & Polish (Deliverable)

## What
Prepare the project for public consumption: update README with the architecture
diagram and headline results table, add a one-command "kill a broker, zero loss"
demo script, and ensure CI is green with all Gradle targets documented.

## Why
The artifact a reviewer opens is the README and the results table. This spec
makes the project's correctness claim legible and reproducible to a stranger.

## What to build

### README.md
- Architecture diagram (ASCII or Mermaid, matching planning/02-architecture.md)
- Headline results table (from `docs/results.md`): throughput, latency, chaos pass/fail
- CI badge (GitHub Actions)
- One-command getting started:
  ```bash
  docker compose up
  # kill a broker mid-publish:
  docker compose stop broker-2
  # consumer receives every message in order
  ```
- All Gradle targets documented: `build`, `test`, `spotlessApply`, `chaosTest`,
  `jmh`, Docker Compose `up`/`down`

### Demo script (`scripts/demo.sh`)
- Starts the 3-broker cluster
- Starts a producer writing 100K messages
- After 10K messages, kills broker-2
- Waits for failover
- Verifies consumer received all 100K messages with zero gaps
- Prints PASS or FAIL

### docs/architecture.md (update from planning doc)
- All Part B design decisions from `distributed-message-broker.md` in final form
- Module diagram
- Wire format table
- Interview talking points in a structured Q&A format

## Acceptance criteria
1. A stranger can clone the repo, run `docker compose up`, run `scripts/demo.sh`,
   and observe PASS with no manual configuration.
2. README headline results table matches `docs/results.md`.
3. CI badge is green on main branch.
4. All documented Gradle targets work.
5. `docs/architecture.md` covers all five invariants and all Part B design decisions.
