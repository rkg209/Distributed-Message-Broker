---
name: spec-plan
description: Turn a spec in /specs into a reviewed implementation plan before any code is written. Use whenever starting a new spec.
---

Read the spec file at `specs/$ARGUMENTS.md` (e.g. `/spec-plan 03` reads `specs/03-durable-log.md`).

Produce an implementation plan with these sections:

## 1. Modules touched
List every module (`protocol/`, `log/`, `raft/`, `broker/`, `client/`, `chaos/`, `bench/`)
this spec touches and what changes in each.

## 2. New interfaces / data structures
List every new Java interface, abstract class, or key data structure to introduce.
For each, write the method signatures (no implementation).

## 3. Implementation order
Ordered list of steps: what to implement first (usually interfaces and data structures),
then implementations, then integration. Each step should be a unit of ~2 hours.

## 4. Test plan
For each acceptance criterion in the spec:
- What JUnit 5 test class covers it
- What setup is required (in-process vs Testcontainers)
- What the test asserts

## 5. Invariants preserved
For each of INV-1 through INV-5, state whether this spec touches it and, if so, how
the implementation preserves it.

## 6. Risks & open questions
Top 3 risks: what could go wrong, what is uncertain, what dependencies are unclear.

---

Rules:
- Do NOT write implementation code. This plan is for review, not execution.
- If the spec depends on another spec that is not yet `status: done`, stop and warn the user.
- After producing the plan, ask the user to review it before proceeding with `/implement-spec`.
- For any spec touching consensus or replication (raft/, replication in broker/), recommend
  running `/raft-review` after implementation.
