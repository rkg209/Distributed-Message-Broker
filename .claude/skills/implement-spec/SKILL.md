---
name: implement-spec
description: Implement a spec against its plan, test-first. Takes the spec number as argument. Use only after /spec-plan has been reviewed and approved.
---

Implement spec `$ARGUMENTS` (e.g. `/implement-spec 03` implements `specs/03-durable-log.md`).

## Pre-flight checks (do these first, stop if any fail)
1. Read `specs/$ARGUMENTS-*.md` — confirm `status: todo` (not already done or in progress).
2. Check that all `depends_on` specs have `status: done`. If not, stop and tell the user.
3. Confirm a plan was produced by `/spec-plan` in this session or previously reviewed.

## Implementation workflow
1. **Test files first** — create the test class(es) from the test plan before any production code.
2. **Interfaces / data types** — implement the interfaces and value types identified in the plan.
3. **Production code** — implement each class in the order specified in the plan.
4. **Wire up** — integrate into the broker/client startup path if required by the spec.
5. **Run tests** — `./gradlew test` must be GREEN before marking the spec done.
6. **Format** — `./gradlew spotlessApply`.

## Constraints (never violate these)
- Java 21 only. Virtual threads for connection handling.
- No JSON, no Protobuf, no gRPC, no Netty in production modules.
- No silent failure: propagate durability/replication errors; never swallow them.
- Keep modules separated — do not add cross-module dependencies not in the module map.
- All configuration externalized (never hardcoded).

## When done
1. Update the spec frontmatter: `status: done`.
2. Update `CLAUDE.md` "Current spec in progress" to the next spec.
3. If the spec has measurable outputs (chaos or benchmark results), update `docs/results.md`.
4. For any consensus/replication spec, run `/raft-review` before declaring done.
5. Commit with message: `feat(spec-XX): <spec title>`.
