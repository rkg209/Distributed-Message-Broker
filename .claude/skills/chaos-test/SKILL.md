---
name: chaos-test
description: Run the fault-injection harness with a configurable crash count and summarize pass/fail. Delegates to the chaos-runner subagent to keep huge output out of context.
---

Run the chaos/fault-injection harness against the running Docker Compose cluster.

Usage: `/chaos-test <crash-count>` (e.g. `/chaos-test 100` for a quick run, `/chaos-test 1000` for the headline test)

Default crash count if no argument provided: `100`.

## Steps
1. Verify `docker compose -f docker/docker-compose.yml ps` shows all 3 brokers running.
   If not, start the cluster first with `docker compose -f docker/docker-compose.yml up -d`
   and wait for all brokers to report healthy.

2. Run the chaos test via the `chaos-runner` subagent:
   ```
   ./gradlew chaosTest -Pcrashes=<crash-count>
   ```
   The subagent handles the raw output; it returns only the summary.

3. Parse the summary and report:
   - Total messages: sent / acked / received
   - Loss count (must be 0 for a passing run)
   - Duplication count (must be 0 for a passing run)
   - Network partitions injected
   - Linearizability: PASS / FAIL
   - Divergence check: PASS / FAIL
   - Overall: **PASS** (all 0s, all PASS) or **FAIL** (with details)

4. If PASS: update `docs/results.md` with the new results (crashes count, messages, date).
5. If FAIL: report the first failure event from the history with full context so it can be debugged.

## When to run
- After any change to `raft/` or broker replication logic.
- Before marking Spec 08, 09, 11 as done.
- On demand to verify the headline claim.
