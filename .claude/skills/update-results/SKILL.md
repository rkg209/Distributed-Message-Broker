---
name: update-results
description: Pull the latest chaos and benchmark numbers into docs/results.md and the README headline table.
---

Update the results documentation with the latest measured numbers.

## Steps
1. Read the current `docs/results.md`.
2. Run `/chaos-test 1000` if the chaos results are stale (older than the last code change
   in `raft/` or `broker/`). Otherwise use the existing numbers.
3. Run `/bench` if the benchmark results are stale (older than the last change in
   `log/`, `protocol/`, or `broker/`). Otherwise use the existing numbers.
4. Update `docs/results.md` with:
   - Chaos results: date, crash count, message count, loss=0, dupe=0, linearizability=PASS
   - Benchmark results: throughput table (RF=1 vs RF=3), latency table (p50/p99)
   - Cost-of-durability delta
5. Update the headline table in `README.md` to match `docs/results.md`.

## When to run
- After Spec 11 (first chaos results).
- After Spec 12 (first benchmark results).
- Whenever preparing to update resume numbers.
- Before tagging a release or creating a GitHub PR for a major spec.
