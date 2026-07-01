---
name: bench
description: Run JMH benchmarks, parse the output, and write the throughput/latency table to docs/results.md. Delegates to the benchmark-analyst subagent.
---

Run the JMH benchmark suite against the running Docker Compose cluster and update the results table.

## Steps
1. Verify the cluster is running: `docker compose -f docker/docker-compose.yml ps`.
   Start it if needed.

2. Run benchmarks via the `benchmark-analyst` subagent:
   ```
   ./gradlew jmh
   ```
   The subagent parses verbose JMH output and returns clean tables.

3. The subagent returns:
   - Throughput table (RF=1 vs RF=3 at 1KB payload)
   - Latency table (p50, p99, p999 at RF=3)
   - Cost-of-durability delta (RF=3 overhead %)

4. Update `docs/results.md` with the new numbers, replacing the previous benchmark section.

5. Report the results inline in the conversation (summary table only).

## When to run
- After completing Spec 12 for the first time (establish the baseline).
- After any performance-relevant change (log, protocol, backpressure).
- Before updating resume numbers.
