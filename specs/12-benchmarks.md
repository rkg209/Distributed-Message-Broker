---
id: "12"
title: Benchmarks (JMH + Load Generator)
status: todo
phase: 6
depends_on: ["11"]
requirements: [FR-55, FR-56, FR-57, NFR-7, NFR-8, NFR-9]
---

# Spec 12 · Benchmarks (JMH + Load Generator)

## What
Implement JMH benchmarks and a standalone load generator that measure sustained
throughput and publish-to-commit latency at varying replication factors and
message sizes. Write the results table to `docs/results.md`.

## Why
The resume cites specific numbers (~200K msgs/sec @ RF=3, p99 ≤8ms). Those numbers
must be measured, not estimated. The benchmark suite also quantifies the cost-of-
durability (RF=1 vs RF=3 delta) which is a concrete interview talking point.

## What to build (in `bench/`)

### JMH benchmarks
- `PublishThroughputBenchmark` — measures sustained msgs/sec at 1KB payloads
  for RF=1 and RF=3 (configurable via JMH params)
- `PublishLatencyBenchmark` — measures publish-to-commit p50/p99/p999 latency
  at RF=3 under the target throughput

### Load generator
- `LoadGenerator` — standalone (non-JMH) multi-threaded producer + consumer that
  can sustain load for a configurable duration (default 5 minutes); used by the
  chaos harness for long-running tests
- Configurable: `--messages`, `--payload-size`, `--threads`, `--rf`, `--duration`

### Results writer
- `ResultsWriter` — parses JMH JSON output; appends formatted markdown table to
  `docs/results.md`; includes RF=1 vs RF=3 delta row

## docs/results.md table format
```markdown
## Throughput (1KB payload, 3-broker cluster)
| Metric          | RF=1    | RF=3    | RF=3 overhead |
|-----------------|---------|---------|---------------|
| Throughput      | X K/s   | Y K/s   | Z%            |

## Latency (RF=3, 1KB payload)
| Percentile | Latency |
|------------|---------|
| p50        | X ms    |
| p99        | Y ms    |
| p999       | Z ms    |
```

## Acceptance criteria
1. `./gradlew jmh` runs against the Docker Compose 3-broker cluster and completes
   without error.
2. `docs/results.md` is updated with the measured throughput and latency tables.
3. Throughput target ≥ 200K msgs/sec at RF=3 (replace with actual; report whatever is measured).
4. p99 latency target ≤ 8ms at RF=3 under target throughput (report actual).
5. RF=1 vs RF=3 cost-of-durability delta is quantified.
6. `LoadGenerator` can sustain a 5-minute run without broker crash or OOM.

## Out of scope
- Automated regression alerting (compare to previous run).
- Flamegraph / profiling integration.
