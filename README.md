# mini-kafka

A from-scratch, Raft-replicated, durable message broker (mini-Kafka) in Java 21.

![CI](https://github.com/rkg209/Distributed-Message-Broker/actions/workflows/ci.yml/badge.svg)

## Headline results

| | Measured | Target |
|---|---|---|
| Committed-message loss | 0 (999 msgs, 4 injected leader crashes) | 0 |
| Duplication | 0 | 0 |
| Linearizability / replica divergence | PASS / PASS | PASS |
| Split-brain under network partition | 0 events | 0 |
| Throughput @ RF=3, 1KB | 549 msgs/s | 200,000 msgs/s |
| p99 publish→commit @ RF=3 | 61.28 ms | 8 ms |
| RF=3 vs RF=1 throughput cost | 67.1% | — |

The chaos row above is at *validated* scale, not the headline `-Pcrashes=1000` /
~10M-message run (that's a follow-up, not required for any spec's close-out — see
`docs/results.md`). Benchmarks run a 3-broker Compose cluster on a single laptop with
fsync on the commit path, so absolute throughput is not comparable to a tuned
multi-host deployment. **The headline claim of this project is the correctness
row, not the throughput row** — zero committed-message loss and zero duplication
across leader crashes, proven by the chaos harness below, not asserted.

Full numbers: [`docs/results.md`](docs/results.md). Full design rationale:
[`docs/architecture.md`](docs/architecture.md).

## Architecture

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

## Quick start — the 60-second demo

```bash
docker compose -f docker/docker-compose.yml up -d --build   # 3-broker cluster
./scripts/demo.sh                                            # 20K msgs, broker-2 killed mid-publish
./scripts/demo.sh --messages 100000                          # full spec-14 scale
```

`scripts/demo.sh` owns the whole loop: brings the cluster up, waits for all three
brokers to join, publishes load across `orders`'s 3 partitions, SIGKILLs `broker-2`
at 10% of the run, lets consumers drain, and verifies zero loss (INV-1), zero
duplication (INV-3), and gap-free offsets — then tears the cluster down (`--keep-up`
to skip that) and exits with the verdict's code.

To see it by hand instead: `docker compose stop broker-2` mid-run and watch the
consumer keep receiving messages in order — the partitions `broker-2` doesn't lead
are unaffected, and the partition it does lead fails over to a peer.

## Gradle targets

| Target | What it does | Needs Docker | Runs in CI |
|---|---|---|---|
| `./gradlew build` | Compiles everything, runs Spotless + unit tests | no | yes |
| `./gradlew test` | Unit tests only | no | yes |
| `./gradlew spotlessApply` | Auto-formats the codebase | no | — |
| `./gradlew :broker:integrationTest` | Cluster formation / failover integration tests | yes | yes |
| `./gradlew :broker:soakTest` | Backpressure heap-soak test (`@Tag("soak")`) | no | no |
| `./gradlew chaosTest -Pcrashes=N -Pmessages=N` | Fault-injection harness: leader kills, loss/dup/linearizability/divergence checks | yes | no |
| `./gradlew :bench:jmh -Prf=1\|3` | JMH throughput/latency benchmarks | yes | no |
| `./gradlew :bench:loadGen` | Standalone load generator against a running cluster | yes | no |
| `./gradlew :bench:writeResults` | Merges JMH JSON output into `docs/results.md` | no | no |
| `./gradlew :chaos:demo -Pmessages=N` | The failover demo runner (used by `scripts/demo.sh`) | yes | no |
| `docker compose -f docker/docker-compose.yml up` / `down -v` | Cluster lifecycle | yes | — |

CI (`.github/workflows/ci.yml`) runs `build test` and `:broker:integrationTest` on
every push/PR to `main`. Chaos, JMH, and the demo are Docker-heavy and deliberately
not in CI — run them locally.

## Guarantees

| Invariant | Test that tries to break it |
|---|---|
| INV-1: a committed write is never lost, even across leader crashes | `HeadlineCrashChaosTest`, `LossChecker` |
| INV-2: per-partition ordering is preserved for a given producer | `LinearizabilityChecker` |
| INV-3: no duplicate delivery from producer retries | `DuplicationChecker` |
| INV-4: two leaders for the same partition can never both commit | `EpochFencingTest` |
| INV-5: consumers only read committed (majority-replicated) data | `DivergenceChecker` |

## Module map

```
broker/ → raft/, log/, protocol/
client/ → protocol/
chaos/  → client/
bench/  → client/
raft/   → (standalone, no broker deps)
log/    → (standalone, no broker deps)
protocol/ → (standalone, value types + codec only)
```

See also: [`docs/architecture.md`](docs/architecture.md) (design rationale, wire
format, interview Q&A), [`docs/results.md`](docs/results.md) (all measured numbers),
[`MANUAL_TESTING.md`](MANUAL_TESTING.md) (manual verification steps).
