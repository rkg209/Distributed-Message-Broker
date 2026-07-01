# mini-kafka

<!-- TODO: replace OWNER/REPO once a GitHub remote exists -->
![CI](https://github.com/OWNER/REPO/actions/workflows/ci.yml/badge.svg)

A from-scratch, Raft-replicated, durable message broker (mini-Kafka) in Java 21.

See [`docs/architecture.md`](docs/architecture.md) for the full design and
[`docs/results.md`](docs/results.md) for chaos/benchmark results.

## Build

```bash
./gradlew build
./gradlew test
./gradlew spotlessApply
```
