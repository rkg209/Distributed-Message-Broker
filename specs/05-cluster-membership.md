---
id: "05"
title: Cluster Membership & Metadata
status: todo
phase: 4
depends_on: ["04"]
requirements: [FR-30, FR-31, FR-32, FR-33, FR-34, DR-1, DR-2, DR-10]
---

# Spec 05 · Cluster Membership & Metadata

## What
Add static cluster membership: brokers form a cluster from Docker Compose
configuration, a lightweight controller/metadata role tracks partition→leader
assignment, brokers detect peer failures via heartbeats, and clients can
discover the current leader for any partition.

## Why
This is the foundation for distribution. Raft (Spec 06) and replication (Spec 07)
require brokers to know who their peers are and to communicate with them. The
metadata service enables client redirect after failover (Spec 08).

## What to build

### Static configuration
- `ClusterConfig` — loaded from environment/config file:
  `BROKER_LIST` (e.g. `broker-1:9092,broker-2:9092,broker-3:9092`),
  `PARTITION_ASSIGNMENTS` (which brokers replicate which partitions),
  `REPLICATION_FACTOR`
- `BrokerDescriptor` — immutable value: broker-id, host, port

### Controller / metadata (`broker/`)
- `MetadataService` — owns `Map<(topic, partition), BrokerDescriptor> leaderMap`;
  updated by Raft elections (Spec 06); answered via `METADATA_REQ`
- One broker is designated controller via static config (not elected for now)

### Heartbeat & failure detection (`broker/`)
- `HeartbeatMonitor` — each broker sends periodic heartbeats to peers; marks a broker
  as SUSPECTED after `heartbeatTimeout` ms without a response
- `PeerConnection` — manages outbound TCP connection to each peer broker; reconnects
  on failure; used by both the heartbeat monitor and (later) Raft RPC

### Docker Compose (`docker/`)
- `docker-compose.yml` — defines 3 broker services with static env config
- `Dockerfile` — builds the broker JAR into a Docker image
- Brokers can start, discover each other, and exchange heartbeats

## Acceptance criteria
1. **Cluster formation:** `docker compose up` starts 3 brokers; all three log that they
   have connected to their peers within the heartbeat timeout.
2. **Failure detection:** killing `broker-2` causes `broker-1` and `broker-3` to log a
   SUSPECTED status for `broker-2` within `2 × heartbeatTimeout`.
3. **Metadata query:** a client sends `METADATA_REQ`; any broker returns the current
   `partition → (leaderId, replicaBrokerIds)` map from static config.
4. **Partition assignment config:** partition assignments are readable from environment
   variables and reflected in the metadata response.
5. `./gradlew test` GREEN; integration tests use Testcontainers to spin up the 3-broker
   Docker Compose cluster.

## Out of scope
- Dynamic membership discovery.
- Raft election (leader is static config for now; Spec 06 makes it dynamic).
- Client redirect on NOT_LEADER (Spec 08).
