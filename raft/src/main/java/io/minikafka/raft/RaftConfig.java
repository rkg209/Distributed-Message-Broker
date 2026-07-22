package io.minikafka.raft;

import java.nio.file.Path;

/** Every tunable of a {@link RaftNode}. CLAUDE.md forbids hardcoded configuration. */
public record RaftConfig(
    long minElectionTimeoutMs,
    long maxElectionTimeoutMs,
    long heartbeatIntervalMs,
    int maxEntriesPerAppend,
    long rpcTimeoutMs,
    Path stateDir) {

  public static final long DEFAULT_MIN_ELECTION_TIMEOUT_MS = 150;
  public static final long DEFAULT_MAX_ELECTION_TIMEOUT_MS = 300;
  public static final long DEFAULT_HEARTBEAT_INTERVAL_MS = 50;
  public static final int DEFAULT_MAX_ENTRIES_PER_APPEND = 100;
  public static final long DEFAULT_RPC_TIMEOUT_MS = 200;

  public RaftConfig {
    if (minElectionTimeoutMs <= 0) {
      throw new IllegalArgumentException("minElectionTimeoutMs must be positive");
    }
    if (maxElectionTimeoutMs < minElectionTimeoutMs) {
      throw new IllegalArgumentException("maxElectionTimeoutMs must be >= minElectionTimeoutMs");
    }
    if (heartbeatIntervalMs <= 0) {
      throw new IllegalArgumentException("heartbeatIntervalMs must be positive");
    }
    if (heartbeatIntervalMs >= minElectionTimeoutMs) {
      throw new IllegalArgumentException(
          "heartbeatIntervalMs must be < minElectionTimeoutMs or leaders will time out");
    }
    if (maxEntriesPerAppend <= 0) {
      throw new IllegalArgumentException("maxEntriesPerAppend must be positive");
    }
    if (rpcTimeoutMs <= 0) {
      throw new IllegalArgumentException("rpcTimeoutMs must be positive");
    }
    if (stateDir == null) {
      throw new IllegalArgumentException("stateDir must not be null");
    }
  }

  public static RaftConfig defaultsFor(Path stateDir) {
    return new RaftConfig(
        DEFAULT_MIN_ELECTION_TIMEOUT_MS,
        DEFAULT_MAX_ELECTION_TIMEOUT_MS,
        DEFAULT_HEARTBEAT_INTERVAL_MS,
        DEFAULT_MAX_ENTRIES_PER_APPEND,
        DEFAULT_RPC_TIMEOUT_MS,
        stateDir);
  }
}
