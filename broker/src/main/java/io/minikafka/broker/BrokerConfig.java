package io.minikafka.broker;

import io.minikafka.log.FsyncPolicy;
import io.minikafka.log.LogConfig;
import io.minikafka.protocol.BrokerInfo;
import io.minikafka.protocol.ProtocolConfig;
import io.minikafka.raft.RaftConfig;
import java.nio.file.Path;
import java.util.Arrays;

/**
 * Broker configuration, loaded from environment variables. All timeouts, sizes, and policies
 * introduced in later specs must be added here rather than hardcoded.
 */
public record BrokerConfig(
    int brokerId,
    String brokerHost,
    int brokerPort,
    int maxFrameBytes,
    int maxPollBytes,
    String logDir,
    FsyncPolicy fsyncPolicy,
    long fsyncIntervalMs,
    int segmentBytes,
    int indexIntervalBytes,
    long retentionBytes,
    long retentionMs,
    TopicConfig topicConfig,
    String offsetDir,
    ClusterConfig clusterConfig,
    long heartbeatIntervalMs,
    long heartbeatTimeoutMs,
    long peerReconnectBackoffMs,
    long raftElectionTimeoutMinMs,
    long raftElectionTimeoutMaxMs,
    long raftHeartbeatIntervalMs,
    long raftRpcTimeoutMs,
    int raftMaxEntriesPerAppend,
    long raftProposeTimeoutMs,
    long raftLeaderWaitMs) {

  private static final String BROKER_ID = "BROKER_ID";
  private static final String BROKER_HOST = "BROKER_HOST";
  private static final String BROKER_PORT = "BROKER_PORT";
  private static final String BROKER_MAX_FRAME_BYTES = "BROKER_MAX_FRAME_BYTES";
  private static final String BROKER_MAX_POLL_BYTES = "BROKER_MAX_POLL_BYTES";
  private static final String BROKER_LOG_DIR = "BROKER_LOG_DIR";
  private static final String BROKER_FSYNC_POLICY = "BROKER_FSYNC_POLICY";
  private static final String BROKER_FSYNC_INTERVAL_MS = "BROKER_FSYNC_INTERVAL_MS";
  private static final String BROKER_SEGMENT_BYTES = "BROKER_SEGMENT_BYTES";
  private static final String BROKER_INDEX_INTERVAL_BYTES = "BROKER_INDEX_INTERVAL_BYTES";
  private static final String BROKER_RETENTION_BYTES = "BROKER_RETENTION_BYTES";
  private static final String BROKER_RETENTION_MS = "BROKER_RETENTION_MS";
  private static final String BROKER_TOPICS = "BROKER_TOPICS";
  private static final String BROKER_DEFAULT_PARTITIONS = "BROKER_DEFAULT_PARTITIONS";
  private static final String BROKER_OFFSET_DIR = "BROKER_OFFSET_DIR";
  private static final String BROKER_LIST = "BROKER_LIST";
  private static final String PARTITION_ASSIGNMENTS = "PARTITION_ASSIGNMENTS";
  private static final String REPLICATION_FACTOR = "REPLICATION_FACTOR";
  private static final String CONTROLLER_ID = "CONTROLLER_ID";
  private static final String BROKER_HEARTBEAT_INTERVAL_MS = "BROKER_HEARTBEAT_INTERVAL_MS";
  private static final String BROKER_HEARTBEAT_TIMEOUT_MS = "BROKER_HEARTBEAT_TIMEOUT_MS";
  private static final String BROKER_PEER_RECONNECT_BACKOFF_MS = "BROKER_PEER_RECONNECT_BACKOFF_MS";
  private static final String RAFT_ELECTION_TIMEOUT_MIN_MS = "RAFT_ELECTION_TIMEOUT_MIN_MS";
  private static final String RAFT_ELECTION_TIMEOUT_MAX_MS = "RAFT_ELECTION_TIMEOUT_MAX_MS";
  private static final String RAFT_HEARTBEAT_INTERVAL_MS = "RAFT_HEARTBEAT_INTERVAL_MS";
  private static final String RAFT_RPC_TIMEOUT_MS = "RAFT_RPC_TIMEOUT_MS";
  private static final String RAFT_MAX_ENTRIES_PER_APPEND = "RAFT_MAX_ENTRIES_PER_APPEND";
  private static final String RAFT_PROPOSE_TIMEOUT_MS = "RAFT_PROPOSE_TIMEOUT_MS";
  private static final String RAFT_LEADER_WAIT_MS = "RAFT_LEADER_WAIT_MS";
  private static final int DEFAULT_MAX_POLL_BYTES = 1024 * 1024;
  private static final int DEFAULT_PARTITIONS = 1;
  private static final String DEFAULT_OFFSET_SUBDIR = "__offsets";
  private static final long DEFAULT_HEARTBEAT_INTERVAL_MS = 500;
  private static final long DEFAULT_HEARTBEAT_TIMEOUT_MS = 2000;
  private static final long DEFAULT_PEER_RECONNECT_BACKOFF_MS = 500;
  private static final long DEFAULT_RAFT_PROPOSE_TIMEOUT_MS = 5000;
  private static final long DEFAULT_RAFT_LEADER_WAIT_MS = 2000;

  /**
   * Loads configuration from environment variables. Throws if a required variable is missing or
   * malformed — configuration errors must never fail silently.
   */
  public static BrokerConfig fromEnv() {
    return fromEnv(System::getenv);
  }

  static BrokerConfig fromEnv(java.util.function.Function<String, String> env) {
    int brokerId = parseInt(BROKER_ID, requireEnv(env, BROKER_ID));
    String brokerHost = requireEnv(env, BROKER_HOST);
    int brokerPort = parseInt(BROKER_PORT, requireEnv(env, BROKER_PORT));
    int maxFrameBytes =
        optionalInt(env, BROKER_MAX_FRAME_BYTES, ProtocolConfig.DEFAULT_MAX_FRAME_BYTES);
    int maxPollBytes = optionalInt(env, BROKER_MAX_POLL_BYTES, DEFAULT_MAX_POLL_BYTES);
    String logDir = requireEnv(env, BROKER_LOG_DIR);
    FsyncPolicy fsyncPolicy = parseFsyncPolicy(env.apply(BROKER_FSYNC_POLICY));
    long fsyncIntervalMs =
        optionalLong(env, BROKER_FSYNC_INTERVAL_MS, LogConfig.DEFAULT_FSYNC_INTERVAL_MS);
    int segmentBytes = optionalInt(env, BROKER_SEGMENT_BYTES, LogConfig.DEFAULT_MAX_SEGMENT_BYTES);
    int indexIntervalBytes =
        optionalInt(env, BROKER_INDEX_INTERVAL_BYTES, LogConfig.DEFAULT_INDEX_INTERVAL_BYTES);
    long retentionBytes = optionalLong(env, BROKER_RETENTION_BYTES, LogConfig.UNLIMITED);
    long retentionMs = optionalLong(env, BROKER_RETENTION_MS, LogConfig.UNLIMITED);
    int defaultPartitions = optionalInt(env, BROKER_DEFAULT_PARTITIONS, DEFAULT_PARTITIONS);
    TopicConfig topicConfig = TopicConfig.parse(env.apply(BROKER_TOPICS), defaultPartitions);
    String offsetDir =
        optionalString(env, BROKER_OFFSET_DIR, Path.of(logDir, DEFAULT_OFFSET_SUBDIR).toString());

    String brokerListSpec = env.apply(BROKER_LIST);
    ClusterConfig clusterConfig;
    if (brokerListSpec == null || brokerListSpec.isBlank()) {
      clusterConfig = ClusterConfig.singleBroker(new BrokerInfo(brokerId, brokerHost, brokerPort));
    } else {
      Integer replicationFactor =
          optionalInteger(env, REPLICATION_FACTOR, "REPLICATION_FACTOR must be an integer");
      Integer controllerId =
          optionalInteger(env, CONTROLLER_ID, "CONTROLLER_ID must be an integer");
      clusterConfig =
          ClusterConfig.parse(
              brokerListSpec,
              env.apply(PARTITION_ASSIGNMENTS),
              replicationFactor,
              controllerId,
              brokerId,
              topicConfig);
    }
    long heartbeatIntervalMs =
        optionalLong(env, BROKER_HEARTBEAT_INTERVAL_MS, DEFAULT_HEARTBEAT_INTERVAL_MS);
    long heartbeatTimeoutMs =
        optionalLong(env, BROKER_HEARTBEAT_TIMEOUT_MS, DEFAULT_HEARTBEAT_TIMEOUT_MS);
    long peerReconnectBackoffMs =
        optionalLong(env, BROKER_PEER_RECONNECT_BACKOFF_MS, DEFAULT_PEER_RECONNECT_BACKOFF_MS);
    long raftElectionTimeoutMinMs =
        optionalLong(env, RAFT_ELECTION_TIMEOUT_MIN_MS, RaftConfig.DEFAULT_MIN_ELECTION_TIMEOUT_MS);
    long raftElectionTimeoutMaxMs =
        optionalLong(env, RAFT_ELECTION_TIMEOUT_MAX_MS, RaftConfig.DEFAULT_MAX_ELECTION_TIMEOUT_MS);
    long raftHeartbeatIntervalMs =
        optionalLong(env, RAFT_HEARTBEAT_INTERVAL_MS, RaftConfig.DEFAULT_HEARTBEAT_INTERVAL_MS);
    long raftRpcTimeoutMs =
        optionalLong(env, RAFT_RPC_TIMEOUT_MS, RaftConfig.DEFAULT_RPC_TIMEOUT_MS);
    int raftMaxEntriesPerAppend =
        optionalInt(env, RAFT_MAX_ENTRIES_PER_APPEND, RaftConfig.DEFAULT_MAX_ENTRIES_PER_APPEND);
    long raftProposeTimeoutMs =
        optionalLong(env, RAFT_PROPOSE_TIMEOUT_MS, DEFAULT_RAFT_PROPOSE_TIMEOUT_MS);
    long raftLeaderWaitMs = optionalLong(env, RAFT_LEADER_WAIT_MS, DEFAULT_RAFT_LEADER_WAIT_MS);

    return new BrokerConfig(
        brokerId,
        brokerHost,
        brokerPort,
        maxFrameBytes,
        maxPollBytes,
        logDir,
        fsyncPolicy,
        fsyncIntervalMs,
        segmentBytes,
        indexIntervalBytes,
        retentionBytes,
        retentionMs,
        topicConfig,
        offsetDir,
        clusterConfig,
        heartbeatIntervalMs,
        heartbeatTimeoutMs,
        peerReconnectBackoffMs,
        raftElectionTimeoutMinMs,
        raftElectionTimeoutMaxMs,
        raftHeartbeatIntervalMs,
        raftRpcTimeoutMs,
        raftMaxEntriesPerAppend,
        raftProposeTimeoutMs,
        raftLeaderWaitMs);
  }

  private static FsyncPolicy parseFsyncPolicy(String value) {
    if (value == null || value.isBlank()) {
      return LogConfig.DEFAULT_FSYNC_POLICY;
    }
    try {
      return FsyncPolicy.valueOf(value);
    } catch (IllegalArgumentException e) {
      throw new IllegalStateException(
          "Environment variable "
              + BROKER_FSYNC_POLICY
              + " must be one of "
              + Arrays.toString(FsyncPolicy.values())
              + ", got: "
              + value,
          e);
    }
  }

  /**
   * Resolves the per-partition durable log directory and config: {@code
   * {logDir}/{topic}-{partition}}.
   */
  public LogConfig logConfigFor(TopicPartition tp) {
    Path dir = Path.of(logDir, tp.topic() + "-" + tp.partition());
    return new LogConfig(
        dir,
        fsyncPolicy,
        fsyncIntervalMs,
        segmentBytes,
        indexIntervalBytes,
        retentionBytes,
        retentionMs);
  }

  /** Resolves the offset-store directory as a {@link Path}. */
  public Path offsetDirPath() {
    return Path.of(offsetDir);
  }

  /** Resolves the per-partition Raft state directory: {@code {logDir}/{topic}-{partition}/raft}. */
  public Path raftStateDirFor(TopicPartition tp) {
    return Path.of(logDir, tp.topic() + "-" + tp.partition(), "raft");
  }

  /** Builds the {@link RaftConfig} for {@code tp}'s Raft group from the configured tunables. */
  public RaftConfig raftConfigFor(TopicPartition tp) {
    return new RaftConfig(
        raftElectionTimeoutMinMs,
        raftElectionTimeoutMaxMs,
        raftHeartbeatIntervalMs,
        raftMaxEntriesPerAppend,
        raftRpcTimeoutMs,
        raftStateDirFor(tp));
  }

  private static String optionalString(
      java.util.function.Function<String, String> env, String name, String defaultValue) {
    String value = env.apply(name);
    return value == null || value.isBlank() ? defaultValue : value;
  }

  private static String requireEnv(java.util.function.Function<String, String> env, String name) {
    String value = env.apply(name);
    if (value == null || value.isBlank()) {
      throw new IllegalStateException("Missing required environment variable: " + name);
    }
    return value;
  }

  private static int optionalInt(
      java.util.function.Function<String, String> env, String name, int defaultValue) {
    String value = env.apply(name);
    if (value == null || value.isBlank()) {
      return defaultValue;
    }
    return parseInt(name, value);
  }

  private static long optionalLong(
      java.util.function.Function<String, String> env, String name, long defaultValue) {
    String value = env.apply(name);
    if (value == null || value.isBlank()) {
      return defaultValue;
    }
    return parseLong(name, value);
  }

  private static Integer optionalInteger(
      java.util.function.Function<String, String> env, String name, String errorMessage) {
    String value = env.apply(name);
    if (value == null || value.isBlank()) {
      return null;
    }
    try {
      return Integer.parseInt(value);
    } catch (NumberFormatException e) {
      throw new IllegalStateException(errorMessage + ", got: " + value, e);
    }
  }

  private static long parseLong(String name, String value) {
    try {
      return Long.parseLong(value);
    } catch (NumberFormatException e) {
      throw new IllegalStateException(
          "Environment variable " + name + " must be a long, got: " + value, e);
    }
  }

  private static int parseInt(String name, String value) {
    try {
      return Integer.parseInt(value);
    } catch (NumberFormatException e) {
      throw new IllegalStateException(
          "Environment variable " + name + " must be an integer, got: " + value, e);
    }
  }
}
