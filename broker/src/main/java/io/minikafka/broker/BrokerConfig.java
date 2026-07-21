package io.minikafka.broker;

import io.minikafka.log.FsyncPolicy;
import io.minikafka.log.LogConfig;
import io.minikafka.protocol.ProtocolConfig;
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
    long retentionMs) {

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
  private static final int DEFAULT_MAX_POLL_BYTES = 1024 * 1024;

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
        retentionMs);
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
