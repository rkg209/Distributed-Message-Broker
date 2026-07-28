package io.minikafka.bench;

import io.minikafka.protocol.ProtocolConfig;
import java.util.function.Function;

/**
 * Every tunable of a benchmark or load-generator run, built from system properties (set by Gradle
 * from {@code -P} flags). Nothing about scale or sizing is hardcoded, per CLAUDE.md.
 */
public record BenchConfig(
    String bootstrapHost,
    int bootstrapPort,
    String topic,
    int partitions,
    int replicationFactor,
    int payloadBytes,
    int threads,
    int maxFrameBytes) {

  private static final String DEFAULT_BOOTSTRAP_HOST = "localhost";
  private static final int DEFAULT_BOOTSTRAP_PORT = 9092;
  private static final String DEFAULT_TOPIC = "bench";
  private static final int DEFAULT_PARTITIONS = 3;
  private static final int DEFAULT_REPLICATION_FACTOR = 3;
  private static final int DEFAULT_PAYLOAD_BYTES = 1024;
  private static final int DEFAULT_THREADS = 8;

  public BenchConfig {
    if (bootstrapPort <= 0) {
      throw new IllegalArgumentException("bootstrapPort must be positive");
    }
    if (partitions <= 0) {
      throw new IllegalArgumentException("partitions must be positive");
    }
    if (replicationFactor <= 0) {
      throw new IllegalArgumentException("replicationFactor must be positive");
    }
    if (payloadBytes <= 0) {
      throw new IllegalArgumentException("payloadBytes must be positive");
    }
    if (threads <= 0) {
      throw new IllegalArgumentException("threads must be positive");
    }
    if (maxFrameBytes <= 0) {
      throw new IllegalArgumentException("maxFrameBytes must be positive");
    }
  }

  public static BenchConfig fromSystemProperties() {
    return fromProperties(System::getProperty);
  }

  static BenchConfig fromProperties(Function<String, String> props) {
    String bootstrap = optionalString(props, "bench.bootstrap", null);
    String bootstrapHost;
    int bootstrapPort;
    if (bootstrap == null) {
      bootstrapHost = DEFAULT_BOOTSTRAP_HOST;
      bootstrapPort = DEFAULT_BOOTSTRAP_PORT;
    } else {
      int colon = bootstrap.lastIndexOf(':');
      if (colon <= 0 || colon == bootstrap.length() - 1) {
        throw new IllegalArgumentException(
            "Malformed bench.bootstrap (expected host:port): " + bootstrap);
      }
      bootstrapHost = bootstrap.substring(0, colon);
      try {
        bootstrapPort = Integer.parseInt(bootstrap.substring(colon + 1));
      } catch (NumberFormatException e) {
        throw new IllegalArgumentException("Malformed bench.bootstrap port: " + bootstrap, e);
      }
    }
    String topic = optionalString(props, "bench.topic", DEFAULT_TOPIC);
    int partitions = optionalInt(props, "bench.partitions", DEFAULT_PARTITIONS);
    int replicationFactor = optionalInt(props, "bench.rf", DEFAULT_REPLICATION_FACTOR);
    int payloadBytes = optionalInt(props, "bench.payloadBytes", DEFAULT_PAYLOAD_BYTES);
    int threads = optionalInt(props, "bench.threads", DEFAULT_THREADS);
    int maxFrameBytes =
        optionalInt(props, "bench.maxFrameBytes", ProtocolConfig.DEFAULT_MAX_FRAME_BYTES);
    return new BenchConfig(
        bootstrapHost,
        bootstrapPort,
        topic,
        partitions,
        replicationFactor,
        payloadBytes,
        threads,
        maxFrameBytes);
  }

  private static String optionalString(Function<String, String> props, String key, String def) {
    String v = props.apply(key);
    return v == null || v.isBlank() ? def : v;
  }

  private static int optionalInt(Function<String, String> props, String key, int def) {
    String v = props.apply(key);
    if (v == null || v.isBlank()) {
      return def;
    }
    try {
      return Integer.parseInt(v);
    } catch (NumberFormatException e) {
      throw new IllegalArgumentException("Malformed " + key + ": " + v, e);
    }
  }
}
