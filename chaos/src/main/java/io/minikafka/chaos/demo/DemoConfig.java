package io.minikafka.chaos.demo;

import java.util.function.Function;

/**
 * Every tunable of the one-command failover demo, built from system properties (set by Gradle from
 * {@code -P} flags). Nothing about run size or timing is hardcoded, per CLAUDE.md. Mirrors {@code
 * ChaosConfig.fromProperties}.
 */
public record DemoConfig(
    String bootstrapHost,
    int bootstrapPort,
    String topic,
    int partitions,
    long messages,
    int producers,
    long killAtMessages,
    String killService,
    String killSignal,
    String composeFile,
    String composeDir,
    boolean restartAfter,
    long settleMs,
    long runTimeoutMs) {

  private static final String DEFAULT_BOOTSTRAP = "localhost:9092";
  private static final String DEFAULT_TOPIC = "orders";
  private static final int DEFAULT_PARTITIONS = 3;
  private static final long DEFAULT_MESSAGES = 20_000;
  private static final int DEFAULT_PRODUCERS = 8;
  private static final String DEFAULT_KILL_SERVICE = "broker-2";
  private static final String DEFAULT_KILL_SIGNAL = "KILL";
  private static final String DEFAULT_COMPOSE_FILE = "docker-compose.yml";
  private static final String DEFAULT_COMPOSE_DIR = "../docker";
  private static final boolean DEFAULT_RESTART_AFTER = true;
  private static final long DEFAULT_SETTLE_MS = 3_000;
  private static final long DEFAULT_RUN_TIMEOUT_MS = 600_000;

  public DemoConfig {
    if (bootstrapPort <= 0) {
      throw new IllegalArgumentException("bootstrapPort must be positive");
    }
    if (partitions <= 0) {
      throw new IllegalArgumentException("partitions must be positive");
    }
    if (messages <= 0) {
      throw new IllegalArgumentException("messages must be positive");
    }
    if (producers <= 0) {
      throw new IllegalArgumentException("producers must be positive");
    }
    if (killAtMessages <= 0) {
      throw new IllegalArgumentException("killAtMessages must be positive");
    }
  }

  public static DemoConfig fromSystemProperties() {
    return fromProperties(System::getProperty);
  }

  static DemoConfig fromProperties(Function<String, String> props) {
    String bootstrap = optionalString(props, "demo.bootstrap", DEFAULT_BOOTSTRAP);
    int colon = bootstrap.lastIndexOf(':');
    if (colon <= 0 || colon == bootstrap.length() - 1) {
      throw new IllegalArgumentException(
          "Malformed demo.bootstrap (expected host:port): " + bootstrap);
    }
    String bootstrapHost = bootstrap.substring(0, colon);
    int bootstrapPort;
    try {
      bootstrapPort = Integer.parseInt(bootstrap.substring(colon + 1));
    } catch (NumberFormatException e) {
      throw new IllegalArgumentException("Malformed demo.bootstrap port: " + bootstrap, e);
    }

    String topic = optionalString(props, "demo.topic", DEFAULT_TOPIC);
    int partitions = optionalInt(props, "demo.partitions", DEFAULT_PARTITIONS);
    long messages = optionalLong(props, "demo.messages", DEFAULT_MESSAGES);
    int producers = optionalInt(props, "demo.producers", DEFAULT_PRODUCERS);
    long killAtMessages = optionalLong(props, "demo.killAtMessages", Math.max(1, messages / 10));
    String killService = optionalString(props, "demo.killService", DEFAULT_KILL_SERVICE);
    String killSignal = optionalString(props, "demo.killSignal", DEFAULT_KILL_SIGNAL);
    String composeFile = optionalString(props, "demo.composeFile", DEFAULT_COMPOSE_FILE);
    String composeDir = optionalString(props, "demo.composeDir", DEFAULT_COMPOSE_DIR);
    boolean restartAfter = optionalBoolean(props, "demo.restartAfter", DEFAULT_RESTART_AFTER);
    long settleMs = optionalLong(props, "demo.settleMs", DEFAULT_SETTLE_MS);
    long runTimeoutMs = optionalLong(props, "demo.runTimeoutMs", DEFAULT_RUN_TIMEOUT_MS);

    return new DemoConfig(
        bootstrapHost,
        bootstrapPort,
        topic,
        partitions,
        messages,
        producers,
        killAtMessages,
        killService,
        killSignal,
        composeFile,
        composeDir,
        restartAfter,
        settleMs,
        runTimeoutMs);
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

  private static long optionalLong(Function<String, String> props, String key, long def) {
    String v = props.apply(key);
    if (v == null || v.isBlank()) {
      return def;
    }
    try {
      return Long.parseLong(v);
    } catch (NumberFormatException e) {
      throw new IllegalArgumentException("Malformed " + key + ": " + v, e);
    }
  }

  private static boolean optionalBoolean(Function<String, String> props, String key, boolean def) {
    String v = props.apply(key);
    return v == null || v.isBlank() ? def : Boolean.parseBoolean(v);
  }
}
