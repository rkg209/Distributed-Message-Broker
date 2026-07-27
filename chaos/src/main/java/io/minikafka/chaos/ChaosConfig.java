package io.minikafka.chaos;

import java.util.function.Function;

/**
 * Every tunable of a chaos run, built from system properties (set by Gradle from {@code -P} flags).
 * Nothing about run size or timing is hardcoded, per CLAUDE.md.
 */
public record ChaosConfig(
    int crashes,
    long messages,
    int producers,
    int consumers,
    String topic,
    int partitions,
    long seed,
    long faultIntervalMessages,
    long recoverySettleMs,
    long partitionHealMs,
    long runTimeoutMs) {

  private static final int DEFAULT_CRASHES = 20;
  private static final long DEFAULT_MESSAGES = 50_000;
  private static final int DEFAULT_PRODUCERS = 8;
  private static final int DEFAULT_CONSUMERS = 3;
  private static final String DEFAULT_TOPIC = "chaos";
  private static final int DEFAULT_PARTITIONS = 3;
  private static final long DEFAULT_SEED = 0;
  private static final long DEFAULT_RECOVERY_SETTLE_MS = 3_000;
  private static final long DEFAULT_PARTITION_HEAL_MIN_MS = 5_000;
  private static final long DEFAULT_PARTITION_HEAL_MAX_MS = 10_000;
  private static final long DEFAULT_RUN_TIMEOUT_MS = 15 * 60 * 1000;

  public ChaosConfig {
    if (crashes < 0) {
      throw new IllegalArgumentException("crashes must be >= 0");
    }
    if (messages <= 0) {
      throw new IllegalArgumentException("messages must be positive");
    }
    if (partitions <= 0) {
      throw new IllegalArgumentException("partitions must be positive");
    }
  }

  public static ChaosConfig fromSystemProperties() {
    return fromProperties(System::getProperty);
  }

  static ChaosConfig fromProperties(Function<String, String> props) {
    int crashes = optionalInt(props, "chaos.crashes", DEFAULT_CRASHES);
    long messages = optionalLong(props, "chaos.messages", DEFAULT_MESSAGES);
    int producers = optionalInt(props, "chaos.producers", DEFAULT_PRODUCERS);
    int consumers = optionalInt(props, "chaos.consumers", DEFAULT_CONSUMERS);
    String topic = optionalString(props, "chaos.topic", DEFAULT_TOPIC);
    int partitions = optionalInt(props, "chaos.partitions", DEFAULT_PARTITIONS);
    long seed = optionalLong(props, "chaos.seed", DEFAULT_SEED);
    long faultIntervalMessages = Math.max(1, messages / Math.max(1, crashes));
    long recoverySettleMs =
        optionalLong(props, "chaos.recoverySettleMs", DEFAULT_RECOVERY_SETTLE_MS);
    long partitionHealMs =
        optionalLong(props, "chaos.partitionHealMs", DEFAULT_PARTITION_HEAL_MIN_MS);
    long runTimeoutMs = optionalLong(props, "chaos.runTimeoutMs", DEFAULT_RUN_TIMEOUT_MS);
    return new ChaosConfig(
        crashes,
        messages,
        producers,
        consumers,
        topic,
        partitions,
        seed,
        faultIntervalMessages,
        recoverySettleMs,
        partitionHealMs,
        runTimeoutMs);
  }

  /** Random partition-heal delay in {@code [5s, 10s)}, seeded from this config's seed. */
  public long randomPartitionHealMs(java.util.Random random) {
    long span = DEFAULT_PARTITION_HEAL_MAX_MS - DEFAULT_PARTITION_HEAL_MIN_MS;
    return DEFAULT_PARTITION_HEAL_MIN_MS + (span <= 0 ? 0 : Math.floorMod(random.nextLong(), span));
  }

  private static String optionalString(Function<String, String> props, String key, String def) {
    String v = props.apply(key);
    return v == null || v.isBlank() ? def : v;
  }

  private static int optionalInt(Function<String, String> props, String key, int def) {
    String v = props.apply(key);
    return v == null || v.isBlank() ? def : Integer.parseInt(v);
  }

  private static long optionalLong(Function<String, String> props, String key, long def) {
    String v = props.apply(key);
    return v == null || v.isBlank() ? def : Long.parseLong(v);
  }
}
