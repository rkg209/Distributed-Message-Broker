package io.minikafka.broker;

import io.minikafka.protocol.ProtocolConfig;

/**
 * Broker configuration, loaded from environment variables. All timeouts, sizes, and policies
 * introduced in later specs must be added here rather than hardcoded.
 */
public record BrokerConfig(int brokerId, String brokerHost, int brokerPort, int maxFrameBytes) {

  private static final String BROKER_ID = "BROKER_ID";
  private static final String BROKER_HOST = "BROKER_HOST";
  private static final String BROKER_PORT = "BROKER_PORT";
  private static final String BROKER_MAX_FRAME_BYTES = "BROKER_MAX_FRAME_BYTES";

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
    return new BrokerConfig(brokerId, brokerHost, brokerPort, maxFrameBytes);
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

  private static int parseInt(String name, String value) {
    try {
      return Integer.parseInt(value);
    } catch (NumberFormatException e) {
      throw new IllegalStateException(
          "Environment variable " + name + " must be an integer, got: " + value, e);
    }
  }
}
