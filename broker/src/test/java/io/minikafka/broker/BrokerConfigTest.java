package io.minikafka.broker;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import io.minikafka.protocol.ProtocolConfig;
import java.util.Map;
import org.junit.jupiter.api.Test;

class BrokerConfigTest {

  @Test
  void parsesAllFieldsFromEnv() {
    Map<String, String> env =
        Map.of(
            "BROKER_ID", "1",
            "BROKER_HOST", "localhost",
            "BROKER_PORT", "9092");

    BrokerConfig config = BrokerConfig.fromEnv(env::get);

    assertEquals(1, config.brokerId());
    assertEquals("localhost", config.brokerHost());
    assertEquals(9092, config.brokerPort());
  }

  @Test
  void defaultsMaxFrameBytesWhenUnset() {
    Map<String, String> env =
        Map.of("BROKER_ID", "1", "BROKER_HOST", "localhost", "BROKER_PORT", "9092");

    BrokerConfig config = BrokerConfig.fromEnv(env::get);

    assertEquals(ProtocolConfig.DEFAULT_MAX_FRAME_BYTES, config.maxFrameBytes());
  }

  @Test
  void parsesMaxFrameBytesWhenSet() {
    Map<String, String> env =
        Map.of(
            "BROKER_ID", "1",
            "BROKER_HOST", "localhost",
            "BROKER_PORT", "9092",
            "BROKER_MAX_FRAME_BYTES", "1048576");

    BrokerConfig config = BrokerConfig.fromEnv(env::get);

    assertEquals(1048576, config.maxFrameBytes());
  }

  @Test
  void defaultsMaxPollBytesWhenUnset() {
    Map<String, String> env =
        Map.of("BROKER_ID", "1", "BROKER_HOST", "localhost", "BROKER_PORT", "9092");

    BrokerConfig config = BrokerConfig.fromEnv(env::get);

    assertEquals(1024 * 1024, config.maxPollBytes());
  }

  @Test
  void parsesMaxPollBytesWhenSet() {
    Map<String, String> env =
        Map.of(
            "BROKER_ID", "1",
            "BROKER_HOST", "localhost",
            "BROKER_PORT", "9092",
            "BROKER_MAX_POLL_BYTES", "2048");

    BrokerConfig config = BrokerConfig.fromEnv(env::get);

    assertEquals(2048, config.maxPollBytes());
  }

  @Test
  void throwsWhenRequiredVariableIsMissing() {
    Map<String, String> env = Map.of("BROKER_HOST", "localhost", "BROKER_PORT", "9092");

    assertThrows(IllegalStateException.class, () -> BrokerConfig.fromEnv(env::get));
  }

  @Test
  void throwsWhenPortIsNotAnInteger() {
    Map<String, String> env =
        Map.of(
            "BROKER_ID", "1",
            "BROKER_HOST", "localhost",
            "BROKER_PORT", "not-a-number");

    assertThrows(IllegalStateException.class, () -> BrokerConfig.fromEnv(env::get));
  }
}
