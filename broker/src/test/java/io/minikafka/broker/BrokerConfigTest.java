package io.minikafka.broker;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

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
