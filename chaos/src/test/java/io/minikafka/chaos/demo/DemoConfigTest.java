package io.minikafka.chaos.demo;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.Map;
import org.junit.jupiter.api.Test;

class DemoConfigTest {

  @Test
  void defaultsWhenNoPropertiesSet() {
    DemoConfig config = DemoConfig.fromProperties(Map.<String, String>of()::get);

    assertEquals("localhost", config.bootstrapHost());
    assertEquals(9092, config.bootstrapPort());
    assertEquals("orders", config.topic());
    assertEquals(3, config.partitions());
    assertEquals(20_000, config.messages());
    assertEquals(8, config.producers());
    assertEquals(2_000, config.killAtMessages());
    assertEquals("broker-2", config.killService());
    assertEquals("KILL", config.killSignal());
    assertEquals("docker-compose.yml", config.composeFile());
    assertEquals("../docker", config.composeDir());
    assertEquals(true, config.restartAfter());
    assertEquals(3_000, config.settleMs());
    assertEquals(600_000, config.runTimeoutMs());
  }

  @Test
  void killAtMessagesDerivesFromMessagesWhenNotOverridden() {
    DemoConfig config = DemoConfig.fromProperties(Map.of("demo.messages", "100000")::get);

    assertEquals(10_000, config.killAtMessages());
  }

  @Test
  void killAtMessagesOverrideWins() {
    Map<String, String> props = Map.of("demo.messages", "100000", "demo.killAtMessages", "500");

    DemoConfig config = DemoConfig.fromProperties(props::get);

    assertEquals(500, config.killAtMessages());
  }

  @Test
  void overridesFromProperties() {
    Map<String, String> props =
        Map.of(
            "demo.bootstrap", "broker-2:9093",
            "demo.topic", "chaos",
            "demo.partitions", "5",
            "demo.messages", "5000",
            "demo.producers", "4",
            "demo.killService", "broker-3",
            "demo.restartAfter", "false");

    DemoConfig config = DemoConfig.fromProperties(props::get);

    assertEquals("broker-2", config.bootstrapHost());
    assertEquals(9093, config.bootstrapPort());
    assertEquals("chaos", config.topic());
    assertEquals(5, config.partitions());
    assertEquals(5000, config.messages());
    assertEquals(4, config.producers());
    assertEquals("broker-3", config.killService());
    assertEquals(false, config.restartAfter());
  }

  @Test
  void rejectsMalformedBootstrap() {
    Map<String, String> props = Map.of("demo.bootstrap", "no-port-here");
    assertThrows(IllegalArgumentException.class, () -> DemoConfig.fromProperties(props::get));
  }

  @Test
  void rejectsNonPositiveMessages() {
    Map<String, String> props = Map.of("demo.messages", "0");
    assertThrows(IllegalArgumentException.class, () -> DemoConfig.fromProperties(props::get));
  }
}
