package io.minikafka.bench;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.Map;
import org.junit.jupiter.api.Test;

class BenchConfigTest {

  @Test
  void defaultsWhenNoPropertiesSet() {
    BenchConfig config = BenchConfig.fromProperties(Map.<String, String>of()::get);

    assertEquals("localhost", config.bootstrapHost());
    assertEquals(9092, config.bootstrapPort());
    assertEquals("bench", config.topic());
    assertEquals(3, config.partitions());
    assertEquals(3, config.replicationFactor());
    assertEquals(1024, config.payloadBytes());
    assertEquals(8, config.threads());
  }

  @Test
  void overridesFromProperties() {
    Map<String, String> props =
        Map.of(
            "bench.bootstrap", "broker-2:9093",
            "bench.topic", "orders",
            "bench.partitions", "5",
            "bench.rf", "1",
            "bench.payloadBytes", "4096",
            "bench.threads", "16");

    BenchConfig config = BenchConfig.fromProperties(props::get);

    assertEquals("broker-2", config.bootstrapHost());
    assertEquals(9093, config.bootstrapPort());
    assertEquals("orders", config.topic());
    assertEquals(5, config.partitions());
    assertEquals(1, config.replicationFactor());
    assertEquals(4096, config.payloadBytes());
    assertEquals(16, config.threads());
  }

  @Test
  void rejectsMalformedBootstrap() {
    Map<String, String> props = Map.of("bench.bootstrap", "no-port-here");
    assertThrows(IllegalArgumentException.class, () -> BenchConfig.fromProperties(props::get));
  }

  @Test
  void rejectsNonNumericBootstrapPort() {
    Map<String, String> props = Map.of("bench.bootstrap", "localhost:notaport");
    assertThrows(IllegalArgumentException.class, () -> BenchConfig.fromProperties(props::get));
  }

  @Test
  void rejectsMalformedIntegerValue() {
    Map<String, String> props = Map.of("bench.threads", "not-a-number");
    assertThrows(IllegalArgumentException.class, () -> BenchConfig.fromProperties(props::get));
  }

  @Test
  void rejectsNonPositivePartitions() {
    assertThrows(
        IllegalArgumentException.class,
        () -> new BenchConfig("localhost", 9092, "bench", 0, 3, 1024, 8, 16 * 1024 * 1024));
  }
}
