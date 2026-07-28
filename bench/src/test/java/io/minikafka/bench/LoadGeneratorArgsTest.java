package io.minikafka.bench;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.time.Duration;
import org.junit.jupiter.api.Test;

class LoadGeneratorArgsTest {

  @Test
  void defaultsWhenNoArgs() {
    LoadGeneratorArgs args = LoadGeneratorArgs.parse(new String[0]);

    assertEquals(0, args.messages());
    assertNull(args.payloadSize());
    assertNull(args.threads());
    assertNull(args.rf());
    assertEquals(Duration.ofMinutes(5), args.duration());
    assertNull(args.bootstrapHost());
  }

  @Test
  void parsesAllFlags() {
    LoadGeneratorArgs args =
        LoadGeneratorArgs.parse(
            new String[] {
              "--messages=100000",
              "--payload-size=2048",
              "--threads=16",
              "--rf=3",
              "--duration=90s",
              "--bootstrap=localhost:9093",
              "--topic=orders",
              "--partitions=5"
            });

    assertEquals(100000, args.messages());
    assertEquals(2048, args.payloadSize());
    assertEquals(16, args.threads());
    assertEquals(3, args.rf());
    assertEquals(Duration.ofSeconds(90), args.duration());
    assertEquals("localhost", args.bootstrapHost());
    assertEquals(9093, args.bootstrapPort());
    assertEquals("orders", args.topic());
    assertEquals(5, args.partitions());
  }

  @Test
  void parsesDurationSuffixes() {
    assertEquals(
        Duration.ofMinutes(5), LoadGeneratorArgs.parse(new String[] {"--duration=5m"}).duration());
    assertEquals(
        Duration.ofHours(1), LoadGeneratorArgs.parse(new String[] {"--duration=1h"}).duration());
    assertEquals(
        Duration.ofSeconds(30), LoadGeneratorArgs.parse(new String[] {"--duration=30"}).duration());
  }

  @Test
  void rejectsUnknownFlag() {
    assertThrows(
        IllegalArgumentException.class, () -> LoadGeneratorArgs.parse(new String[] {"--bogus=1"}));
  }

  @Test
  void rejectsMalformedFlag() {
    assertThrows(
        IllegalArgumentException.class, () -> LoadGeneratorArgs.parse(new String[] {"nodashes"}));
  }

  @Test
  void rejectsMalformedBootstrap() {
    assertThrows(
        IllegalArgumentException.class,
        () -> LoadGeneratorArgs.parse(new String[] {"--bootstrap=noport"}));
  }

  @Test
  void rejectsMalformedDuration() {
    assertThrows(
        IllegalArgumentException.class,
        () -> LoadGeneratorArgs.parse(new String[] {"--duration=abc"}));
  }
}
