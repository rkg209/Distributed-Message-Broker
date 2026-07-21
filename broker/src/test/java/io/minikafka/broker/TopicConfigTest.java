package io.minikafka.broker;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;

class TopicConfigTest {

  @Test
  void parsesMultipleTopics() {
    TopicConfig config = TopicConfig.parse("orders:4,events:8", 1);
    assertEquals(4, config.partitionCountFor("orders"));
    assertEquals(8, config.partitionCountFor("events"));
  }

  @Test
  void unlistedTopicUsesDefault() {
    TopicConfig config = TopicConfig.parse("orders:4", 2);
    assertEquals(2, config.partitionCountFor("unlisted"));
  }

  @Test
  void blankSpecUsesDefaultForEverything() {
    TopicConfig config = TopicConfig.parse(null, 3);
    assertEquals(3, config.partitionCountFor("anything"));
    config = TopicConfig.parse("  ", 3);
    assertEquals(3, config.partitionCountFor("anything"));
  }

  @Test
  void throwsOnMissingColon() {
    assertThrows(IllegalStateException.class, () -> TopicConfig.parse("orders4", 1));
  }

  @Test
  void throwsOnNonPositiveCount() {
    assertThrows(IllegalStateException.class, () -> TopicConfig.parse("orders:0", 1));
    assertThrows(IllegalStateException.class, () -> TopicConfig.parse("orders:-1", 1));
  }

  @Test
  void throwsOnNonNumericCount() {
    assertThrows(IllegalStateException.class, () -> TopicConfig.parse("orders:abc", 1));
  }

  @Test
  void constructorRejectsNonPositiveDefault() {
    assertThrows(IllegalArgumentException.class, () -> new TopicConfig(java.util.Map.of(), 0));
  }
}
