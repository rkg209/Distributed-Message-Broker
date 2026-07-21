package io.minikafka.broker;

import java.util.HashMap;
import java.util.Map;

/**
 * Static partition-count configuration, parsed from {@code BROKER_TOPICS} (e.g. {@code
 * "orders:4,events:8"}). Topics not listed fall back to {@code defaultPartitions}.
 */
public record TopicConfig(Map<String, Integer> partitionCounts, int defaultPartitions) {

  public TopicConfig {
    if (partitionCounts == null) {
      throw new IllegalArgumentException("partitionCounts must not be null");
    }
    if (defaultPartitions <= 0) {
      throw new IllegalArgumentException(
          "defaultPartitions must be positive: " + defaultPartitions);
    }
    partitionCounts = Map.copyOf(partitionCounts);
  }

  /** The configured partition count for {@code topic}, or {@link #defaultPartitions()}. */
  public int partitionCountFor(String topic) {
    return partitionCounts.getOrDefault(topic, defaultPartitions);
  }

  /**
   * Parses {@code "topic1:n1,topic2:n2"} into a {@link TopicConfig}. Throws on malformed entries or
   * non-positive counts.
   *
   * @param spec the raw {@code BROKER_TOPICS} value; blank or {@code null} yields no configured
   *     topics
   */
  public static TopicConfig parse(String spec, int defaultPartitions) {
    Map<String, Integer> counts = new HashMap<>();
    if (spec != null && !spec.isBlank()) {
      for (String entry : spec.split(",")) {
        String trimmed = entry.trim();
        if (trimmed.isEmpty()) {
          continue;
        }
        int colon = trimmed.indexOf(':');
        if (colon <= 0 || colon == trimmed.length() - 1) {
          throw new IllegalStateException(
              "Malformed BROKER_TOPICS entry (expected topic:count): " + trimmed);
        }
        String topic = trimmed.substring(0, colon);
        String countStr = trimmed.substring(colon + 1);
        int count;
        try {
          count = Integer.parseInt(countStr);
        } catch (NumberFormatException e) {
          throw new IllegalStateException(
              "Malformed BROKER_TOPICS partition count for " + topic + ": " + countStr, e);
        }
        if (count <= 0) {
          throw new IllegalStateException(
              "BROKER_TOPICS partition count for " + topic + " must be positive: " + count);
        }
        counts.put(topic, count);
      }
    }
    return new TopicConfig(counts, defaultPartitions);
  }
}
