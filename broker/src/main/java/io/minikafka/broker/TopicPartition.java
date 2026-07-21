package io.minikafka.broker;

/** Identifies a single partition of a topic; the registry key for {@link TopicRegistry}. */
public record TopicPartition(String topic, int partition) {

  public TopicPartition {
    if (topic == null) {
      throw new IllegalArgumentException("topic must not be null");
    }
  }
}
