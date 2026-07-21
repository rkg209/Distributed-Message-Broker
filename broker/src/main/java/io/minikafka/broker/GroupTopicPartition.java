package io.minikafka.broker;

/** Identifies one consumer group's committed-offset slot for a topic partition. */
public record GroupTopicPartition(String group, String topic, int partition) {

  public GroupTopicPartition {
    if (group == null) {
      throw new IllegalArgumentException("group must not be null");
    }
    if (topic == null) {
      throw new IllegalArgumentException("topic must not be null");
    }
  }
}
