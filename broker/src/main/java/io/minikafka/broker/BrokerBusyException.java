package io.minikafka.broker;

/**
 * Thrown when a partition's publish admission gate is saturated: {@code capacity} publishes are
 * already in flight and none freed up within the acquire timeout. The message was <b>not</b>
 * proposed to Raft — the caller must not treat it as delivered.
 */
public final class BrokerBusyException extends RuntimeException {

  public BrokerBusyException(TopicPartition topicPartition, int capacity) {
    super(
        "Publish queue full for "
            + topicPartition
            + " (capacity="
            + capacity
            + "); retry after backing off");
  }
}
