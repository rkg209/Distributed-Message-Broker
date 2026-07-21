package io.minikafka.broker;

/** Thrown when a request names a partition id outside a topic's configured partition count. */
public final class UnknownPartitionException extends RuntimeException {

  public UnknownPartitionException(String topic, int partition, int partitionCount) {
    super(
        "Partition "
            + partition
            + " is out of range for topic "
            + topic
            + " (partitionCount="
            + partitionCount
            + ")");
  }
}
