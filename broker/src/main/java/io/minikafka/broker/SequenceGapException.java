package io.minikafka.broker;

/**
 * Thrown when a producer's sequence number skips ahead of {@code lastCommitted + 1} for a
 * partition. The message was <b>not</b> appended; the producer must not treat it as delivered.
 */
public final class SequenceGapException extends RuntimeException {

  public SequenceGapException(
      TopicPartition topicPartition, long producerId, long expectedSeq, long actualSeq) {
    super(
        "Sequence gap for producer "
            + producerId
            + " on "
            + topicPartition
            + ": expected "
            + expectedSeq
            + " but got "
            + actualSeq);
  }
}
