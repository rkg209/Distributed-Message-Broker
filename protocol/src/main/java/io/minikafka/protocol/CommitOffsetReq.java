package io.minikafka.protocol;

/** Client → Broker: commit a consumer group's offset for a topic partition. */
public record CommitOffsetReq(
    long correlationId, String group, String topic, int partition, long offset) implements Message {

  public CommitOffsetReq {
    if (group == null) {
      throw new IllegalArgumentException("group must not be null");
    }
    if (topic == null) {
      throw new IllegalArgumentException("topic must not be null");
    }
  }

  @Override
  public MessageType type() {
    return MessageType.COMMIT_OFFSET_REQ;
  }
}
