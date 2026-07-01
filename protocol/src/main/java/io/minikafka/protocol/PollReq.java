package io.minikafka.protocol;

/** Client → Broker: fetch records from a topic partition starting at an offset. */
public record PollReq(long correlationId, String topic, int partition, long offset)
    implements Message {

  public PollReq {
    if (topic == null) {
      throw new IllegalArgumentException("topic must not be null");
    }
  }

  @Override
  public MessageType type() {
    return MessageType.POLL_REQ;
  }
}
