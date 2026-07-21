package io.minikafka.protocol;

/** Client → Broker: fetch a consumer group's last committed offset for a topic partition. */
public record FetchOffsetReq(long correlationId, String group, String topic, int partition)
    implements Message {

  public FetchOffsetReq {
    if (group == null) {
      throw new IllegalArgumentException("group must not be null");
    }
    if (topic == null) {
      throw new IllegalArgumentException("topic must not be null");
    }
  }

  @Override
  public MessageType type() {
    return MessageType.FETCH_OFFSET_REQ;
  }
}
