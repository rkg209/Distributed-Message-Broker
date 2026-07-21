package io.minikafka.protocol;

import java.util.Arrays;

/** Client → Broker: publish a record to a topic partition, with an optional routing key. */
public record PublishReq(
    long correlationId, String topic, int partition, byte[] key, byte[] payload)
    implements Message {

  public PublishReq {
    if (topic == null) {
      throw new IllegalArgumentException("topic must not be null");
    }
    if (payload == null) {
      throw new IllegalArgumentException("payload must not be null");
    }
  }

  @Override
  public MessageType type() {
    return MessageType.PUBLISH_REQ;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    return o instanceof PublishReq p
        && correlationId == p.correlationId
        && partition == p.partition
        && topic.equals(p.topic)
        && Arrays.equals(key, p.key)
        && Arrays.equals(payload, p.payload);
  }

  @Override
  public int hashCode() {
    int result = Long.hashCode(correlationId);
    result = 31 * result + topic.hashCode();
    result = 31 * result + partition;
    result = 31 * result + Arrays.hashCode(key);
    result = 31 * result + Arrays.hashCode(payload);
    return result;
  }

  @Override
  public String toString() {
    return "PublishReq[correlationId="
        + correlationId
        + ", topic="
        + topic
        + ", partition="
        + partition
        + ", keyLen="
        + (key == null ? -1 : key.length)
        + ", payloadLen="
        + payload.length
        + "]";
  }
}
