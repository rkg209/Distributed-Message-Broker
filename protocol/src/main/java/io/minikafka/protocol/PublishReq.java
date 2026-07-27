package io.minikafka.protocol;

import java.util.Arrays;

/** Client → Broker: publish a record to a topic partition, with an optional routing key. */
public record PublishReq(
    long correlationId,
    String topic,
    int partition,
    long producerId,
    long seqNo,
    byte[] key,
    byte[] payload)
    implements Message {

  /** Sentinel producer id for non-idempotent publishes; bypasses dedup entirely. */
  public static final long NO_PRODUCER_ID = -1L;

  /** Sentinel sequence number paired with {@link #NO_PRODUCER_ID}. */
  public static final long NO_SEQ = -1L;

  public PublishReq {
    if (topic == null) {
      throw new IllegalArgumentException("topic must not be null");
    }
    if (payload == null) {
      throw new IllegalArgumentException("payload must not be null");
    }
    if (producerId != NO_PRODUCER_ID && seqNo < 0) {
      throw new IllegalArgumentException("seqNo must be >= 0 when producerId is set");
    }
    if (producerId == NO_PRODUCER_ID && seqNo != NO_SEQ) {
      throw new IllegalArgumentException("seqNo must be NO_SEQ when producerId is NO_PRODUCER_ID");
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
        && producerId == p.producerId
        && seqNo == p.seqNo
        && topic.equals(p.topic)
        && Arrays.equals(key, p.key)
        && Arrays.equals(payload, p.payload);
  }

  @Override
  public int hashCode() {
    int result = Long.hashCode(correlationId);
    result = 31 * result + topic.hashCode();
    result = 31 * result + partition;
    result = 31 * result + Long.hashCode(producerId);
    result = 31 * result + Long.hashCode(seqNo);
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
        + ", producerId="
        + producerId
        + ", seqNo="
        + seqNo
        + ", keyLen="
        + (key == null ? -1 : key.length)
        + ", payloadLen="
        + payload.length
        + "]";
  }
}
