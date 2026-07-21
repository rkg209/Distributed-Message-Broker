package io.minikafka.protocol;

/** Broker → Client: a consumer group's last committed offset, or {@link #NO_OFFSET}. */
public record FetchOffsetResp(long correlationId, long offset) implements Message {

  /** Sentinel meaning the group has never committed an offset for this topic partition. */
  public static final long NO_OFFSET = -1L;

  @Override
  public MessageType type() {
    return MessageType.FETCH_OFFSET_RESP;
  }
}
