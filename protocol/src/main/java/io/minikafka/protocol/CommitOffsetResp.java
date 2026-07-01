package io.minikafka.protocol;

/** Broker → Client: acknowledges a consumer offset commit. */
public record CommitOffsetResp(long correlationId, boolean ok) implements Message {

  @Override
  public MessageType type() {
    return MessageType.COMMIT_OFFSET_RESP;
  }
}
