package io.minikafka.protocol;

/** Broker → Client: the offset assigned to a published record. */
public record PublishResp(long correlationId, long offset) implements Message {

  @Override
  public MessageType type() {
    return MessageType.PUBLISH_RESP;
  }
}
