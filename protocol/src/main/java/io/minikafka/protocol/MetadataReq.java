package io.minikafka.protocol;

/** Client → Broker: request cluster metadata (broker list, partition leadership). */
public record MetadataReq(long correlationId) implements Message {

  @Override
  public MessageType type() {
    return MessageType.METADATA_REQ;
  }
}
