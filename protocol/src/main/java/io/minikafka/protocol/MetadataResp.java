package io.minikafka.protocol;

import java.util.List;

/** Broker → Client: the set of brokers in the cluster (partition maps arrive in later specs). */
public record MetadataResp(long correlationId, List<BrokerInfo> brokers) implements Message {

  public MetadataResp {
    if (brokers == null) {
      throw new IllegalArgumentException("brokers must not be null");
    }
    brokers = List.copyOf(brokers);
  }

  @Override
  public MessageType type() {
    return MessageType.METADATA_RESP;
  }
}
