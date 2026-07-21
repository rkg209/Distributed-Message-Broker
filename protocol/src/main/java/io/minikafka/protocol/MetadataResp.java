package io.minikafka.protocol;

import java.util.List;

/** Broker → Client: the set of brokers in the cluster and known topic partition metadata. */
public record MetadataResp(long correlationId, List<BrokerInfo> brokers, List<TopicMetadata> topics)
    implements Message {

  public MetadataResp {
    if (brokers == null) {
      throw new IllegalArgumentException("brokers must not be null");
    }
    if (topics == null) {
      throw new IllegalArgumentException("topics must not be null");
    }
    brokers = List.copyOf(brokers);
    topics = List.copyOf(topics);
  }

  @Override
  public MessageType type() {
    return MessageType.METADATA_RESP;
  }
}
