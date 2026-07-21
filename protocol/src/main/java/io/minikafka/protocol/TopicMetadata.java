package io.minikafka.protocol;

import java.util.List;

/** A topic's known partitions, as advertised in a {@link MetadataResp}. */
public record TopicMetadata(String topic, List<PartitionMetadata> partitions) {

  public TopicMetadata {
    if (topic == null) {
      throw new IllegalArgumentException("topic must not be null");
    }
    if (partitions == null) {
      throw new IllegalArgumentException("partitions must not be null");
    }
    partitions = List.copyOf(partitions);
  }
}
