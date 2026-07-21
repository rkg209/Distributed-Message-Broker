package io.minikafka.broker;

import io.minikafka.protocol.BrokerInfo;
import io.minikafka.protocol.PartitionMetadata;
import io.minikafka.protocol.TopicMetadata;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListSet;

/**
 * Tracks this broker's identity, the partition counts of topics known to it (either configured up
 * front via {@link TopicConfig} or discovered when first touched by a publish/poll), and the static
 * cluster membership/assignment from {@link ClusterConfig}. A partition with no configured
 * assignment falls back to {@code (self, [self])} — today's single-broker behaviour, and what every
 * Spec 02-04 test still exercises.
 */
public final class MetadataService {

  private final BrokerInfo self;
  private final TopicConfig topicConfig;
  private final ClusterConfig clusterConfig;
  private final Set<String> knownTopics = new ConcurrentSkipListSet<>();

  public MetadataService(BrokerInfo self, TopicConfig topicConfig) {
    this(self, topicConfig, ClusterConfig.singleBroker(self));
  }

  public MetadataService(BrokerInfo self, TopicConfig topicConfig, ClusterConfig clusterConfig) {
    this.self = self;
    this.topicConfig = topicConfig;
    this.clusterConfig = clusterConfig;
    knownTopics.addAll(topicConfig.partitionCounts().keySet());
  }

  /** The partition count for {@code topic}: configured, or {@code defaultPartitions}. */
  public int partitionCountFor(String topic) {
    return topicConfig.partitionCountFor(topic);
  }

  /** Records that {@code topic} has been touched by a publish/poll, so it appears in metadata. */
  public void markTouched(String topic) {
    knownTopics.add(topic);
  }

  /** This broker's identity. */
  public BrokerInfo self() {
    return self;
  }

  /** Every broker in the static cluster configuration. */
  public List<BrokerInfo> clusterBrokers() {
    return clusterConfig.brokers();
  }

  /** Whether this broker is the static controller. */
  public boolean isController() {
    return clusterConfig.isController(self.brokerId());
  }

  /**
   * Describes every known topic and its partitions, with leader/replicas resolved from {@link
   * ClusterConfig}; an unassigned partition defaults to this broker as sole leader and replica.
   */
  public List<TopicMetadata> describeTopics() {
    List<TopicMetadata> topics = new ArrayList<>(knownTopics.size());
    for (String topic : knownTopics) {
      int partitionCount = topicConfig.partitionCountFor(topic);
      List<PartitionMetadata> partitions = new ArrayList<>(partitionCount);
      for (int p = 0; p < partitionCount; p++) {
        TopicPartition tp = new TopicPartition(topic, p);
        var assignment = clusterConfig.assignmentFor(tp);
        if (assignment.isPresent()) {
          partitions.add(
              new PartitionMetadata(p, assignment.get().leaderId(), assignment.get().replicaIds()));
        } else {
          partitions.add(new PartitionMetadata(p, self.brokerId(), List.of(self.brokerId())));
        }
      }
      topics.add(new TopicMetadata(topic, partitions));
    }
    return topics;
  }
}
