package io.minikafka.broker;

import io.minikafka.protocol.BrokerInfo;
import io.minikafka.protocol.PartitionMetadata;
import io.minikafka.protocol.TopicMetadata;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
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
  private final Map<TopicPartition, Integer> liveLeaders = new ConcurrentHashMap<>();

  // Set once via attachPartitionManager: PartitionManager's constructor takes a MetadataService,
  // so the two objects have a construction-order cycle that a constructor parameter can't express.
  private volatile PartitionManager partitionManager;

  public MetadataService(BrokerInfo self, TopicConfig topicConfig) {
    this(self, topicConfig, ClusterConfig.singleBroker(self));
  }

  public MetadataService(BrokerInfo self, TopicConfig topicConfig, ClusterConfig clusterConfig) {
    this.self = self;
    this.topicConfig = topicConfig;
    this.clusterConfig = clusterConfig;
    knownTopics.addAll(topicConfig.partitionCounts().keySet());
  }

  /** Wires in the {@link PartitionManager} so {@link #describeTopics()} can report live leaders. */
  public void attachPartitionManager(PartitionManager partitionManager) {
    this.partitionManager = partitionManager;
  }

  /** The partition count for {@code topic}: configured, or {@code defaultPartitions}. */
  public int partitionCountFor(String topic) {
    return topicConfig.partitionCountFor(topic);
  }

  /** Records that {@code topic} has been touched by a publish/poll, so it appears in metadata. */
  public void markTouched(String topic) {
    knownTopics.add(topic);
  }

  /**
   * Records a Raft leadership change for {@code tp} pushed from a locally hosted {@link
   * PartitionReplica}, so {@link #describePartition} reflects the new leader immediately rather
   * than on next poll. {@code leaderId == PersistentState.NONE} (a step-down) clears the entry,
   * falling back to the existing replica/assignment/self chain.
   */
  public void onLeadershipChange(TopicPartition tp, int leaderId, long epoch) {
    if (leaderId == io.minikafka.raft.PersistentState.NONE) {
      liveLeaders.remove(tp);
    } else {
      liveLeaders.put(tp, leaderId);
    }
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
        partitions.add(describePartition(topic, p));
      }
      topics.add(new TopicMetadata(topic, partitions));
    }
    return topics;
  }

  private PartitionMetadata describePartition(String topic, int p) {
    TopicPartition tp = new TopicPartition(topic, p);
    var assignment = clusterConfig.assignmentFor(tp);
    List<Integer> replicaIds =
        assignment
            .map(ClusterConfig.PartitionAssignment::replicaIds)
            .orElse(List.of(self.brokerId()));

    Integer pushedLeader = liveLeaders.get(tp);
    if (pushedLeader != null) {
      return new PartitionMetadata(p, pushedLeader, replicaIds);
    }

    PartitionManager pm = partitionManager;
    if (pm != null) {
      PartitionReplica replica = pm.replica(tp);
      if (replica != null) {
        int liveLeader = replica.leaderId();
        if (liveLeader != io.minikafka.raft.PersistentState.NONE) {
          return new PartitionMetadata(p, liveLeader, replicaIds);
        }
      }
    }

    int fallbackLeader =
        assignment.map(ClusterConfig.PartitionAssignment::leaderId).orElse(self.brokerId());
    return new PartitionMetadata(p, fallbackLeader, replicaIds);
  }
}
