package io.minikafka.protocol;

import java.util.List;

/** One partition of a topic: the broker id currently leading it, and its full replica set. */
public record PartitionMetadata(int partitionId, int leaderId, List<Integer> replicaIds) {

  public PartitionMetadata {
    if (replicaIds == null) {
      throw new IllegalArgumentException("replicaIds must not be null");
    }
    replicaIds = List.copyOf(replicaIds);
  }
}
