package io.minikafka.client;

import java.util.ArrayList;
import java.util.List;

/**
 * Static (non-rebalancing) partition-to-consumer assignment: contiguous ranges of partitions, with
 * any remainder spread one-per-member over the first members. Every partition is assigned to
 * exactly one member.
 */
public final class StaticAssignor {

  private StaticAssignor() {}

  /** The partitions assigned to member {@code memberIndex} of {@code memberCount} members. */
  public static List<Integer> assign(int memberIndex, int memberCount, int partitionCount) {
    if (memberCount <= 0) {
      throw new IllegalArgumentException("memberCount must be positive: " + memberCount);
    }
    if (memberIndex < 0 || memberIndex >= memberCount) {
      throw new IllegalArgumentException(
          "memberIndex " + memberIndex + " out of range [0, " + memberCount + ")");
    }
    if (partitionCount < 0) {
      throw new IllegalArgumentException("partitionCount must not be negative: " + partitionCount);
    }

    int base = partitionCount / memberCount;
    int remainder = partitionCount % memberCount;

    int start = memberIndex * base + Math.min(memberIndex, remainder);
    int count = base + (memberIndex < remainder ? 1 : 0);

    List<Integer> partitions = new ArrayList<>(count);
    for (int p = start; p < start + count; p++) {
      partitions.add(p);
    }
    return partitions;
  }
}
