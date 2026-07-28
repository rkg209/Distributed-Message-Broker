package io.minikafka.broker;

import java.util.ArrayList;
import java.util.List;

/**
 * Contiguous-range partition assignment: base count plus remainder spread one-per-member over the
 * first members. Deliberately duplicated from {@code client/StaticAssignor} rather than shared —
 * the module map forbids {@code broker -> client}, and {@code protocol} is value-types-and-codec
 * only — so keep the two algorithms in lockstep if either changes.
 */
final class RangeAssignor {

  private RangeAssignor() {}

  /** The partitions assigned to member {@code memberIndex} of {@code memberCount} members. */
  static List<Integer> assign(int memberIndex, int memberCount, int partitionCount) {
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
