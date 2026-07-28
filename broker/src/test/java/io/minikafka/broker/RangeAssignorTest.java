package io.minikafka.broker;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.junit.jupiter.api.Test;

class RangeAssignorTest {

  @Test
  void unionOfAllMembersCoversEveryPartitionExactlyOnce() {
    int memberCount = 3;
    int partitionCount = 8;
    List<Integer> all = new ArrayList<>();
    for (int m = 0; m < memberCount; m++) {
      all.addAll(RangeAssignor.assign(m, memberCount, partitionCount));
    }
    Set<Integer> unique = new HashSet<>(all);
    assertEquals(partitionCount, all.size());
    assertEquals(partitionCount, unique.size());
    for (int p = 0; p < partitionCount; p++) {
      assertTrue(unique.contains(p));
    }
  }

  @Test
  void assignmentsAreBalancedWithinOne() {
    int memberCount = 3;
    int partitionCount = 8;
    int min = Integer.MAX_VALUE;
    int max = Integer.MIN_VALUE;
    for (int m = 0; m < memberCount; m++) {
      int size = RangeAssignor.assign(m, memberCount, partitionCount).size();
      min = Math.min(min, size);
      max = Math.max(max, size);
    }
    assertTrue(max - min <= 1);
  }

  /** AC1: 4 partitions / 3 members splits {@code [2, 1, 1]}. */
  @Test
  void fourPartitionsThreeMembersSplitsTwoOneOne() {
    assertEquals(List.of(0, 1), RangeAssignor.assign(0, 3, 4));
    assertEquals(List.of(2), RangeAssignor.assign(1, 3, 4));
    assertEquals(List.of(3), RangeAssignor.assign(2, 3, 4));
  }

  /** AC2: 4 partitions / 2 members splits {@code [2, 2]}. */
  @Test
  void fourPartitionsTwoMembersSplitsTwoTwo() {
    assertEquals(List.of(0, 1), RangeAssignor.assign(0, 2, 4));
    assertEquals(List.of(2, 3), RangeAssignor.assign(1, 2, 4));
  }

  @Test
  void singleMemberGetsEveryPartition() {
    assertEquals(List.of(0, 1, 2, 3), RangeAssignor.assign(0, 1, 4));
  }
}
