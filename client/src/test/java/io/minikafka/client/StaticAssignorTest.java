package io.minikafka.client;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.junit.jupiter.api.Test;

class StaticAssignorTest {

  @Test
  void unionOfAllMembersCoversEveryPartitionExactlyOnce() {
    int memberCount = 3;
    int partitionCount = 8;
    List<Integer> all = new ArrayList<>();
    for (int m = 0; m < memberCount; m++) {
      all.addAll(StaticAssignor.assign(m, memberCount, partitionCount));
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
      int size = StaticAssignor.assign(m, memberCount, partitionCount).size();
      min = Math.min(min, size);
      max = Math.max(max, size);
    }
    assertTrue(max - min <= 1);
  }

  @Test
  void evenSplitGivesContiguousRanges() {
    assertEquals(List.of(0, 1), StaticAssignor.assign(0, 2, 4));
    assertEquals(List.of(2, 3), StaticAssignor.assign(1, 2, 4));
  }

  @Test
  void singleMemberGetsEveryPartition() {
    assertEquals(List.of(0, 1, 2, 3), StaticAssignor.assign(0, 1, 4));
  }
}
