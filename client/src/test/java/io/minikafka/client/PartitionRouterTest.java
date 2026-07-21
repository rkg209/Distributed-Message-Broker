package io.minikafka.client;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.charset.StandardCharsets;
import java.util.HashSet;
import java.util.Set;
import org.junit.jupiter.api.Test;

/** AC-1 support: same key always routes to the same partition; round-robin spreads evenly. */
class PartitionRouterTest {

  private static final int PARTITIONS = 4;

  @Test
  void sameKeyAlwaysRoutesToSamePartition() {
    PartitionRouter router = new PartitionRouter();
    for (int k = 0; k < 10; k++) {
      byte[] key = ("key-" + k).getBytes(StandardCharsets.UTF_8);
      int first = router.route(key, PARTITIONS);
      for (int i = 0; i < 20; i++) {
        assertEquals(first, router.route(key, PARTITIONS));
      }
    }
  }

  @Test
  void tenKeysUseAllFourPartitions() {
    PartitionRouter router = new PartitionRouter();
    Set<Integer> usedPartitions = new HashSet<>();
    for (int k = 0; k < 10; k++) {
      byte[] key = ("key-" + k).getBytes(StandardCharsets.UTF_8);
      usedPartitions.add(router.route(key, PARTITIONS));
    }
    assertEquals(PARTITIONS, usedPartitions.size());
  }

  @Test
  void nullKeyRoundRobinsAcrossAllPartitions() {
    PartitionRouter router = new PartitionRouter();
    Set<Integer> seen = new HashSet<>();
    for (int i = 0; i < PARTITIONS * 3; i++) {
      seen.add(router.route(null, PARTITIONS));
    }
    assertEquals(PARTITIONS, seen.size());
  }

  @Test
  void nullKeyRoundRobinVisitsEachPartitionEquallyOverAFullCycle() {
    PartitionRouter router = new PartitionRouter();
    int[] counts = new int[PARTITIONS];
    for (int i = 0; i < PARTITIONS * 5; i++) {
      counts[router.route(null, PARTITIONS)]++;
    }
    for (int count : counts) {
      assertEquals(5, count);
    }
  }

  @Test
  void routeIsWithinPartitionRange() {
    PartitionRouter router = new PartitionRouter();
    for (int k = 0; k < 100; k++) {
      byte[] key = ("k" + k).getBytes(StandardCharsets.UTF_8);
      int p = router.route(key, PARTITIONS);
      assertTrue(p >= 0 && p < PARTITIONS);
    }
  }
}
