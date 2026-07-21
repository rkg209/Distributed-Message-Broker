package io.minikafka.client;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * Resolves the target partition for a producer record. Routing is entirely producer-side: the
 * broker only ever receives an already-resolved partition id. A non-null key hashes
 * (Kafka-compatible murmur2) to a partition; a null key round-robins.
 */
public final class PartitionRouter {

  private final AtomicInteger roundRobinCounter = new AtomicInteger();

  /** Returns the partition {@code key} (or round-robin, if {@code key} is null) routes to. */
  public int route(byte[] key, int numPartitions) {
    if (numPartitions <= 0) {
      throw new IllegalArgumentException("numPartitions must be positive: " + numPartitions);
    }
    if (key == null) {
      return Math.floorMod(roundRobinCounter.getAndIncrement(), numPartitions);
    }
    return toPositive(murmur2(key)) % numPartitions;
  }

  private static int toPositive(int value) {
    return value & 0x7FFFFFFF;
  }

  /**
   * Kafka's murmur2 32-bit hash (MurmurHash2, seed {@code 0x9747b28c}), reproduced exactly so keyed
   * routing here matches Kafka's default partitioner byte-for-byte.
   */
  private static int murmur2(byte[] data) {
    int seed = 0x9747b28c;
    int m = 0x5bd1e995;
    int r = 24;

    int length = data.length;
    int h = seed ^ length;
    int length4 = length >> 2;

    for (int i = 0; i < length4; i++) {
      int i4 = i << 2;
      int k =
          (data[i4] & 0xff)
              | ((data[i4 + 1] & 0xff) << 8)
              | ((data[i4 + 2] & 0xff) << 16)
              | ((data[i4 + 3] & 0xff) << 24);
      k *= m;
      k ^= k >>> r;
      k *= m;
      h *= m;
      h ^= k;
    }

    int remainder = length & 3;
    int base = length4 << 2;
    if (remainder == 3) {
      h ^= (data[base + 2] & 0xff) << 16;
    }
    if (remainder >= 2) {
      h ^= (data[base + 1] & 0xff) << 8;
    }
    if (remainder >= 1) {
      h ^= data[base] & 0xff;
      h *= m;
    }

    h ^= h >>> 13;
    h *= m;
    h ^= h >>> 15;
    return h;
  }
}
