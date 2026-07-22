package io.minikafka.broker;

import java.io.IOException;

/** Small polling helpers shared by Spec 07 replication tests. */
final class RaftTestSupport {

  private RaftTestSupport() {}

  /**
   * Polls until some broker in {@code cluster} believes itself leader for {@code tp}, then confirms
   * that broker is still leader a beat later — a just-elected leader in a multi-node group can
   * still be deposed by a higher-term vote from a candidate whose own election timer fired
   * concurrently, before the new leader's first heartbeat reaches it.
   */
  static int awaitLeader(TestCluster cluster, TopicPartition tp) throws IOException {
    long deadline = System.nanoTime() + 10_000_000_000L;
    while (System.nanoTime() < deadline) {
      int leader = cluster.leaderOf(tp);
      if (leader != -1) {
        sleep();
        if (cluster.leaderOf(tp) == leader) {
          return leader;
        }
      } else {
        sleep();
      }
    }
    throw new IOException("No stable leader elected for " + tp + " within timeout");
  }

  /** Polls until {@code brokerId}'s log for {@code tp} has at least {@code count} records. */
  static void awaitLogSize(TestCluster cluster, int brokerId, TopicPartition tp, long count)
      throws IOException {
    long deadline = System.nanoTime() + 10_000_000_000L;
    while (System.nanoTime() < deadline) {
      var log = cluster.partitionLogOf(brokerId, tp);
      if (log != null && log.nextOffset() >= count) {
        return;
      }
      sleep();
    }
    throw new IOException(
        "Broker " + brokerId + "'s log for " + tp + " did not reach " + count + " records");
  }

  private static void sleep() throws IOException {
    try {
      Thread.sleep(20);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IOException(e);
    }
  }
}
