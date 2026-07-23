package io.minikafka.broker;

import io.minikafka.raft.RaftRole;
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
    long deadline = System.nanoTime() + 20_000_000_000L;
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

  /**
   * Like {@link #awaitLeader}, but rejects {@code excludedBrokerId} — used after killing a leader
   * to confirm a genuinely *new* leader was elected, not a stale read of the old one.
   */
  static int awaitNewLeader(TestCluster cluster, TopicPartition tp, int excludedBrokerId)
      throws IOException {
    long deadline = System.nanoTime() + 20_000_000_000L;
    while (System.nanoTime() < deadline) {
      int leader = cluster.leaderOf(tp);
      if (leader != -1 && leader != excludedBrokerId) {
        sleep();
        int confirmed = cluster.leaderOf(tp);
        if (confirmed == leader) {
          return leader;
        }
      } else {
        sleep();
      }
    }
    throw new IOException(
        "No new leader (excluding " + excludedBrokerId + ") elected for " + tp + " within timeout");
  }

  /** Polls until {@code brokerId} reaches {@code expectedRole} for {@code tp}. */
  static void awaitRole(TestCluster cluster, int brokerId, TopicPartition tp, RaftRole expectedRole)
      throws IOException {
    long deadline = System.nanoTime() + 20_000_000_000L;
    while (System.nanoTime() < deadline) {
      if (cluster.roleOf(brokerId, tp) == expectedRole) {
        return;
      }
      sleep();
    }
    throw new IOException(
        "Broker "
            + brokerId
            + " did not reach role "
            + expectedRole
            + " for "
            + tp
            + " within timeout (was "
            + cluster.roleOf(brokerId, tp)
            + ")");
  }

  /**
   * Polls until {@code brokerId}'s leader epoch (Raft term) for {@code tp} reaches at least {@code
   * minTerm}. A restarted node's role is {@code FOLLOWER} from the instant it's constructed —
   * before it has heard from anyone — so {@link #awaitRole} alone can't confirm it actually
   * rejoined by contact with the new leader; this waits for the term bump that only a real {@code
   * AppendEntries}/{@code RequestVote} at the higher term produces.
   */
  static void awaitTermAtLeast(TestCluster cluster, int brokerId, TopicPartition tp, long minTerm)
      throws IOException {
    long deadline = System.nanoTime() + 20_000_000_000L;
    while (System.nanoTime() < deadline) {
      if (cluster.leaderEpochOf(brokerId, tp) >= minTerm) {
        return;
      }
      sleep();
    }
    throw new IOException(
        "Broker "
            + brokerId
            + "'s term for "
            + tp
            + " did not reach "
            + minTerm
            + " within timeout (was "
            + cluster.leaderEpochOf(brokerId, tp)
            + ")");
  }

  /** Polls until {@code brokerId}'s log for {@code tp} has at least {@code count} records. */
  static void awaitLogSize(TestCluster cluster, int brokerId, TopicPartition tp, long count)
      throws IOException {
    long deadline = System.nanoTime() + 20_000_000_000L;
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
