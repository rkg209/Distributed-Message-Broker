package io.minikafka.raft;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import org.junit.jupiter.api.Test;

/**
 * INV-4: two leaders for the same partition can never both commit. A stale, isolated leader is
 * fenced by the term of the new leader elected by the majority, and steps down (without ever
 * advancing its own {@code commitIndex} past what it had before isolation) as soon as it hears from
 * the new term.
 */
class TermFencingTest {

  @Test
  void staleIsolatedLeaderStepsDownOnReconnect() throws Exception {
    try (TestRaftCluster cluster = new TestRaftCluster(3)) {
      RaftNode staleLeader = cluster.awaitStableLeader(Duration.ofSeconds(5));
      int staleLeaderId = staleLeader.selfId();

      int[] others =
          cluster.nodes().stream()
              .map(RaftNode::selfId)
              .filter(id -> id != staleLeaderId)
              .mapToInt(Integer::intValue)
              .toArray();

      cluster.partition(staleLeaderId, others[0]);
      cluster.partition(staleLeaderId, others[1]);
      // Let any in-flight replication settle so isolation is fully in effect before we snapshot.
      Thread.sleep(300);
      long commitBeforeIsolation = staleLeader.commitIndex();

      // The majority partition elects a new leader in a strictly higher term.
      RaftNode newLeader =
          cluster.awaitLeaderAmong(
              others,
              Duration.ofMillis(
                  2 * TestRaftCluster.FAST_CONFIG_TEMPLATE.maxElectionTimeoutMs() + 3000));
      assertNotEquals(staleLeaderId, newLeader.selfId());
      assertTrue(newLeader.currentTerm() > staleLeader.currentTerm());

      proposeAmong(cluster, others, "committed-by-new-leader".getBytes(), Duration.ofSeconds(3));

      // The stale leader, still isolated, must never have advanced its own commit index.
      assertEquals(commitBeforeIsolation, staleLeader.commitIndex());
      assertEquals(
          RaftRole.LEADER, staleLeader.role(), "still believes itself leader while isolated");

      cluster.heal(staleLeaderId, others[0]);
      cluster.heal(staleLeaderId, others[1]);

      awaitStepDown(staleLeader, Duration.ofSeconds(3));
      assertEquals(RaftRole.FOLLOWER, staleLeader.role());
      assertTrue(staleLeader.currentTerm() >= newLeader.currentTerm());
    }
  }

  /** Like {@link TestRaftCluster#proposeOnLeader}, but restricted to a known-reachable subset. */
  private static void proposeAmong(
      TestRaftCluster cluster, int[] candidateIds, byte[] command, Duration timeout)
      throws Exception {
    long deadline = System.nanoTime() + timeout.toNanos();
    while (System.nanoTime() < deadline) {
      RaftNode leader =
          cluster.awaitLeaderAmong(candidateIds, Duration.ofNanos(deadline - System.nanoTime()));
      try {
        leader.propose(command).get(2, java.util.concurrent.TimeUnit.SECONDS);
        return;
      } catch (java.util.concurrent.ExecutionException e) {
        if (!(e.getCause() instanceof NotLeaderException)) {
          throw e;
        }
      }
    }
    throw new AssertionError(
        "propose among " + java.util.Arrays.toString(candidateIds) + " never succeeded");
  }

  private static void awaitStepDown(RaftNode node, Duration timeout) throws InterruptedException {
    long deadline = System.nanoTime() + timeout.toNanos();
    while (System.nanoTime() < deadline) {
      if (node.role() != RaftRole.LEADER) {
        return;
      }
      Thread.sleep(10);
    }
    throw new AssertionError("node " + node.selfId() + " never stepped down within " + timeout);
  }
}
