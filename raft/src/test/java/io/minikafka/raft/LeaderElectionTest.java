package io.minikafka.raft;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.util.Set;
import org.junit.jupiter.api.Test;

/** AC-1: 3 nodes elect exactly one leader; safety property 1 (at most one leader per term). */
class LeaderElectionTest {

  @Test
  void electsExactlyOneLeaderWithinTwiceMaxTimeout() throws Exception {
    try (TestRaftCluster cluster = new TestRaftCluster(3)) {
      RaftNode leader =
          cluster.awaitLeader(
              Duration.ofMillis(
                  2 * TestRaftCluster.FAST_CONFIG_TEMPLATE.maxElectionTimeoutMs() + 2000));
      assertTrue(leader.currentTerm() > 0);

      // Sample repeatedly rather than once: at most one leader must ever be observed at a time
      // (the true safety property), and the cluster must settle back to exactly one.
      long settleDeadline = System.nanoTime() + Duration.ofSeconds(2).toNanos();
      long finalLeaderCount = 0;
      while (System.nanoTime() < settleDeadline) {
        long count = cluster.nodes().stream().filter(n -> n.role() == RaftRole.LEADER).count();
        assertTrue(count <= 1, "never more than one node may be LEADER at once, saw: " + count);
        finalLeaderCount = count;
        Thread.sleep(20);
      }
      assertEquals(1, finalLeaderCount, "cluster must settle with exactly one leader");

      for (Set<Integer> leadersInTerm : cluster.leadersByTerm().values()) {
        assertTrue(
            leadersInTerm.size() <= 1,
            "at most one node may believe itself leader in a given term, saw: " + leadersInTerm);
      }
    }
  }
}
