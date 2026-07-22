package io.minikafka.raft;

import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import org.junit.jupiter.api.Test;

/** AC-3 / INV-1 / safety property 3: a leader crash mid-flight never loses a committed entry. */
class LeaderCrashTest {

  @Test
  void crashedLeaderIsReplacedAndCommittedEntriesSurvive() throws Exception {
    try (TestRaftCluster cluster = new TestRaftCluster(3)) {
      RaftNode firstLeader = cluster.awaitLeader(Duration.ofSeconds(3));
      int firstLeaderId = firstLeader.selfId();

      cluster.proposeOnLeader(
          "before-crash".getBytes(StandardCharsets.UTF_8), Duration.ofSeconds(3));
      long committedBeforeCrash = cluster.awaitLeader(Duration.ofSeconds(3)).commitIndex();
      for (RaftNode node : cluster.nodes()) {
        cluster.awaitCommitIndex(node, committedBeforeCrash, Duration.ofSeconds(3));
      }

      cluster.crash(firstLeaderId);

      RaftNode newLeader =
          cluster.awaitLeader(
              Duration.ofMillis(
                  2 * TestRaftCluster.FAST_CONFIG_TEMPLATE.maxElectionTimeoutMs() + 3000));
      assertNotEquals(firstLeaderId, newLeader.selfId(), "a new node must take over");

      assertTrue(newLeader.commitIndex() >= committedBeforeCrash);

      cluster.proposeOnLeader(
          "after-crash".getBytes(StandardCharsets.UTF_8), Duration.ofSeconds(3));
      long finalCommit = cluster.awaitLeader(Duration.ofSeconds(3)).commitIndex();
      for (RaftNode node : cluster.nodes()) {
        if (node.selfId() == firstLeaderId) {
          continue;
        }
        cluster.awaitCommitIndex(node, finalCommit, Duration.ofSeconds(3));
      }

      int survivorId =
          cluster.nodes().stream()
              .map(RaftNode::selfId)
              .filter(id -> id != firstLeaderId)
              .findFirst()
              .orElseThrow();

      boolean sawBeforeCrash =
          cluster.stateMachineOf(survivorId).applied().stream()
              .anyMatch(
                  a -> new String(a.command(), StandardCharsets.UTF_8).equals("before-crash"));
      assertTrue(sawBeforeCrash, "pre-crash committed entry must survive on the new leader");

      boolean sawAfterCrash =
          cluster.stateMachineOf(survivorId).applied().stream()
              .anyMatch(a -> new String(a.command(), StandardCharsets.UTF_8).equals("after-crash"));
      assertTrue(sawAfterCrash, "post-crash entry must be replicated");
    }
  }
}
