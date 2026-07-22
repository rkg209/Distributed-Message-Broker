package io.minikafka.raft;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import org.junit.jupiter.api.Test;

/** AC-4: an isolated follower catches back up via {@code nextIndex} back-off once healed. */
class PartitionCatchUpTest {

  @Test
  void isolatedFollowerConvergesAfterHealing() throws Exception {
    try (TestRaftCluster cluster = new TestRaftCluster(3)) {
      RaftNode leader = cluster.awaitLeader(Duration.ofSeconds(3));
      int leaderId = leader.selfId();
      int isolated =
          cluster.nodes().stream()
              .map(RaftNode::selfId)
              .filter(id -> id != leaderId)
              .findFirst()
              .orElseThrow();
      int other =
          cluster.nodes().stream()
              .map(RaftNode::selfId)
              .filter(id -> id != leaderId && id != isolated)
              .findFirst()
              .orElseThrow();

      cluster.partition(leaderId, isolated);
      cluster.partition(other, isolated);

      for (int i = 0; i < 10; i++) {
        cluster.proposeOnLeader(
            ("cmd-" + i).getBytes(StandardCharsets.UTF_8), Duration.ofSeconds(3));
      }
      long commitIndex = cluster.awaitLeader(Duration.ofSeconds(3)).commitIndex();

      cluster.heal(leaderId, isolated);
      cluster.heal(other, isolated);

      RaftNode isolatedNode = cluster.node(isolated);
      cluster.awaitCommitIndex(isolatedNode, commitIndex, Duration.ofSeconds(5));

      List<CountingStateMachine.Applied> leaderApplied =
          cluster.stateMachineOf(leaderId).applied().stream()
              .filter(a -> a.index() <= commitIndex)
              .toList();
      List<CountingStateMachine.Applied> isolatedApplied =
          cluster.stateMachineOf(isolated).applied().stream()
              .filter(a -> a.index() <= commitIndex)
              .toList();

      assertEquals(leaderApplied.size(), isolatedApplied.size());
      for (int i = 0; i < leaderApplied.size(); i++) {
        assertEquals(leaderApplied.get(i).index(), isolatedApplied.get(i).index());
        assertArrayEquals(leaderApplied.get(i).command(), isolatedApplied.get(i).command());
      }
    }
  }
}
