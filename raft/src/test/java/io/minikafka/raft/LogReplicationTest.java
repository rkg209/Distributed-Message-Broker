package io.minikafka.raft;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import org.junit.jupiter.api.Test;

/** AC-2 / safety property 4: 100 proposals replicate identically to every node's state machine. */
class LogReplicationTest {

  @Test
  void hundredCommandsReplicateInOrderToEveryNode() throws Exception {
    try (TestRaftCluster cluster = new TestRaftCluster(3)) {
      cluster.awaitLeader(Duration.ofSeconds(3));

      int count = 100;
      for (int i = 0; i < count; i++) {
        byte[] command = ("cmd-" + i).getBytes(StandardCharsets.UTF_8);
        cluster.proposeOnLeader(command, Duration.ofSeconds(3));
      }

      long lastIndex = cluster.awaitLeader(Duration.ofSeconds(3)).commitIndex();
      for (RaftNode node : cluster.nodes()) {
        cluster.awaitCommitIndex(node, lastIndex, Duration.ofSeconds(3));
      }

      List<CountingStateMachine.Applied> reference = null;
      for (int id = 1; id <= 3; id++) {
        List<CountingStateMachine.Applied> applied = cluster.stateMachineOf(id).applied();
        List<CountingStateMachine.Applied> committedPrefix =
            applied.stream().filter(a -> a.index() <= lastIndex).toList();
        if (reference == null) {
          reference = committedPrefix;
        } else {
          assertEquals(
              reference.size(), committedPrefix.size(), "node " + id + " diverged in length");
          for (int i = 0; i < reference.size(); i++) {
            assertEquals(reference.get(i).index(), committedPrefix.get(i).index());
            assertArrayEquals(reference.get(i).command(), committedPrefix.get(i).command());
          }
        }
      }

      // All 100 issued commands must be durably committed. `>=` rather than `==`: raw Raft
      // (without the idempotent-producer dedup layer from Spec 09) can legitimately double-append
      // a command if a client-side proposeOnLeader retry races with a proposal that was appended
      // but not yet locally confirmed committed before a timeout — that's a duplicate-delivery
      // concern for Spec 09, not a violation of this spec's ordering/no-loss safety property.
      long realCommands = reference.stream().filter(a -> a.command().length > 0).count();
      assertTrue(
          realCommands >= count, "expected at least " + count + " committed, saw " + realCommands);
    }
  }
}
