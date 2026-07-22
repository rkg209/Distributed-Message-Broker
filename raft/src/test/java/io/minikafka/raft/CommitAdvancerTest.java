package io.minikafka.raft;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * Safety property 5 (Raft §5.4.2, "Figure 8"): a leader must not commit an entry replicated from a
 * previous term by majority match alone — only once an entry from its own current term also reaches
 * a majority does the previous-term entry (and everything before it) become committed.
 */
class CommitAdvancerTest {

  @Test
  void doesNotCommitPreviousTermEntryOnMajorityMatchAlone() {
    InMemoryRaftLogStore log = new InMemoryRaftLogStore();
    log.append(new RaftEntry(1, 1, new byte[0]));
    log.append(
        new RaftEntry(2, 2, new byte[0])); // replicated to a majority, but term 2 != currentTerm 3

    long currentTerm = 3;
    // matchIndex: leader(self)=2, peer1=2, peer2=0 — majority (2 of 3) have index 2.
    long result = CommitAdvancer.advance(currentTerm, List.of(2L, 2L, 0L), log, 0);

    assertEquals(0, result, "must not commit index 2: its term (2) != currentTerm (3)");
  }

  @Test
  void commitsUpToAndIncludingCurrentTermEntryOnceMajorityReachesIt() {
    InMemoryRaftLogStore log = new InMemoryRaftLogStore();
    log.append(new RaftEntry(1, 1, new byte[0]));
    log.append(new RaftEntry(2, 2, new byte[0]));
    log.append(new RaftEntry(3, 3, new byte[0])); // leader's own no-op, current term

    long currentTerm = 3;
    long result = CommitAdvancer.advance(currentTerm, List.of(3L, 3L, 0L), log, 0);

    assertEquals(3, result, "index 3 has currentTerm and a majority match — must commit");
  }

  @Test
  void doesNotRegressBelowCurrentCommitIndex() {
    InMemoryRaftLogStore log = new InMemoryRaftLogStore();
    log.append(new RaftEntry(1, 1, new byte[0]));

    long result = CommitAdvancer.advance(1, List.of(0L, 0L, 0L), log, 1);

    assertEquals(1, result);
  }
}
