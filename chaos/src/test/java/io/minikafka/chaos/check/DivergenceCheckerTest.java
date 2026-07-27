package io.minikafka.chaos.check;

import static org.junit.jupiter.api.Assertions.assertEquals;

import io.minikafka.chaos.HistoryRecorder;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;

class DivergenceCheckerTest {

  /** An in-memory fake so the checker's comparison logic is testable without Docker. */
  private static final class FakeReplicaReader implements ReplicaReader {
    private final int brokerId;
    private final List<Entry> log;

    FakeReplicaReader(int brokerId, List<Entry> log) {
      this.brokerId = brokerId;
      this.log = log;
    }

    @Override
    public int brokerId() {
      return brokerId;
    }

    @Override
    public List<Entry> read(String topic, int partition, long fromOffset, int maxBytes) {
      List<Entry> batch = new ArrayList<>();
      for (Entry e : log) {
        if (e.offset() >= fromOffset) {
          batch.add(e);
        }
      }
      return batch;
    }
  }

  private static ReplicaReader.Entry entry(long offset, String payload) {
    return new ReplicaReader.Entry(offset, payload.getBytes());
  }

  @Test
  void passesWhenAllReplicasAgree() {
    ReplicaReader r1 = new FakeReplicaReader(1, List.of(entry(0, "a"), entry(1, "b")));
    ReplicaReader r2 = new FakeReplicaReader(2, List.of(entry(0, "a"), entry(1, "b")));

    DivergenceChecker checker = new DivergenceChecker(List.of(r1, r2), "t", 1, 1024);
    CheckResult result = checker.check(new HistoryRecorder().snapshot());

    assertEquals(CheckResult.Status.PASS, result.status());
  }

  @Test
  void lagIsNotDivergence() {
    ReplicaReader leader =
        new FakeReplicaReader(1, List.of(entry(0, "a"), entry(1, "b"), entry(2, "c")));
    ReplicaReader laggingFollower = new FakeReplicaReader(2, List.of(entry(0, "a"), entry(1, "b")));

    DivergenceChecker checker =
        new DivergenceChecker(List.of(leader, laggingFollower), "t", 1, 1024);
    CheckResult result = checker.check(new HistoryRecorder().snapshot());

    assertEquals(CheckResult.Status.PASS, result.status());
  }

  @Test
  void failsWhenTwoReplicasDisagreeAtTheSameOffset() {
    ReplicaReader r1 = new FakeReplicaReader(1, List.of(entry(0, "a"), entry(1, "b")));
    ReplicaReader r2 = new FakeReplicaReader(2, List.of(entry(0, "a"), entry(1, "DIFFERENT")));

    DivergenceChecker checker = new DivergenceChecker(List.of(r1, r2), "t", 1, 1024);
    CheckResult result = checker.check(new HistoryRecorder().snapshot());

    assertEquals(CheckResult.Status.FAIL, result.status());
    assertEquals(1, result.violations().size());
  }
}
