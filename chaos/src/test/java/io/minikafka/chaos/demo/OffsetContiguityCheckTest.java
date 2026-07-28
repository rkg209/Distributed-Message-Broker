package io.minikafka.chaos.demo;

import static org.junit.jupiter.api.Assertions.assertEquals;

import io.minikafka.chaos.HistoryRecorder;
import io.minikafka.chaos.check.CheckResult;
import org.junit.jupiter.api.Test;

class OffsetContiguityCheckTest {

  @Test
  void contiguousRunPasses() {
    HistoryRecorder recorder = new HistoryRecorder();
    recorder.recordConsumer("c0", 1, 0, 0, 0, 0, 1);
    recorder.recordConsumer("c0", 1, 1, 0, 1, 1, 2);
    recorder.recordConsumer("c0", 1, 2, 0, 2, 2, 3);

    CheckResult result = new OffsetContiguityCheck().check(recorder.snapshot());

    assertEquals(CheckResult.Status.PASS, result.status());
    assertEquals(0, result.violations().size());
  }

  @Test
  void injectedGapFails() {
    HistoryRecorder recorder = new HistoryRecorder();
    recorder.recordConsumer("c0", 1, 0, 0, 0, 0, 1);
    recorder.recordConsumer("c0", 1, 1, 0, 2, 1, 2); // skips offset 1

    CheckResult result = new OffsetContiguityCheck().check(recorder.snapshot());

    assertEquals(CheckResult.Status.FAIL, result.status());
    assertEquals(1, result.violations().size());
  }

  @Test
  void interleavedPartitionsAreIndependent() {
    HistoryRecorder recorder = new HistoryRecorder();
    // partition 0: contiguous
    recorder.recordConsumer("c0", 1, 0, 0, 0, 0, 1);
    recorder.recordConsumer("c0", 1, 1, 0, 1, 1, 2);
    // partition 1: has a gap
    recorder.recordConsumer("c1", 2, 0, 1, 0, 0, 1);
    recorder.recordConsumer("c1", 2, 1, 1, 5, 1, 2);

    CheckResult result = new OffsetContiguityCheck().check(recorder.snapshot());

    assertEquals(CheckResult.Status.FAIL, result.status());
    assertEquals(1, result.violations().size());
  }

  @Test
  void redeliveredOffsetIsNotAGap() {
    HistoryRecorder recorder = new HistoryRecorder();
    recorder.recordConsumer("c0", 1, 0, 0, 0, 0, 1);
    recorder.recordConsumer("c0", 1, 0, 0, 0, 1, 2); // redelivery of the same offset
    recorder.recordConsumer("c0", 1, 1, 0, 1, 2, 3);

    CheckResult result = new OffsetContiguityCheck().check(recorder.snapshot());

    assertEquals(CheckResult.Status.PASS, result.status());
  }

  @Test
  void emptyHistoryPasses() {
    HistoryRecorder.History empty = new HistoryRecorder().snapshot();
    CheckResult result = new OffsetContiguityCheck().check(empty);
    assertEquals(CheckResult.Status.PASS, result.status());
  }
}
