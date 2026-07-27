package io.minikafka.chaos.check;

import static org.junit.jupiter.api.Assertions.assertEquals;

import io.minikafka.chaos.HistoryRecorder;
import io.minikafka.chaos.HistoryRecorder.ProducerOutcome;
import org.junit.jupiter.api.Test;

class LinearizabilityCheckerTest {

  private static final long AMPLE_BUDGET = 100_000;

  @Test
  void passesOnAKnownLinearizableHistory() {
    HistoryRecorder recorder = new HistoryRecorder();
    // Two acked appends, strictly ordered in both real time and offset, each observed by a read
    // matching exactly what was written.
    recorder.recordProducer(1, 0, 0, 0, 10, ProducerOutcome.ACKED, 0);
    recorder.recordProducer(1, 1, 0, 20, 30, ProducerOutcome.ACKED, 1);
    recorder.recordConsumer("c1", 1, 0, 0, 0, 40, 41);
    recorder.recordConsumer("c1", 1, 1, 0, 1, 42, 43);

    CheckResult result = new LinearizabilityChecker(AMPLE_BUDGET).check(recorder.snapshot());

    assertEquals(CheckResult.Status.PASS, result.status(), result.violations().toString());
  }

  @Test
  void failsOnALostWrite() {
    HistoryRecorder recorder = new HistoryRecorder();
    // offset 1 completes (in real time) long before offset 0 even starts: no valid linearization
    // can place offset 0 first as the append order requires, since offset 1 must precede it in
    // real time yet must be linearized after it by the offset ordering — a contradiction.
    recorder.recordProducer(9, 0, 0, 0, 10, ProducerOutcome.ACKED, 1);
    recorder.recordProducer(9, 1, 0, 20, 30, ProducerOutcome.ACKED, 0);

    CheckResult result = new LinearizabilityChecker(AMPLE_BUDGET).check(recorder.snapshot());

    assertEquals(CheckResult.Status.FAIL, result.status());
  }

  @Test
  void failsOnAStaleRead() {
    HistoryRecorder recorder = new HistoryRecorder();
    // offset 0 is actually (producerId=1, seqNo=0), but a consumer reports a different record at
    // that same offset — a read that could never have been produced by any valid linearization.
    recorder.recordProducer(1, 0, 0, 0, 10, ProducerOutcome.ACKED, 0);
    recorder.recordConsumer("c1", 2, 0, 0, 0, 20, 21);

    CheckResult result = new LinearizabilityChecker(AMPLE_BUDGET).check(recorder.snapshot());

    assertEquals(CheckResult.Status.FAIL, result.status());
  }

  @Test
  void failsOnADuplicateOffsetCommit() {
    HistoryRecorder recorder = new HistoryRecorder();
    // Two distinct producers both acked at offset 0 — a broker-side corruption that cannot be
    // explained by any total order of a single append-only register.
    recorder.recordProducer(1, 0, 0, 0, 10, ProducerOutcome.ACKED, 0);
    recorder.recordProducer(2, 0, 0, 0, 10, ProducerOutcome.ACKED, 0);

    CheckResult result = new LinearizabilityChecker(AMPLE_BUDGET).check(recorder.snapshot());

    assertEquals(CheckResult.Status.FAIL, result.status());
  }

  @Test
  void reportsUnknownRatherThanASilentPassWhenBudgetIsExhausted() {
    HistoryRecorder recorder = new HistoryRecorder();
    recorder.recordProducer(1, 0, 0, 0, 10, ProducerOutcome.ACKED, 0);
    recorder.recordProducer(1, 1, 0, 20, 30, ProducerOutcome.ACKED, 1);
    recorder.recordConsumer("c1", 1, 0, 0, 0, 40, 41);
    recorder.recordConsumer("c1", 1, 1, 0, 1, 42, 43);

    CheckResult result = new LinearizabilityChecker(1).check(recorder.snapshot());

    assertEquals(CheckResult.Status.UNKNOWN, result.status());
  }
}
