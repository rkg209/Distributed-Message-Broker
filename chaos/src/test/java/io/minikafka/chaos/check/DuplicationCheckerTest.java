package io.minikafka.chaos.check;

import static org.junit.jupiter.api.Assertions.assertEquals;

import io.minikafka.chaos.HistoryRecorder;
import io.minikafka.chaos.HistoryRecorder.ProducerOutcome;
import org.junit.jupiter.api.Test;

class DuplicationCheckerTest {

  @Test
  void passesWithNoDuplicates() {
    HistoryRecorder recorder = new HistoryRecorder();
    recorder.recordProducer(1, 0, 0, 0, 1, ProducerOutcome.ACKED, 0);
    recorder.recordProducer(1, 1, 0, 1, 2, ProducerOutcome.ACKED, 1);
    recorder.recordConsumer("c1", 1, 0, 0, 0, 3, 4);
    recorder.recordConsumer("c1", 1, 1, 0, 1, 4, 5);

    CheckResult result = new DuplicationChecker().check(recorder.snapshot());

    assertEquals(CheckResult.Status.PASS, result.status());
  }

  @Test
  void failsWhenSameProducerSeqAppearsAtTwoOffsets() {
    HistoryRecorder recorder = new HistoryRecorder();
    recorder.recordConsumer("c1", 1, 0, 0, 5, 0, 1);
    recorder.recordConsumer("c1", 1, 0, 0, 9, 2, 3); // same (producerId,seqNo), different offset

    CheckResult result = new DuplicationChecker().check(recorder.snapshot());

    assertEquals(CheckResult.Status.FAIL, result.status());
    assertEquals(1, result.violations().size());
  }

  @Test
  void failsWhenOneOffsetHoldsTwoDistinctRecords() {
    HistoryRecorder recorder = new HistoryRecorder();
    recorder.recordConsumer("c1", 1, 0, 0, 5, 0, 1);
    recorder.recordConsumer("c1", 2, 0, 0, 5, 2, 3); // same offset, different producer/seq

    CheckResult result = new DuplicationChecker().check(recorder.snapshot());

    assertEquals(CheckResult.Status.FAIL, result.status());
  }

  @Test
  void redeliveryOfSameOffsetToSameConsumerIsNotAViolation() {
    HistoryRecorder recorder = new HistoryRecorder();
    recorder.recordConsumer("c1", 1, 0, 0, 5, 0, 1);
    recorder.recordConsumer("c1", 1, 0, 0, 5, 2, 3); // redelivery after rewind: same everything

    CheckResult result = new DuplicationChecker().check(recorder.snapshot());

    assertEquals(CheckResult.Status.PASS, result.status());
    assertEquals(0, result.violations().size());
  }
}
