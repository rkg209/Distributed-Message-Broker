package io.minikafka.chaos.check;

import static org.junit.jupiter.api.Assertions.assertEquals;

import io.minikafka.chaos.HistoryRecorder;
import io.minikafka.chaos.HistoryRecorder.History;
import io.minikafka.chaos.HistoryRecorder.ProducerOutcome;
import org.junit.jupiter.api.Test;

class LossCheckerTest {

  @Test
  void passesWhenEveryAckedMessageWasReceived() {
    HistoryRecorder recorder = new HistoryRecorder();
    recorder.recordProducer(1, 0, 0, 0, 1, ProducerOutcome.ACKED, 0);
    recorder.recordProducer(1, 1, 0, 1, 2, ProducerOutcome.ACKED, 1);
    recorder.recordConsumer("c1", 1, 0, 0, 0, 3, 4);
    recorder.recordConsumer("c1", 1, 1, 0, 1, 4, 5);

    CheckResult result = new LossChecker().check(recorder.snapshot());

    assertEquals(CheckResult.Status.PASS, result.status());
    assertEquals(0, result.violations().size());
  }

  @Test
  void failsWhenAnAckedMessageWasNeverReceived() {
    HistoryRecorder recorder = new HistoryRecorder();
    recorder.recordProducer(1, 0, 0, 0, 1, ProducerOutcome.ACKED, 0);
    // never received by any consumer

    CheckResult result = new LossChecker().check(recorder.snapshot());

    assertEquals(CheckResult.Status.FAIL, result.status());
    assertEquals(1, result.violations().size());
  }

  @Test
  void timeoutMessagesDoNotCountAsLossEvenWhenUnreceived() {
    HistoryRecorder recorder = new HistoryRecorder();
    recorder.recordProducer(1, 0, 0, 0, 1, ProducerOutcome.TIMEOUT, -1);

    CheckResult result = new LossChecker().check(recorder.snapshot());

    assertEquals(CheckResult.Status.PASS, result.status());
  }

  @Test
  void emptyHistoryPasses() {
    History empty = new HistoryRecorder().snapshot();
    CheckResult result = new LossChecker().check(empty);
    assertEquals(CheckResult.Status.PASS, result.status());
  }
}
