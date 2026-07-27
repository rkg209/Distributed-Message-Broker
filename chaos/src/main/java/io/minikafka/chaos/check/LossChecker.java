package io.minikafka.chaos.check;

import io.minikafka.chaos.HistoryRecorder.ConsumerEvent;
import io.minikafka.chaos.HistoryRecorder.History;
import io.minikafka.chaos.HistoryRecorder.ProducerEvent;
import io.minikafka.chaos.HistoryRecorder.ProducerOutcome;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * INV-1: a committed write is never lost. Every {@code ACKED} publish must be observed by some
 * consumer. {@code TIMEOUT} publishes have an unknown outcome — the producer never learned whether
 * the broker committed it — so their absence from the consumer view is not a violation, only a
 * counted statistic.
 */
public final class LossChecker implements Checker {

  private record ProducerKey(long producerId, long seqNo) {}

  @Override
  public CheckResult check(History history) {
    Set<ProducerKey> received = new HashSet<>();
    for (ConsumerEvent e : history.consumerEvents()) {
      received.add(new ProducerKey(e.producerId(), e.seqNo()));
    }

    List<CheckResult.Violation> violations = new ArrayList<>();
    long ackedCount = 0;
    long timeoutCount = 0;
    for (ProducerEvent e : history.producerEvents()) {
      if (e.outcome() == ProducerOutcome.ACKED) {
        ackedCount++;
        if (!received.contains(new ProducerKey(e.producerId(), e.seqNo()))) {
          violations.add(
              new CheckResult.Violation(
                  "acked (producerId="
                      + e.producerId()
                      + ", seqNo="
                      + e.seqNo()
                      + ") at offset "
                      + e.offset()
                      + " partition "
                      + e.partition()
                      + " was never received by any consumer"));
        }
      } else if (e.outcome() == ProducerOutcome.TIMEOUT) {
        timeoutCount++;
      }
    }

    CheckResult.Status status =
        violations.isEmpty() ? CheckResult.Status.PASS : CheckResult.Status.FAIL;
    String summary =
        ackedCount
            + " acked, "
            + timeoutCount
            + " timed out (excluded from loss check), "
            + violations.size()
            + " lost";
    return new CheckResult("LossChecker", status, summary, violations);
  }
}
