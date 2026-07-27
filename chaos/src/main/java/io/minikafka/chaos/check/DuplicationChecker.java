package io.minikafka.chaos.check;

import io.minikafka.chaos.HistoryRecorder.ConsumerEvent;
import io.minikafka.chaos.HistoryRecorder.History;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

/**
 * INV-3: no duplicate broker-side append. A single logical message, identified by (producerId,
 * seqNo), must never occupy two distinct offsets in the same partition — that would mean the
 * idempotent producer's dedupe failed. Consumer-side redelivery of the *same* offset after an
 * offset-commit rewind is expected at-least-once behavior, not a violation of this invariant, and
 * is reported separately as an informational count.
 */
public final class DuplicationChecker implements Checker {

  private record ProducerKey(long producerId, long seqNo) {}

  private record OffsetKey(int partition, long offset) {}

  private record ConsumerOffsetKey(String consumerId, int partition, long offset) {}

  @Override
  public CheckResult check(History history) {
    Map<ProducerKey, Set<OffsetKey>> offsetsByProducerSeq =
        new TreeMap<>(
            (a, b) ->
                a.producerId() != b.producerId()
                    ? Long.compare(a.producerId(), b.producerId())
                    : Long.compare(a.seqNo(), b.seqNo()));
    Map<OffsetKey, Set<ProducerKey>> producersByOffset =
        new TreeMap<>(
            (a, b) ->
                a.partition() != b.partition()
                    ? Integer.compare(a.partition(), b.partition())
                    : Long.compare(a.offset(), b.offset()));

    for (ConsumerEvent e : history.consumerEvents()) {
      ProducerKey pk = new ProducerKey(e.producerId(), e.seqNo());
      OffsetKey ok = new OffsetKey(e.partition(), e.offset());
      offsetsByProducerSeq.computeIfAbsent(pk, k -> new LinkedHashSet<>()).add(ok);
      producersByOffset.computeIfAbsent(ok, k -> new LinkedHashSet<>()).add(pk);
    }

    List<CheckResult.Violation> violations = new ArrayList<>();
    for (var entry : offsetsByProducerSeq.entrySet()) {
      if (entry.getValue().size() > 1) {
        violations.add(
            new CheckResult.Violation(
                "(producerId="
                    + entry.getKey().producerId()
                    + ", seqNo="
                    + entry.getKey().seqNo()
                    + ") appeared at multiple distinct offsets: "
                    + entry.getValue()));
      }
    }
    for (var entry : producersByOffset.entrySet()) {
      if (entry.getValue().size() > 1) {
        violations.add(
            new CheckResult.Violation(
                "partition "
                    + entry.getKey().partition()
                    + " offset "
                    + entry.getKey().offset()
                    + " held distinct records: "
                    + entry.getValue()));
      }
    }

    Map<ConsumerOffsetKey, Integer> deliveryCounts = new java.util.HashMap<>();
    for (ConsumerEvent e : history.consumerEvents()) {
      deliveryCounts.merge(
          new ConsumerOffsetKey(e.consumerId(), e.partition(), e.offset()), 1, Integer::sum);
    }
    int redeliveries = 0;
    for (int count : deliveryCounts.values()) {
      if (count > 1) {
        redeliveries += count - 1;
      }
    }

    CheckResult.Status status =
        violations.isEmpty() ? CheckResult.Status.PASS : CheckResult.Status.FAIL;
    String summary =
        violations.size()
            + " broker-side duplicate appends, "
            + redeliveries
            + " at-least-once redeliveries (not violations)";
    return new CheckResult("DuplicationChecker", status, summary, violations);
  }
}
