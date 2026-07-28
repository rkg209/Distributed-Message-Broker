package io.minikafka.chaos.demo;

import io.minikafka.chaos.HistoryRecorder.ConsumerEvent;
import io.minikafka.chaos.HistoryRecorder.History;
import io.minikafka.chaos.check.CheckResult;
import io.minikafka.chaos.check.Checker;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.TreeSet;

/**
 * The demo's literal "zero gaps" claim: for each partition, independently, the distinct offsets a
 * consumer observed must form an ascending run with no gap from the first offset seen. Neither
 * {@link io.minikafka.chaos.check.LossChecker} nor {@link
 * io.minikafka.chaos.check.DuplicationChecker} covers this — both key on producer (producerId,
 * seqNo) pairs, not on log offsets.
 */
public final class OffsetContiguityCheck implements Checker {

  @Override
  public CheckResult check(History history) {
    Map<Integer, TreeSet<Long>> offsetsByPartition = new TreeMap<>();
    for (ConsumerEvent e : history.consumerEvents()) {
      offsetsByPartition.computeIfAbsent(e.partition(), k -> new TreeSet<>()).add(e.offset());
    }

    List<CheckResult.Violation> violations = new ArrayList<>();
    long totalOffsets = 0;
    for (var entry : offsetsByPartition.entrySet()) {
      int partition = entry.getKey();
      TreeSet<Long> offsets = entry.getValue();
      totalOffsets += offsets.size();
      long expected = offsets.first();
      for (long offset : offsets) {
        if (offset != expected) {
          violations.add(
              new CheckResult.Violation(
                  "partition "
                      + partition
                      + " has a gap: expected offset "
                      + expected
                      + " but next observed offset was "
                      + offset));
          expected = offset;
        }
        expected++;
      }
    }

    CheckResult.Status status =
        violations.isEmpty() ? CheckResult.Status.PASS : CheckResult.Status.FAIL;
    String summary =
        offsetsByPartition.size()
            + " partitions, "
            + totalOffsets
            + " distinct offsets observed, "
            + violations.size()
            + " gaps";
    return new CheckResult("OffsetContiguityCheck", status, summary, violations);
  }
}
