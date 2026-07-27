package io.minikafka.chaos.check;

import io.minikafka.chaos.HistoryRecorder.History;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * INV-4/INV-5: two leaders for the same partition can never both commit, and consumers only ever
 * read committed (majority-replicated) data — so every replica's locally-applied log, read
 * independently via {@link ReplicaReader}, must agree at every offset both replicas hold. A replica
 * that simply has fewer records is lagging (it hasn't yet applied everything the majority has
 * committed) and is reported separately, never as a violation; only two replicas disagreeing on the
 * payload at the *same* offset is a true divergence.
 */
public final class DivergenceChecker implements Checker {

  private final List<ReplicaReader> replicas;
  private final String topic;
  private final int partitions;
  private final int maxBytesPerRead;

  public DivergenceChecker(
      List<ReplicaReader> replicas, String topic, int partitions, int maxBytesPerRead) {
    if (replicas.size() < 2) {
      throw new IllegalArgumentException("need at least 2 replicas to check divergence");
    }
    this.replicas = List.copyOf(replicas);
    this.topic = topic;
    this.partitions = partitions;
    this.maxBytesPerRead = maxBytesPerRead;
  }

  @Override
  public CheckResult check(History history) {
    List<CheckResult.Violation> violations = new ArrayList<>();
    List<String> lagNotes = new ArrayList<>();

    for (int partition = 0; partition < partitions; partition++) {
      Map<Integer, List<ReplicaReader.Entry>> byBroker = new LinkedHashMap<>();
      for (ReplicaReader replica : replicas) {
        byBroker.put(replica.brokerId(), readAll(replica, partition));
      }

      int minLen = byBroker.values().stream().mapToInt(List::size).min().orElse(0);
      int maxLen = byBroker.values().stream().mapToInt(List::size).max().orElse(0);
      if (minLen != maxLen) {
        lagNotes.add(
            "partition "
                + partition
                + ": replica lengths range "
                + minLen
                + ".."
                + maxLen
                + " (lag, not divergence)");
      }

      for (int offset = 0; offset < minLen; offset++) {
        Integer firstBroker = null;
        byte[] firstPayload = null;
        for (var entry : byBroker.entrySet()) {
          byte[] payload = entry.getValue().get(offset).payload();
          if (firstPayload == null) {
            firstBroker = entry.getKey();
            firstPayload = payload;
          } else if (!Arrays.equals(firstPayload, payload)) {
            violations.add(
                new CheckResult.Violation(
                    "partition "
                        + partition
                        + " offset "
                        + offset
                        + ": broker "
                        + firstBroker
                        + " and broker "
                        + entry.getKey()
                        + " hold different payloads"));
          }
        }
      }
    }

    CheckResult.Status status =
        violations.isEmpty() ? CheckResult.Status.PASS : CheckResult.Status.FAIL;
    String summary =
        violations.isEmpty()
            ? "no divergence across "
                + replicas.size()
                + " replicas"
                + (lagNotes.isEmpty() ? "" : "; " + lagNotes.size() + " lag note(s)")
            : violations.size() + " divergent offset(s) found";
    return new CheckResult("DivergenceChecker", status, summary, violations);
  }

  private List<ReplicaReader.Entry> readAll(ReplicaReader replica, int partition) {
    List<ReplicaReader.Entry> all = new ArrayList<>();
    long nextOffset = 0;
    while (true) {
      List<ReplicaReader.Entry> batch;
      try {
        batch = replica.read(topic, partition, nextOffset, maxBytesPerRead);
      } catch (IOException e) {
        throw new UncheckedIOException(
            "failed reading partition " + partition + " from broker " + replica.brokerId(), e);
      }
      if (batch.isEmpty()) {
        return all;
      }
      all.addAll(batch);
      nextOffset = batch.get(batch.size() - 1).offset() + 1;
    }
  }
}
