package io.minikafka.chaos.check;

import io.minikafka.chaos.HistoryRecorder.ConsumerEvent;
import io.minikafka.chaos.HistoryRecorder.History;
import io.minikafka.chaos.HistoryRecorder.ProducerEvent;
import io.minikafka.chaos.HistoryRecorder.ProducerOutcome;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;

/**
 * INV-2/INV-5: models one partition as an append-only log register and checks whether a Wing-Gong
 * style linearization exists — a single total order of all appends and reads, consistent with
 * real-time invocation/response order, under which:
 *
 * <ul>
 *   <li>a successful ({@code ACKED}) append is placed at exactly the offset it returned;
 *   <li>a {@code TIMEOUT} append is optional: it may land at whatever offset is next when (if) it's
 *       linearized, or never be linearized at all;
 *   <li>a read (a batch a consumer observed starting at some offset) must, at the moment it
 *       linearizes, match the model's contents at those offsets exactly.
 * </ul>
 *
 * <p>Because an append-only log register has only one possible total order for its committed writes
 * — offset order — the real-time-consistency check for {@code ACKED} appends is closed form; the
 * only genuine combinatorial question is which (if any) unresolved {@code TIMEOUT} appends filled
 * gaps between committed offsets, and in what order relative to reads. That is exactly what the
 * bounded backtracking search below explores, memoized on {@code (logSize, pendingOpBitset)}
 * exactly as the search-budget design calls for: a chunk whose state space isn't fully explored
 * within the budget reports {@code UNKNOWN} rather than a silently-wrong {@code PASS}.
 *
 * <p><b>Known scalability limitation:</b> unlike the design's explicit quiescent-point chunking,
 * this implementation runs one unified search per partition; a real-time "no pending predecessor"
 * readiness rule naturally prevents crossing a quiescent point without needing to split the history
 * up front, but the per-candidate readiness scan is O(n) per step, so a single very large, densely
 * concurrent partition history (millions of ops with no quiescent gaps) is not tractable here —
 * this checker is meant to run over per-scenario or per-partition windows of a chaos run, not the
 * raw unbounded headline-run history.
 */
public final class LinearizabilityChecker implements Checker {

  private final long searchBudget;

  public LinearizabilityChecker(long searchBudget) {
    if (searchBudget <= 0) {
      throw new IllegalArgumentException("searchBudget must be positive");
    }
    this.searchBudget = searchBudget;
  }

  @Override
  public CheckResult check(History history) {
    Map<Integer, List<Op>> byPartition = new TreeMap<>();
    for (ProducerEvent e : history.producerEvents()) {
      if (e.outcome() == ProducerOutcome.REJECTED) {
        continue; // never appended; nothing to linearize
      }
      byPartition
          .computeIfAbsent(e.partition(), k -> new ArrayList<>())
          .add(
              new AppendOp(
                  e.invokedNanos(),
                  e.respondedNanos(),
                  e.producerId(),
                  e.seqNo(),
                  e.outcome() == ProducerOutcome.ACKED,
                  e.offset()));
    }
    for (var entry : groupReadsByPartitionAndConsumer(history).entrySet()) {
      byPartition.computeIfAbsent(entry.getKey(), k -> new ArrayList<>()).addAll(entry.getValue());
    }

    List<CheckResult.Violation> violations = new ArrayList<>();
    int failCount = 0;
    int unknownCount = 0;
    for (var entry : byPartition.entrySet()) {
      Result result = new Solver(entry.getValue(), searchBudget).solve();
      if (result == Result.UNKNOWN) {
        unknownCount++;
        violations.add(
            new CheckResult.Violation(
                "partition " + entry.getKey() + ": search budget exhausted, result UNKNOWN"));
      } else if (result == Result.FAIL) {
        failCount++;
        violations.add(
            new CheckResult.Violation(
                "partition "
                    + entry.getKey()
                    + ": no valid linearization exists for the recorded history"));
      }
    }

    CheckResult.Status status;
    if (failCount > 0) {
      status = CheckResult.Status.FAIL;
    } else if (unknownCount > 0) {
      status = CheckResult.Status.UNKNOWN;
    } else {
      status = CheckResult.Status.PASS;
    }
    String summary =
        byPartition.size() + " partition(s) checked, " + violations.size() + " problem(s)";
    return new CheckResult("LinearizabilityChecker", status, summary, violations);
  }

  private static Map<Integer, List<Op>> groupReadsByPartitionAndConsumer(History history) {
    Map<Integer, Map<String, List<ConsumerEvent>>> grouped = new HashMap<>();
    for (ConsumerEvent e : history.consumerEvents()) {
      grouped
          .computeIfAbsent(e.partition(), k -> new HashMap<>())
          .computeIfAbsent(e.consumerId(), k -> new ArrayList<>())
          .add(e);
    }
    Map<Integer, List<Op>> result = new HashMap<>();
    for (var partitionEntry : grouped.entrySet()) {
      List<Op> readOps = new ArrayList<>();
      for (var consumerEntry : partitionEntry.getValue().entrySet()) {
        List<ConsumerEvent> events = new ArrayList<>(consumerEntry.getValue());
        events.sort((a, b) -> Long.compare(a.offset(), b.offset()));
        readOps.addAll(coalesceIntoBatches(events));
      }
      result.put(partitionEntry.getKey(), readOps);
    }
    return result;
  }

  /**
   * Each recorded consumer event is one delivered record; treat runs of contiguous offsets
   * delivered by the same poll (adjacent invoke/respond nanos) as one read op observing a batch.
   */
  private static List<Op> coalesceIntoBatches(List<ConsumerEvent> events) {
    List<Op> ops = new ArrayList<>();
    int i = 0;
    while (i < events.size()) {
      ConsumerEvent first = events.get(i);
      List<Entry> batch = new ArrayList<>();
      batch.add(new Entry(first.producerId(), first.seqNo()));
      long invoke = first.invokedNanos();
      long respond = first.respondedNanos();
      long expectedOffset = first.offset() + 1;
      int j = i + 1;
      while (j < events.size()
          && events.get(j).offset() == expectedOffset
          && events.get(j).invokedNanos() == first.invokedNanos()) {
        batch.add(new Entry(events.get(j).producerId(), events.get(j).seqNo()));
        respond = Math.max(respond, events.get(j).respondedNanos());
        expectedOffset++;
        j++;
      }
      ops.add(new ReadOp(invoke, respond, first.offset(), batch));
      i = j;
    }
    return ops;
  }

  private enum Result {
    PASS,
    FAIL,
    UNKNOWN
  }

  private sealed interface Op permits AppendOp, ReadOp {
    long invoke();

    long respond();
  }

  private record AppendOp(
      long invoke, long respond, long producerId, long seqNo, boolean acked, long offset)
      implements Op {}

  private record ReadOp(long invoke, long respond, long fromOffset, List<Entry> observed)
      implements Op {}

  private record Entry(long producerId, long seqNo) {}

  private record StateKey(int logSize, BitSet pending) {
    @Override
    public boolean equals(Object o) {
      return o instanceof StateKey k && logSize == k.logSize && pending.equals(k.pending);
    }

    @Override
    public int hashCode() {
      return Objects.hash(logSize, pending);
    }
  }

  private static final class Solver {
    private final List<Op> ops;
    private final long budget;
    private final Set<StateKey> visited = new HashSet<>();
    private boolean budgetExceeded = false;

    Solver(List<Op> ops, long budget) {
      List<Op> sorted = new ArrayList<>(ops);
      sorted.sort((a, b) -> Long.compare(a.invoke(), b.invoke()));
      this.ops = sorted;
      this.budget = budget;
    }

    Result solve() {
      BitSet allPending = new BitSet(ops.size());
      allPending.set(0, ops.size());
      boolean ok = search(allPending, new ArrayList<>());
      if (ok) {
        return Result.PASS;
      }
      return budgetExceeded ? Result.UNKNOWN : Result.FAIL;
    }

    private boolean search(BitSet pending, List<Entry> log) {
      if (pending.isEmpty()) {
        return true;
      }
      StateKey key = new StateKey(log.size(), (BitSet) pending.clone());
      if (visited.contains(key)) {
        return false;
      }
      if (visited.size() >= budget) {
        budgetExceeded = true;
        return false;
      }
      visited.add(key);

      for (int i = pending.nextSetBit(0); i >= 0; i = pending.nextSetBit(i + 1)) {
        if (!isReady(i, pending)) {
          continue;
        }
        Op op = ops.get(i);
        if (op instanceof AppendOp a) {
          if (a.acked()) {
            if (log.size() == a.offset()) {
              List<Entry> newLog = extend(log, new Entry(a.producerId(), a.seqNo()));
              BitSet next = (BitSet) pending.clone();
              next.clear(i);
              if (search(next, newLog)) {
                return true;
              }
            }
          } else {
            BitSet skip = (BitSet) pending.clone();
            skip.clear(i);
            if (search(skip, log)) {
              return true;
            }
            List<Entry> newLog = extend(log, new Entry(a.producerId(), a.seqNo()));
            BitSet apply = (BitSet) pending.clone();
            apply.clear(i);
            if (search(apply, newLog)) {
              return true;
            }
          }
        } else {
          ReadOp r = (ReadOp) op;
          if (matches(log, r)) {
            BitSet next = (BitSet) pending.clone();
            next.clear(i);
            if (search(next, log)) {
              return true;
            }
          }
        }
        if (budgetExceeded) {
          return false;
        }
      }
      return false;
    }

    private boolean isReady(int i, BitSet pending) {
      long invoke = ops.get(i).invoke();
      for (int j = pending.nextSetBit(0); j >= 0; j = pending.nextSetBit(j + 1)) {
        if (j != i && ops.get(j).respond() <= invoke) {
          return false;
        }
      }
      return true;
    }

    private static boolean matches(List<Entry> log, ReadOp r) {
      long from = r.fromOffset();
      if (log.size() < from + r.observed().size()) {
        return false;
      }
      for (int k = 0; k < r.observed().size(); k++) {
        if (!log.get((int) (from + k)).equals(r.observed().get(k))) {
          return false;
        }
      }
      return true;
    }

    private static List<Entry> extend(List<Entry> log, Entry e) {
      List<Entry> copy = new ArrayList<>(log);
      copy.add(e);
      return copy;
    }
  }
}
