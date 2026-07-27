package io.minikafka.broker;

import io.minikafka.log.LogRecord;
import io.minikafka.log.PartitionLog;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Tracks the last committed {@code (seqNo, offset)} per producer for one {@link PartitionReplica}.
 * Consulted from {@link PartitionReplica#apply}, which runs identically (under the Raft lock) on
 * every replica, so all replicas converge on the same idempotency state and a newly elected leader
 * is instantly correct — see the plan's decision to place the check in {@code apply()} rather than
 * {@code append()}.
 */
final class IdempotencyStore {

  private final ConcurrentHashMap<Long, Ack> lastAck = new ConcurrentHashMap<>();

  private IdempotencyStore() {}

  record Ack(long seqNo, long offset) {}

  enum Decision {
    APPEND,
    DUPLICATE,
    GAP
  }

  record Verdict(Decision decision, long cachedOffset, long expectedSeq) {
    private static final Verdict APPEND = new Verdict(Decision.APPEND, -1, -1);
  }

  /** Pure; does not mutate state. Call {@link #record} after the record is durably appended. */
  Verdict check(long producerId, long seqNo) {
    if (producerId == LogRecord.NO_PRODUCER_ID) {
      return Verdict.APPEND;
    }
    Ack last = lastAck.get(producerId);
    long lastSeq = last == null ? -1 : last.seqNo();
    if (seqNo == lastSeq + 1) {
      return Verdict.APPEND;
    }
    if (seqNo <= lastSeq) {
      // Cached-offset window is exactly one: only the most recent seq carries a usable offset.
      long cachedOffset = (seqNo == lastSeq) ? last.offset() : -1;
      return new Verdict(Decision.DUPLICATE, cachedOffset, -1);
    }
    return new Verdict(Decision.GAP, -1, lastSeq + 1);
  }

  void record(long producerId, long seqNo, long offset) {
    if (producerId == LogRecord.NO_PRODUCER_ID) {
      return;
    }
    lastAck.put(producerId, new Ack(seqNo, offset));
  }

  /** Rebuilds dedup state by scanning the full recovered log — see plan decision 3. */
  static IdempotencyStore rebuildFrom(PartitionLog log) {
    IdempotencyStore store = new IdempotencyStore();
    long offset = log.firstOffset();
    long end = log.nextOffset();
    int chunkBytes = 4 * 1024 * 1024;
    while (offset < end) {
      List<LogRecord> records = log.read(offset, chunkBytes);
      if (records.isEmpty()) {
        break;
      }
      for (LogRecord record : records) {
        if (record.producerId() != LogRecord.NO_PRODUCER_ID) {
          store.record(record.producerId(), record.seqNo(), record.offset());
        }
      }
      offset = records.get(records.size() - 1).offset() + 1;
    }
    return store;
  }
}
