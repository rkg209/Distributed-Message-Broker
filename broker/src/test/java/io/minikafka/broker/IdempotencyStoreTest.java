package io.minikafka.broker;

import static org.junit.jupiter.api.Assertions.assertEquals;

import io.minikafka.log.DiskPartitionLog;
import io.minikafka.log.LogConfig;
import io.minikafka.log.LogRecord;
import io.minikafka.log.PartitionLog;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Unit tests for {@link IdempotencyStore}'s {@code APPEND}/{@code DUPLICATE}/{@code GAP} decision
 * table, the sentinel bypass, and {@link IdempotencyStore#rebuildFrom} over a real durable log.
 */
class IdempotencyStoreTest {

  @Test
  void firstMessageFromAProducerMustStartAtSeqZero() {
    IdempotencyStore store =
        IdempotencyStore.rebuildFrom(new io.minikafka.log.InMemoryPartitionLog());
    IdempotencyStore.Verdict verdict = store.check(1L, 0L);
    assertEquals(IdempotencyStore.Decision.APPEND, verdict.decision());
  }

  @Test
  void nextSeqInOrderIsAppend() {
    IdempotencyStore store =
        IdempotencyStore.rebuildFrom(new io.minikafka.log.InMemoryPartitionLog());
    store.check(1L, 0L);
    store.record(1L, 0L, 100L);
    IdempotencyStore.Verdict verdict = store.check(1L, 1L);
    assertEquals(IdempotencyStore.Decision.APPEND, verdict.decision());
  }

  @Test
  void repeatOfLastSeqIsDuplicateWithCachedOffset() {
    IdempotencyStore store =
        IdempotencyStore.rebuildFrom(new io.minikafka.log.InMemoryPartitionLog());
    store.record(1L, 0L, 100L);
    IdempotencyStore.Verdict verdict = store.check(1L, 0L);
    assertEquals(IdempotencyStore.Decision.DUPLICATE, verdict.decision());
    assertEquals(100L, verdict.cachedOffset());
  }

  @Test
  void olderThanLastSeqIsDuplicateWithNoCachedOffset() {
    IdempotencyStore store =
        IdempotencyStore.rebuildFrom(new io.minikafka.log.InMemoryPartitionLog());
    store.record(1L, 0L, 100L);
    store.record(1L, 1L, 101L);
    IdempotencyStore.Verdict verdict = store.check(1L, 0L);
    assertEquals(IdempotencyStore.Decision.DUPLICATE, verdict.decision());
    assertEquals(-1L, verdict.cachedOffset());
  }

  @Test
  void seqThatSkipsAheadIsGap() {
    IdempotencyStore store =
        IdempotencyStore.rebuildFrom(new io.minikafka.log.InMemoryPartitionLog());
    store.record(1L, 0L, 100L);
    IdempotencyStore.Verdict verdict = store.check(1L, 5L);
    assertEquals(IdempotencyStore.Decision.GAP, verdict.decision());
    assertEquals(1L, verdict.expectedSeq());
  }

  @Test
  void sentinelProducerIdAlwaysAppends() {
    IdempotencyStore store =
        IdempotencyStore.rebuildFrom(new io.minikafka.log.InMemoryPartitionLog());
    assertEquals(
        IdempotencyStore.Decision.APPEND,
        store.check(LogRecord.NO_PRODUCER_ID, LogRecord.NO_SEQ).decision());
    assertEquals(
        IdempotencyStore.Decision.APPEND,
        store.check(LogRecord.NO_PRODUCER_ID, LogRecord.NO_SEQ).decision());
  }

  @Test
  void rebuildFromScansLogAndRecoversLastAckPerProducer(@TempDir Path dir) {
    LogConfig config = LogConfig.defaultsFor(dir);
    PartitionLog log = new DiskPartitionLog(config);
    try {
      log.append(new LogRecord(0, 0, 1L, 0L, null, "a".getBytes()));
      log.append(new LogRecord(0, 0, 1L, 1L, null, "b".getBytes()));
      log.append(new LogRecord(0, 0, 2L, 0L, null, "c".getBytes()));
      log.append(
          new LogRecord(0, 0, LogRecord.NO_PRODUCER_ID, LogRecord.NO_SEQ, null, "d".getBytes()));

      IdempotencyStore store = IdempotencyStore.rebuildFrom(log);

      assertEquals(IdempotencyStore.Decision.DUPLICATE, store.check(1L, 1L).decision());
      assertEquals(1L, store.check(1L, 1L).cachedOffset());
      assertEquals(IdempotencyStore.Decision.APPEND, store.check(1L, 2L).decision());
      assertEquals(IdempotencyStore.Decision.DUPLICATE, store.check(2L, 0L).decision());
      assertEquals(IdempotencyStore.Decision.APPEND, store.check(2L, 1L).decision());
    } finally {
      log.close();
    }
  }
}
