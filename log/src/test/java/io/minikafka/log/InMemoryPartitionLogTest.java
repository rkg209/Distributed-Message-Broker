package io.minikafka.log;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Test;

class InMemoryPartitionLogTest {

  @Test
  void appendAssignsMonotonicOffsets() {
    InMemoryPartitionLog log = new InMemoryPartitionLog();
    for (int i = 0; i < 5; i++) {
      AppendResult result = log.append(new LogRecord(0, 0, null, ("v" + i).getBytes()));
      assertEquals(i, result.offset());
    }
    assertEquals(5, log.nextOffset());
  }

  @Test
  void readFromZeroReturnsAllInOrder() {
    InMemoryPartitionLog log = new InMemoryPartitionLog();
    for (int i = 0; i < 10; i++) {
      log.append(new LogRecord(0, 0, null, ("v" + i).getBytes()));
    }
    List<LogRecord> records = log.read(0, Integer.MAX_VALUE);
    assertEquals(10, records.size());
    for (int i = 0; i < 10; i++) {
      assertEquals(i, records.get(i).offset());
    }
  }

  @Test
  void readFromMidOffsetReturnsTail() {
    InMemoryPartitionLog log = new InMemoryPartitionLog();
    for (int i = 0; i < 10; i++) {
      log.append(new LogRecord(0, 0, null, ("v" + i).getBytes()));
    }
    List<LogRecord> records = log.read(6, Integer.MAX_VALUE);
    assertEquals(4, records.size());
    assertEquals(6, records.get(0).offset());
    assertEquals(9, records.get(3).offset());
  }

  @Test
  void readBeyondEndReturnsEmpty() {
    InMemoryPartitionLog log = new InMemoryPartitionLog();
    log.append(new LogRecord(0, 0, null, "v0".getBytes()));
    assertEquals(List.of(), log.read(5, Integer.MAX_VALUE));
    assertEquals(List.of(), log.read(1, Integer.MAX_VALUE));
  }

  @Test
  void readOnEmptyLogReturnsEmpty() {
    InMemoryPartitionLog log = new InMemoryPartitionLog();
    assertEquals(List.of(), log.read(0, Integer.MAX_VALUE));
  }

  @Test
  void maxBytesCapsBatchButAlwaysReturnsAtLeastOne() {
    InMemoryPartitionLog log = new InMemoryPartitionLog();
    for (int i = 0; i < 5; i++) {
      log.append(new LogRecord(0, 0, null, new byte[10]));
    }
    List<LogRecord> capped = log.read(0, 25);
    assertEquals(2, capped.size());

    List<LogRecord> singleOversized = log.read(0, 1);
    assertEquals(1, singleOversized.size());
  }

  @Test
  void concurrentAppendsYieldContiguousOffsets() throws InterruptedException {
    InMemoryPartitionLog log = new InMemoryPartitionLog();
    int threads = 8;
    int perThread = 200;
    ExecutorService pool = Executors.newFixedThreadPool(threads);
    AtomicLong seen = new AtomicLong();
    try {
      List<Runnable> tasks =
          java.util.stream.IntStream.range(0, threads)
              .<Runnable>mapToObj(
                  t ->
                      () -> {
                        for (int i = 0; i < perThread; i++) {
                          log.append(new LogRecord(0, 0, null, "v".getBytes()));
                          seen.incrementAndGet();
                        }
                      })
              .collect(Collectors.toList());
      tasks.forEach(pool::submit);
    } finally {
      pool.shutdown();
      assertTrue(pool.awaitTermination(10, TimeUnit.SECONDS));
    }

    assertEquals((long) threads * perThread, seen.get());
    assertEquals(threads * perThread, log.nextOffset());
    List<LogRecord> all = log.read(0, Integer.MAX_VALUE);
    Set<Long> offsets = all.stream().map(LogRecord::offset).collect(Collectors.toSet());
    assertEquals(threads * perThread, offsets.size());
    for (long i = 0; i < threads * perThread; i++) {
      assertTrue(offsets.contains(i));
    }
  }
}
