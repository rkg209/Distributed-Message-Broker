package io.minikafka.log;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Volatile, in-process {@link PartitionLog} backed by an {@link ArrayList}. No durability: {@link
 * #flush()} and {@link #recover()} are intentional no-ops here — persistence arrives in Spec 03.
 *
 * <p>Append takes the write lock so offset assignment and insertion are atomic, giving monotonic,
 * contiguous offsets under concurrent appends (INV-2). Reads take the read lock and stay concurrent
 * with each other.
 */
public final class InMemoryPartitionLog implements PartitionLog {

  private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
  private final List<LogRecord> records = new ArrayList<>();

  @Override
  public AppendResult append(LogRecord record) {
    lock.writeLock().lock();
    try {
      long offset = records.size();
      long timestamp = record.timestamp() != 0 ? record.timestamp() : System.currentTimeMillis();
      records.add(new LogRecord(offset, timestamp, record.key(), record.value()));
      return new AppendResult(offset, timestamp);
    } finally {
      lock.writeLock().unlock();
    }
  }

  @Override
  public List<LogRecord> read(long fromOffset, int maxBytes) {
    lock.readLock().lock();
    try {
      if (fromOffset >= records.size()) {
        return List.of();
      }
      int start = (int) Math.max(fromOffset, 0);
      List<LogRecord> result = new ArrayList<>();
      long accumulatedBytes = 0;
      for (int i = start; i < records.size(); i++) {
        LogRecord record = records.get(i);
        if (!result.isEmpty() && accumulatedBytes + record.value().length > maxBytes) {
          break;
        }
        result.add(record);
        accumulatedBytes += record.value().length;
      }
      return result;
    } finally {
      lock.readLock().unlock();
    }
  }

  @Override
  public long nextOffset() {
    lock.readLock().lock();
    try {
      return records.size();
    } finally {
      lock.readLock().unlock();
    }
  }

  @Override
  public long firstOffset() {
    return 0;
  }

  @Override
  public void flush() {
    // No-op: this log is volatile; durability arrives with the Spec 03 segment log.
  }

  @Override
  public void recover() {
    // No-op: there is nothing to restore for a volatile, in-memory log.
  }

  @Override
  public void close() {
    lock.writeLock().lock();
    try {
      records.clear();
    } finally {
      lock.writeLock().unlock();
    }
  }
}
