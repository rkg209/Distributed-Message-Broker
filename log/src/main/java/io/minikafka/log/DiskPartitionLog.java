package io.minikafka.log;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Disk-backed {@link PartitionLog}: rolling segment files, a configurable {@link FsyncPolicy},
 * sparse mmap offset indices, and crash-safe recovery via {@link LogRecovery}. Recovery runs
 * eagerly in the constructor (CLAUDE.md: durability errors must never be swallowed, so a broken log
 * fails to construct rather than silently starting empty).
 *
 * <p>Same lock discipline as {@link InMemoryPartitionLog}: append takes the write lock so offset
 * assignment is atomic (INV-2); reads take the read lock and stay concurrent with each other.
 */
public final class DiskPartitionLog implements PartitionLog, AutoCloseable {

  private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
  private final LogConfig config;
  private final LogStats stats = new LogStats();
  private final SegmentManager segmentManager;
  private volatile Thread periodicFlusher;
  private volatile boolean closed;

  public DiskPartitionLog(LogConfig config) {
    this.config = config;
    try {
      this.segmentManager = SegmentManager.open(config, stats);
    } catch (IOException e) {
      throw new UncheckedIOException("Failed to open durable log at " + config.dir(), e);
    }
    if (config.fsyncPolicy() == FsyncPolicy.PERIODIC) {
      startPeriodicFlusher();
    }
  }

  private void startPeriodicFlusher() {
    periodicFlusher =
        Thread.ofVirtual()
            .name("log-flusher-" + config.dir())
            .start(
                () -> {
                  try {
                    while (!Thread.currentThread().isInterrupted()) {
                      Thread.sleep(config.fsyncIntervalMs());
                      flush();
                    }
                  } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                  }
                });
  }

  @Override
  public AppendResult append(LogRecord record) {
    lock.writeLock().lock();
    try {
      long offset = segmentManager.nextOffset();
      long timestamp = record.timestamp() != 0 ? record.timestamp() : System.currentTimeMillis();
      LogRecord toWrite = new LogRecord(offset, timestamp, record.key(), record.value());
      segmentManager.append(toWrite);
      return new AppendResult(offset, timestamp);
    } catch (IOException e) {
      throw new UncheckedIOException("Failed to append to durable log at " + config.dir(), e);
    } finally {
      lock.writeLock().unlock();
    }
  }

  @Override
  public List<LogRecord> read(long fromOffset, int maxBytes) {
    lock.readLock().lock();
    try {
      long next = segmentManager.nextOffset();
      long first = segmentManager.firstOffset();
      if (fromOffset >= next) {
        return List.of();
      }
      if (fromOffset < first) {
        throw new OffsetOutOfRangeException(fromOffset, first, next);
      }
      return segmentManager.read(fromOffset, maxBytes);
    } catch (IOException e) {
      throw new UncheckedIOException("Failed to read durable log at " + config.dir(), e);
    } finally {
      lock.readLock().unlock();
    }
  }

  @Override
  public long nextOffset() {
    lock.readLock().lock();
    try {
      return segmentManager.nextOffset();
    } finally {
      lock.readLock().unlock();
    }
  }

  @Override
  public long firstOffset() {
    lock.readLock().lock();
    try {
      return segmentManager.firstOffset();
    } finally {
      lock.readLock().unlock();
    }
  }

  @Override
  public void flush() {
    lock.writeLock().lock();
    try {
      segmentManager.flush();
    } catch (IOException e) {
      throw new UncheckedIOException("Failed to flush durable log at " + config.dir(), e);
    } finally {
      lock.writeLock().unlock();
    }
  }

  @Override
  public void recover() {
    // No-op: recovery already ran eagerly in the constructor before this log became usable.
  }

  @Override
  public void close() {
    lock.writeLock().lock();
    try {
      if (closed) {
        return;
      }
      closed = true;
      if (periodicFlusher != null) {
        periodicFlusher.interrupt();
      }
      segmentManager.close();
    } catch (IOException e) {
      throw new UncheckedIOException("Failed to close durable log at " + config.dir(), e);
    } finally {
      lock.writeLock().unlock();
    }
  }

  /** Exposed package-private for tests asserting the O(log n) lookup claim (criterion 4). */
  long segmentLookups() {
    return stats.segmentLookups();
  }

  long indexLookups() {
    return stats.indexLookups();
  }

  long recordScans() {
    return stats.recordScans();
  }
}
