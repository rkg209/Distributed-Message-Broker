package io.minikafka.log;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;

/**
 * Sparse offset index for one segment: a flat, memory-mapped array of {@code (logicalOffset 8B,
 * filePosition 4B)} entries, one per {@code indexIntervalBytes} of log data. {@link #lookup}
 * binary-searches for the largest indexed offset {@code <=} the target in O(log n).
 */
final class OffsetIndex {

  static final int ENTRY_BYTES = 12;

  private final Path file;
  private final int maxEntries;
  private final int indexIntervalBytes;
  private final LogStats stats;
  private final RandomAccessFile raf;
  private final FileChannel channel;
  private final MappedByteBuffer mmap;
  private int entryCount;

  private OffsetIndex(
      Path file,
      int maxEntries,
      int indexIntervalBytes,
      LogStats stats,
      RandomAccessFile raf,
      FileChannel channel,
      MappedByteBuffer mmap,
      int entryCount) {
    this.file = file;
    this.maxEntries = maxEntries;
    this.indexIntervalBytes = indexIntervalBytes;
    this.stats = stats;
    this.raf = raf;
    this.channel = channel;
    this.mmap = mmap;
    this.entryCount = entryCount;
  }

  /**
   * Opens {@code file}, creating it if absent. An existing file's entries (its length divided by
   * {@value #ENTRY_BYTES}) are preserved; the file is then grown to {@code maxEntries} capacity so
   * further appends never need to remap.
   */
  static OffsetIndex open(Path file, int maxEntries, int indexIntervalBytes, LogStats stats)
      throws IOException {
    long existingLength = Files.exists(file) ? Files.size(file) : 0;
    int existingEntries = (int) (existingLength / ENTRY_BYTES);

    RandomAccessFile raf = new RandomAccessFile(file.toFile(), "rw");
    long capacityBytes = Math.max((long) maxEntries * ENTRY_BYTES, existingLength);
    raf.setLength(capacityBytes);
    FileChannel channel = raf.getChannel();
    MappedByteBuffer mmap = channel.map(FileChannel.MapMode.READ_WRITE, 0, capacityBytes);

    return new OffsetIndex(
        file, maxEntries, indexIntervalBytes, stats, raf, channel, mmap, existingEntries);
  }

  int indexIntervalBytes() {
    return indexIntervalBytes;
  }

  int entryCount() {
    return entryCount;
  }

  void append(long offset, int position) {
    if (entryCount >= maxEntries) {
      throw new IllegalStateException("Offset index " + file + " is full at " + maxEntries);
    }
    int pos = entryCount * ENTRY_BYTES;
    mmap.putLong(pos, offset);
    mmap.putInt(pos + 8, position);
    entryCount++;
  }

  /** Appends a new entry only once at least {@code indexIntervalBytes} have accumulated. */
  void maybeAppend(long offset, int position, long bytesSinceLastEntry) {
    if (bytesSinceLastEntry >= indexIntervalBytes) {
      append(offset, position);
    }
  }

  /**
   * Returns the file position of the largest indexed entry {@code <= targetOffset}, or 0 if every
   * entry is greater (including when the index is empty) — the caller then scans forward from the
   * start of the segment.
   */
  int lookup(long targetOffset) {
    stats.indexLookups.incrementAndGet();
    int lo = 0;
    int hi = entryCount - 1;
    int result = 0;
    while (lo <= hi) {
      int mid = (lo + hi) >>> 1;
      long midOffset = mmap.getLong(mid * ENTRY_BYTES);
      if (midOffset <= targetOffset) {
        result = mmap.getInt(mid * ENTRY_BYTES + 8);
        lo = mid + 1;
      } else {
        hi = mid - 1;
      }
    }
    return result;
  }

  /** Discards entries whose logical offset exceeds {@code offset} (used during recovery). */
  void truncateTo(long offset) {
    int lo = 0;
    int hi = entryCount - 1;
    int newCount = 0;
    while (lo <= hi) {
      int mid = (lo + hi) >>> 1;
      long midOffset = mmap.getLong(mid * ENTRY_BYTES);
      if (midOffset <= offset) {
        newCount = mid + 1;
        lo = mid + 1;
      } else {
        hi = mid - 1;
      }
    }
    entryCount = newCount;
  }

  void reset() {
    entryCount = 0;
  }

  void flush() {
    mmap.force();
  }

  /** Flushes, truncates the file to its exact used size, and releases resources. */
  void close() {
    try {
      flush();
      channel.truncate((long) entryCount * ENTRY_BYTES);
      channel.close();
      raf.close();
    } catch (IOException e) {
      throw new java.io.UncheckedIOException("Failed to close offset index " + file, e);
    }
  }
}
