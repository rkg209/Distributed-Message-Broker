package io.minikafka.log;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

/**
 * One {@code .log} file plus its {@link OffsetIndex}. Filenames are the segment's base offset,
 * zero-padded to 20 digits: {@code {baseOffset:020d}.log} / {@code .index}.
 */
final class LogSegment {

  private final long baseOffset;
  private final Path logFile;
  private final Path indexFile;
  private final RandomAccessFile logRaf;
  private final FileChannel channel;
  private final OffsetIndex index;
  private final FsyncPolicy fsyncPolicy;
  private final LogStats stats;

  private long size;
  private long bytesSinceLastIndexEntry;
  private long lastOffset = -1;

  private LogSegment(
      long baseOffset,
      Path logFile,
      Path indexFile,
      RandomAccessFile logRaf,
      FileChannel channel,
      OffsetIndex index,
      FsyncPolicy fsyncPolicy,
      LogStats stats,
      long size,
      long lastOffset) {
    this.baseOffset = baseOffset;
    this.logFile = logFile;
    this.indexFile = indexFile;
    this.logRaf = logRaf;
    this.channel = channel;
    this.index = index;
    this.fsyncPolicy = fsyncPolicy;
    this.stats = stats;
    this.size = size;
    this.lastOffset = lastOffset;
  }

  static String logFileName(long baseOffset) {
    return String.format("%020d.log", baseOffset);
  }

  static String indexFileName(long baseOffset) {
    return String.format("%020d.index", baseOffset);
  }

  static int maxIndexEntries(LogConfig config) {
    return config.maxSegmentBytes() / config.indexIntervalBytes() + 2;
  }

  /** Creates a brand-new, empty segment starting at {@code baseOffset}. */
  static LogSegment createNew(Path dir, long baseOffset, LogConfig config, LogStats stats)
      throws IOException {
    Path logFile = dir.resolve(logFileName(baseOffset));
    Path indexFile = dir.resolve(indexFileName(baseOffset));
    RandomAccessFile raf = new RandomAccessFile(logFile.toFile(), "rw");
    FileChannel channel = raf.getChannel();
    OffsetIndex index =
        OffsetIndex.open(indexFile, maxIndexEntries(config), config.indexIntervalBytes(), stats);
    return new LogSegment(
        baseOffset, logFile, indexFile, raf, channel, index, config.fsyncPolicy(), stats, 0, -1);
  }

  /**
   * Reopens an existing segment whose valid extent has already been determined by {@link
   * LogRecovery} — the log file is truncated to {@code validSize} and a freshly rebuilt {@code
   * index} covering exactly that range is adopted.
   */
  static LogSegment reopen(
      Path dir,
      long baseOffset,
      LogConfig config,
      LogStats stats,
      OffsetIndex index,
      long validSize,
      long lastValidOffset)
      throws IOException {
    Path logFile = dir.resolve(logFileName(baseOffset));
    Path indexFile = dir.resolve(indexFileName(baseOffset));

    RandomAccessFile raf = new RandomAccessFile(logFile.toFile(), "rw");
    FileChannel channel = raf.getChannel();
    channel.truncate(validSize);
    channel.position(validSize);

    return new LogSegment(
        baseOffset,
        logFile,
        indexFile,
        raf,
        channel,
        index,
        config.fsyncPolicy(),
        stats,
        validSize,
        lastValidOffset);
  }

  long baseOffset() {
    return baseOffset;
  }

  /** -1 if this segment holds no records (only possible for a fresh, empty log). */
  long lastOffset() {
    return lastOffset;
  }

  long sizeBytes() {
    return size;
  }

  long lastModifiedMs() {
    try {
      return Files.getLastModifiedTime(logFile).toMillis();
    } catch (IOException e) {
      throw new java.io.UncheckedIOException(e);
    }
  }

  int append(LogRecord record) throws IOException {
    byte[] encoded = RecordCodec.encode(record);
    int position = (int) size;
    index.maybeAppend(record.offset(), position, bytesSinceLastIndexEntry);
    if (bytesSinceLastIndexEntry >= index.indexIntervalBytes()) {
      bytesSinceLastIndexEntry = 0;
    }

    channel.write(ByteBuffer.wrap(encoded), position);
    size += encoded.length;
    bytesSinceLastIndexEntry += encoded.length;
    lastOffset = record.offset();

    applyFsyncPolicy();
    return encoded.length;
  }

  private void applyFsyncPolicy() throws IOException {
    if (fsyncPolicy == FsyncPolicy.EVERY_WRITE) {
      force();
    }
  }

  /**
   * Reads records starting at {@code fromOffset}, using the index to seek near the start instead of
   * scanning from byte 0. Accumulates until the next record would exceed {@code maxBytes}, always
   * returning at least one record if one is present in range.
   */
  List<LogRecord> read(long fromOffset, int maxBytes) throws IOException {
    int startPos = index.lookup(fromOffset);
    long readLength = size - startPos;
    if (readLength <= 0) {
      return List.of();
    }
    ByteBuffer buf = ByteBuffer.allocate((int) readLength);
    channel.read(buf, startPos);
    buf.flip();

    List<LogRecord> result = new ArrayList<>();
    long accumulatedBytes = 0;
    while (buf.hasRemaining()) {
      LogRecord record = RecordCodec.decode(buf);
      if (record == null) {
        break;
      }
      stats.recordScans.incrementAndGet();
      if (record.offset() < fromOffset) {
        continue;
      }
      if (!result.isEmpty() && accumulatedBytes + record.value().length > maxBytes) {
        break;
      }
      result.add(record);
      accumulatedBytes += record.value().length;
    }
    return result;
  }

  void force() throws IOException {
    channel.force(true);
    index.flush();
  }

  void close() throws IOException {
    force();
    channel.close();
    logRaf.close();
    index.close();
  }

  void delete() throws IOException {
    channel.close();
    logRaf.close();
    index.close();
    Files.deleteIfExists(logFile);
    Files.deleteIfExists(indexFile);
  }
}
