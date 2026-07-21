package io.minikafka.log;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;

/**
 * Crash-safe segment recovery: scans a segment's records verifying CRCs and rebuilds its index from
 * scratch. On the active ({@code isLast}) segment, a truncated or corrupt trailing record is the
 * expected shape of a torn write from a SIGKILL mid-append — it is truncated away silently, no
 * exception. On any other, already-rolled segment, the same condition is real corruption and throws
 * (CLAUDE.md: no silent failure).
 */
final class LogRecovery {

  private LogRecovery() {}

  static LogSegment recoverSegment(
      Path dir, long baseOffset, LogConfig config, boolean isLast, LogStats stats)
      throws IOException {
    Path logFile = dir.resolve(LogSegment.logFileName(baseOffset));
    Path indexFile = dir.resolve(LogSegment.indexFileName(baseOffset));
    Files.deleteIfExists(indexFile);

    byte[] content = Files.readAllBytes(logFile);
    ByteBuffer buf = ByteBuffer.wrap(content);
    OffsetIndex index =
        OffsetIndex.open(
            indexFile, LogSegment.maxIndexEntries(config), config.indexIntervalBytes(), stats);

    long validEnd = 0;
    long lastValidOffset = -1;
    long bytesSinceLastEntry = 0;
    while (buf.hasRemaining()) {
      int recordStart = buf.position();
      LogRecord record;
      try {
        record = RecordCodec.decode(buf);
      } catch (RecordCodec.CorruptRecordException e) {
        if (isLast) {
          break;
        }
        throw new IllegalStateException(
            "Corrupt record in non-final segment " + logFile + ": " + e.getMessage(), e);
      }
      if (record == null) {
        if (isLast) {
          break;
        }
        throw new IllegalStateException(
            "Truncated record in non-final segment " + logFile + " at byte " + recordStart);
      }

      index.maybeAppend(record.offset(), recordStart, bytesSinceLastEntry);
      long recordBytes = buf.position() - recordStart;
      bytesSinceLastEntry =
          bytesSinceLastEntry >= config.indexIntervalBytes()
              ? recordBytes
              : bytesSinceLastEntry + recordBytes;
      validEnd = buf.position();
      lastValidOffset = record.offset();
    }

    return LogSegment.reopen(dir, baseOffset, config, stats, index, validEnd, lastValidOffset);
  }
}
