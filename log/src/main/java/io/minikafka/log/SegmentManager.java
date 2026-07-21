package io.minikafka.log;

import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;

/**
 * Owns the ordered set of {@link LogSegment}s for one partition directory: rolls a new segment when
 * the active one is full, finds the segment holding a given offset in O(log n) via {@link
 * NavigableMap#floorEntry}, and enforces size/time retention by deleting whole oldest segments.
 */
final class SegmentManager {

  private final Path dir;
  private final LogConfig config;
  private final LogStats stats;
  private final NavigableMap<Long, LogSegment> segments = new TreeMap<>();

  private long firstOffset;
  private long nextOffset;

  private SegmentManager(Path dir, LogConfig config, LogStats stats) {
    this.dir = dir;
    this.config = config;
    this.stats = stats;
  }

  static SegmentManager open(LogConfig config, LogStats stats) throws IOException {
    Files.createDirectories(config.dir());
    SegmentManager manager = new SegmentManager(config.dir(), config, stats);
    manager.recoverSegments();
    return manager;
  }

  private void recoverSegments() throws IOException {
    List<Long> baseOffsets = listSegmentBaseOffsets(dir);
    if (baseOffsets.isEmpty()) {
      LogSegment segment = LogSegment.createNew(dir, 0, config, stats);
      segments.put(0L, segment);
      firstOffset = 0;
      nextOffset = 0;
      return;
    }
    for (int i = 0; i < baseOffsets.size(); i++) {
      long base = baseOffsets.get(i);
      boolean isLast = i == baseOffsets.size() - 1;
      LogSegment segment = LogRecovery.recoverSegment(dir, base, config, isLast, stats);
      segments.put(base, segment);
    }
    firstOffset = segments.firstKey();
    LogSegment last = segments.lastEntry().getValue();
    nextOffset = last.lastOffset() == -1 ? last.baseOffset() : last.lastOffset() + 1;
  }

  private static List<Long> listSegmentBaseOffsets(Path dir) throws IOException {
    List<Long> baseOffsets = new ArrayList<>();
    try (DirectoryStream<Path> stream = Files.newDirectoryStream(dir, "*.log")) {
      for (Path p : stream) {
        String name = p.getFileName().toString();
        baseOffsets.add(Long.parseLong(name.substring(0, name.indexOf('.'))));
      }
    }
    baseOffsets.sort(Long::compareTo);
    return baseOffsets;
  }

  long firstOffset() {
    return firstOffset;
  }

  long nextOffset() {
    return nextOffset;
  }

  void append(LogRecord record) throws IOException {
    LogSegment active = segments.lastEntry().getValue();
    int recordSize = RecordCodec.sizeOf(record);
    if (active.sizeBytes() > 0 && active.sizeBytes() + recordSize > config.maxSegmentBytes()) {
      active = roll(record.offset());
    }
    active.append(record);
    nextOffset = record.offset() + 1;
    enforceRetention();
  }

  private LogSegment roll(long baseOffset) throws IOException {
    LogSegment segment = LogSegment.createNew(dir, baseOffset, config, stats);
    segments.put(baseOffset, segment);
    return segment;
  }

  List<LogRecord> read(long fromOffset, int maxBytes) throws IOException {
    Map.Entry<Long, LogSegment> entry = segments.floorEntry(fromOffset);
    stats.segmentLookups.incrementAndGet();
    if (entry == null) {
      entry = segments.firstEntry();
    }

    List<LogRecord> result = new ArrayList<>();
    long remaining = maxBytes;
    long nextExpectedOffset = fromOffset;
    for (LogSegment segment : segments.tailMap(entry.getKey()).values()) {
      List<LogRecord> fromSegment =
          segment.read(nextExpectedOffset, (int) Math.min(remaining, Integer.MAX_VALUE));
      if (fromSegment.isEmpty()) {
        if (!result.isEmpty()) {
          break;
        }
        continue;
      }
      result.addAll(fromSegment);
      for (LogRecord r : fromSegment) {
        remaining -= r.value().length;
      }
      nextExpectedOffset = fromSegment.get(fromSegment.size() - 1).offset() + 1;
      if (remaining <= 0 || nextExpectedOffset >= nextOffset) {
        break;
      }
    }
    return result;
  }

  void enforceRetention() throws IOException {
    if (config.retentionBytes() < 0 && config.retentionMs() < 0) {
      return;
    }
    long now = System.currentTimeMillis();
    while (segments.size() > 1) {
      LogSegment oldest = segments.firstEntry().getValue();
      long totalBytes = segments.values().stream().mapToLong(LogSegment::sizeBytes).sum();
      boolean overSize = config.retentionBytes() >= 0 && totalBytes > config.retentionBytes();
      boolean overAge =
          config.retentionMs() >= 0 && now - oldest.lastModifiedMs() > config.retentionMs();
      if (!overSize && !overAge) {
        break;
      }
      segments.remove(oldest.baseOffset());
      oldest.delete();
      firstOffset = segments.firstKey();
    }
  }

  void flush() throws IOException {
    for (LogSegment segment : segments.values()) {
      segment.force();
    }
  }

  void close() throws IOException {
    for (LogSegment segment : segments.values()) {
      segment.close();
    }
  }
}
