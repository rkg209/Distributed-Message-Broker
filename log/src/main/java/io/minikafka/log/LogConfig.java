package io.minikafka.log;

import java.nio.file.Path;

/**
 * Every tunable of a {@link DiskPartitionLog}. CLAUDE.md forbids hardcoded configuration, so all
 * broker call sites must build this from {@code BrokerConfig} rather than using the defaults
 * directly in production.
 */
public record LogConfig(
    Path dir,
    FsyncPolicy fsyncPolicy,
    long fsyncIntervalMs,
    int maxSegmentBytes,
    int indexIntervalBytes,
    long retentionBytes,
    long retentionMs) {

  /** 1 GiB — chosen so a segment's max byte position always fits in the index's 4-byte field. */
  public static final int DEFAULT_MAX_SEGMENT_BYTES = 1 << 30;

  public static final int DEFAULT_INDEX_INTERVAL_BYTES = 4096;
  public static final FsyncPolicy DEFAULT_FSYNC_POLICY = FsyncPolicy.EVERY_WRITE;
  public static final long DEFAULT_FSYNC_INTERVAL_MS = 1000;

  /** Retention disabled. */
  public static final long UNLIMITED = -1;

  public LogConfig {
    if (dir == null) {
      throw new IllegalArgumentException("dir must not be null");
    }
    if (fsyncPolicy == null) {
      throw new IllegalArgumentException("fsyncPolicy must not be null");
    }
    if (maxSegmentBytes <= 0 || maxSegmentBytes > DEFAULT_MAX_SEGMENT_BYTES) {
      throw new IllegalArgumentException(
          "maxSegmentBytes must be in (0, "
              + DEFAULT_MAX_SEGMENT_BYTES
              + "] so index file positions fit in 4 bytes, got: "
              + maxSegmentBytes);
    }
    if (indexIntervalBytes <= 0) {
      throw new IllegalArgumentException("indexIntervalBytes must be positive");
    }
  }

  /** Convenience factory using every default except the storage directory. */
  public static LogConfig defaultsFor(Path dir) {
    return new LogConfig(
        dir,
        DEFAULT_FSYNC_POLICY,
        DEFAULT_FSYNC_INTERVAL_MS,
        DEFAULT_MAX_SEGMENT_BYTES,
        DEFAULT_INDEX_INTERVAL_BYTES,
        UNLIMITED,
        UNLIMITED);
  }
}
