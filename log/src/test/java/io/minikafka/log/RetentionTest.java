package io.minikafka.log;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class RetentionTest {

  @Test
  void retentionBytesDeletesOldSegmentsAndAdvancesFirstOffset(@TempDir Path dir)
      throws IOException {
    int segmentBytes = 64 * 1024;
    LogConfig config =
        new LogConfig(dir, FsyncPolicy.OS_MANAGED, 1000, segmentBytes, 4096, 2L * segmentBytes, -1);
    byte[] value = new byte[20];

    try (DiskPartitionLog log = new DiskPartitionLog(config)) {
      int recordsPerSegment = (segmentBytes / 48) + 10;
      int recordCount = recordsPerSegment * 6; // well past 2 segments' worth of retention
      for (int i = 0; i < recordCount; i++) {
        log.append(new LogRecord(0, 0, null, value));
      }

      assertTrue(log.firstOffset() > 0, "expected retention to advance firstOffset past 0");
      assertTrue(countFiles(dir, "*.log") <= 3, "expected old segments to be deleted");

      assertThrows(OffsetOutOfRangeException.class, () -> log.read(0, Integer.MAX_VALUE));

      // The retained tail must still be readable end to end.
      var tail = log.read(log.firstOffset(), Integer.MAX_VALUE);
      assertEquals(log.nextOffset() - log.firstOffset(), tail.size());
    }
  }

  @Test
  void retentionMsDeletesSegmentsOlderThanBackdatedLastModified(@TempDir Path dir)
      throws IOException {
    int segmentBytes = 32 * 1024;
    LogConfig config =
        new LogConfig(dir, FsyncPolicy.OS_MANAGED, 1000, segmentBytes, 4096, -1, 1000);
    byte[] value = new byte[20];

    try (DiskPartitionLog log = new DiskPartitionLog(config)) {
      int recordsPerSegment = (segmentBytes / 48) + 5;
      for (int i = 0; i < recordsPerSegment; i++) {
        log.append(new LogRecord(0, 0, null, value));
      }
      log.flush();

      // Back-date every rolled segment's mtime so the next append's retention check sees it as
      // older than retentionMs, without needing a real 1-second sleep in the test.
      backdateAllLogFiles(dir, System.currentTimeMillis() - 10_000);

      // Force a roll + retention check.
      for (int i = 0; i < recordsPerSegment * 2; i++) {
        log.append(new LogRecord(0, 0, null, value));
      }

      assertTrue(log.firstOffset() > 0, "expected retentionMs to delete the aged-out segment");
    }
  }

  private static void backdateAllLogFiles(Path dir, long epochMillis) throws IOException {
    try (DirectoryStream<Path> stream = Files.newDirectoryStream(dir, "*.log")) {
      for (Path p : stream) {
        Files.setLastModifiedTime(p, java.nio.file.attribute.FileTime.fromMillis(epochMillis));
      }
    }
  }

  private static long countFiles(Path dir, String glob) throws IOException {
    long count = 0;
    try (DirectoryStream<Path> stream = Files.newDirectoryStream(dir, glob)) {
      for (Path ignored : stream) {
        count++;
      }
    }
    return count;
  }
}
