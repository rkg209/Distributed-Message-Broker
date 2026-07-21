package io.minikafka.log;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class SegmentRollTest {

  @Test
  void rollsMultipleSegmentsAndSurvivesReopen(@TempDir Path dir) throws IOException {
    LogConfig config = new LogConfig(dir, FsyncPolicy.OS_MANAGED, 1000, 1024 * 1024, 4096, -1, -1);
    int recordCount = 70_000; // 48 bytes/record => ~3.2 MB => 4 segments at 1 MiB each
    byte[] value = new byte[20];

    try (DiskPartitionLog log = new DiskPartitionLog(config)) {
      for (int i = 0; i < recordCount; i++) {
        log.append(new LogRecord(0, 0, null, value));
      }
    }

    long logFileCount = countFiles(dir, "*.log");
    assertTrue(logFileCount >= 3, "expected at least 3 segments, got " + logFileCount);

    try (DiskPartitionLog reopened = new DiskPartitionLog(config)) {
      assertEquals(recordCount, reopened.nextOffset());
      List<LogRecord> all = reopened.read(0, Integer.MAX_VALUE);
      assertEquals(recordCount, all.size());
      for (int i = 0; i < recordCount; i++) {
        assertEquals(i, all.get(i).offset());
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
