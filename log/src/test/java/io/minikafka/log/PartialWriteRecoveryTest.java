package io.minikafka.log;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** Criterion 2: a torn trailing write is truncated away silently, not thrown. */
class PartialWriteRecoveryTest {

  @Test
  void truncatedTrailingRecordIsDroppedWithoutException(@TempDir Path dir) throws IOException {
    LogConfig config = LogConfig.defaultsFor(dir);
    int n = 20;
    try (DiskPartitionLog log = new DiskPartitionLog(config)) {
      for (int i = 0; i < n; i++) {
        log.append(new LogRecord(0, 0, null, ("value-" + i).getBytes()));
      }
    }

    Path logFile = activeSegmentLogFile(dir);
    try (RandomAccessFile raf = new RandomAccessFile(logFile.toFile(), "rw")) {
      long truncatedLength = raf.length() - 3;
      raf.setLength(truncatedLength);
    }

    try (DiskPartitionLog reopened = new DiskPartitionLog(config)) {
      assertEquals(n - 1, reopened.nextOffset());
      List<LogRecord> records = reopened.read(0, Integer.MAX_VALUE);
      assertEquals(n - 1, records.size());

      AppendResult result = reopened.append(new LogRecord(0, 0, null, "next".getBytes()));
      assertEquals(n - 1, result.offset());
    }
  }

  @Test
  void corruptedTrailingRecordCrcIsDroppedWithoutException(@TempDir Path dir) throws IOException {
    LogConfig config = LogConfig.defaultsFor(dir);
    int n = 20;
    try (DiskPartitionLog log = new DiskPartitionLog(config)) {
      for (int i = 0; i < n; i++) {
        log.append(new LogRecord(0, 0, null, ("value-" + i).getBytes()));
      }
    }

    Path logFile = activeSegmentLogFile(dir);
    try (RandomAccessFile raf = new RandomAccessFile(logFile.toFile(), "rw")) {
      long lastBytePos = raf.length() - 1;
      raf.seek(lastBytePos);
      int b = raf.read();
      raf.seek(lastBytePos);
      raf.write(b ^ 0xFF);
    }

    try (DiskPartitionLog reopened = new DiskPartitionLog(config)) {
      assertEquals(n - 1, reopened.nextOffset());
      List<LogRecord> records = reopened.read(0, Integer.MAX_VALUE);
      assertEquals(n - 1, records.size());
    }
  }

  private static Path activeSegmentLogFile(Path dir) throws IOException {
    try (DirectoryStream<Path> stream = Files.newDirectoryStream(dir, "*.log")) {
      List<Path> files = new java.util.ArrayList<>();
      stream.forEach(files::add);
      return files.stream().max(Comparator.comparing(Path::toString)).orElseThrow();
    }
  }
}
