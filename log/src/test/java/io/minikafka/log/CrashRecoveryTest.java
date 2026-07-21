package io.minikafka.log;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * INV-1 at the single-broker level: a real subprocess is SIGKILLed mid-write with {@link
 * FsyncPolicy#EVERY_WRITE}, and every acknowledged offset must survive intact with zero
 * duplication.
 */
@Tag("slow")
class CrashRecoveryTest {

  private static final int RECORD_COUNT = 10_000;

  @Test
  void acknowledgedWritesSurviveSigkill(@TempDir Path dir)
      throws IOException, InterruptedException {
    String javaBin =
        ProcessHandle.current()
            .info()
            .command()
            .orElseThrow(() -> new IllegalStateException("no java binary"));
    String classpath = System.getProperty("java.class.path");

    Process process =
        new ProcessBuilder(
                javaBin,
                "-cp",
                classpath,
                CrashWriterMain.class.getName(),
                dir.toString(),
                String.valueOf(RECORD_COUNT))
            .redirectErrorStream(true)
            .start();

    Set<Long> ackedOffsets = new HashSet<>();
    try (BufferedReader reader =
        new BufferedReader(
            new InputStreamReader(process.getInputStream(), StandardCharsets.UTF_8))) {
      String line;
      while (ackedOffsets.size() < RECORD_COUNT && (line = reader.readLine()) != null) {
        try {
          ackedOffsets.add(Long.parseLong(line.trim()));
        } catch (NumberFormatException ignored) {
          // Not an offset line (e.g. a stray log message); ignore.
        }
      }
    }

    assertEquals(RECORD_COUNT, ackedOffsets.size(), "subprocess did not acknowledge all writes");

    process.destroyForcibly();
    assertTrue(process.waitFor(10, TimeUnit.SECONDS), "subprocess did not die after SIGKILL");

    LogConfig config = LogConfig.defaultsFor(dir);
    try (DiskPartitionLog reopened = new DiskPartitionLog(config)) {
      assertEquals(RECORD_COUNT, reopened.nextOffset());

      List<LogRecord> records = reopened.read(0, Integer.MAX_VALUE);
      assertEquals(RECORD_COUNT, records.size());

      Set<Long> recoveredOffsets = new HashSet<>();
      for (int i = 0; i < records.size(); i++) {
        LogRecord record = records.get(i);
        assertEquals(i, record.offset(), "offsets must be contiguous with no gaps or duplicates");
        assertEquals("record-" + i, new String(record.value(), StandardCharsets.UTF_8));
        recoveredOffsets.add(record.offset());
      }
      assertEquals(RECORD_COUNT, recoveredOffsets.size(), "duplicate offsets after recovery");
    }
  }
}
