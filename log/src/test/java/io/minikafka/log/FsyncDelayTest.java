package io.minikafka.log;

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Path;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** Confirms {@code BROKER_FSYNC_DELAY_MS} actually delays {@link LogSegment#force()}. */
class FsyncDelayTest {

  @Test
  void nonZeroDelayMeasurablySlowsForce(@TempDir Path dir) {
    LogConfig config =
        new LogConfig(dir, FsyncPolicy.EVERY_WRITE, 1000, 1024 * 1024, 4096, -1, -1, 200);
    try (DiskPartitionLog log = new DiskPartitionLog(config)) {
      long start = System.nanoTime();
      log.append(new LogRecord(0, 0, -1, -1, null, "v".getBytes()));
      long elapsedMs = (System.nanoTime() - start) / 1_000_000;
      assertTrue(elapsedMs >= 200, "expected append to take at least 200ms, took " + elapsedMs);
    }
  }

  @Test
  void zeroDelayAddsNoMeasurableCost(@TempDir Path dir) {
    LogConfig config = LogConfig.defaultsFor(dir);
    try (DiskPartitionLog log = new DiskPartitionLog(config)) {
      long start = System.nanoTime();
      log.append(new LogRecord(0, 0, -1, -1, null, "v".getBytes()));
      long elapsedMs = (System.nanoTime() - start) / 1_000_000;
      assertTrue(elapsedMs < 200, "expected append to be fast with zero delay, took " + elapsedMs);
    }
  }
}
