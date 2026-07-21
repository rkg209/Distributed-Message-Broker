package io.minikafka.log;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Path;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Criterion 4: a lookup at a high offset costs O(log n) seeks, not a full scan. Asserted by
 * counting seeks via {@link LogStats}, not by timing wall-clock.
 */
class IndexLookupTest {

  @Test
  void lookupNearTheEndCostsBoundedSeeksNotFullScan(@TempDir Path dir) {
    LogConfig config = new LogConfig(dir, FsyncPolicy.OS_MANAGED, 1000, 64 * 1024, 256, -1, -1);
    int recordCount = 5000;
    byte[] value = new byte[20];

    DiskPartitionLog log = new DiskPartitionLog(config);
    try {
      for (int i = 0; i < recordCount; i++) {
        log.append(new LogRecord(0, 0, null, value));
      }

      long segmentLookupsBefore = log.segmentLookups();
      long indexLookupsBefore = log.indexLookups();
      long recordScansBefore = log.recordScans();

      List<LogRecord> result = log.read(recordCount - 1, 1);

      assertEquals(1, result.size());
      assertEquals(recordCount - 1, result.get(0).offset());

      long segmentLookups = log.segmentLookups() - segmentLookupsBefore;
      long indexLookups = log.indexLookups() - indexLookupsBefore;
      long recordScans = log.recordScans() - recordScansBefore;

      assertEquals(1, segmentLookups, "expected a single O(log n) segment lookup");
      assertEquals(1, indexLookups, "expected a single O(log n) index binary search");
      assertTrue(
          recordScans < 50,
          "expected a bounded forward scan near the index interval, got " + recordScans);
    } finally {
      log.close();
    }
  }
}
