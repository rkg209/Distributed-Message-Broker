package io.minikafka.log;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.nio.file.Path;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class OffsetIndexTest {

  @Test
  void sparseEntriesWrittenOnlyAtInterval(@TempDir Path dir) throws Exception {
    OffsetIndex index =
        OffsetIndex.open(dir.resolve("00000000000000000000.index"), 1000, 100, new LogStats());
    try {
      // Below the interval: no entry.
      index.maybeAppend(0, 0, 50);
      assertEquals(0, index.entryCount());

      // At/above the interval: an entry is written.
      index.maybeAppend(1, 50, 100);
      assertEquals(1, index.entryCount());

      index.maybeAppend(2, 150, 40);
      assertEquals(1, index.entryCount());
    } finally {
      index.close();
    }
  }

  @Test
  void lookupReturnsLargestEntryLessOrEqualToTarget(@TempDir Path dir) throws Exception {
    OffsetIndex index =
        OffsetIndex.open(dir.resolve("00000000000000000000.index"), 1000, 1, new LogStats());
    try {
      index.append(0, 0);
      index.append(10, 500);
      index.append(20, 1000);

      assertEquals(0, index.lookup(0));
      assertEquals(0, index.lookup(5));
      assertEquals(500, index.lookup(10));
      assertEquals(500, index.lookup(15));
      assertEquals(1000, index.lookup(20));
      assertEquals(1000, index.lookup(1000));
    } finally {
      index.close();
    }
  }

  @Test
  void lookupBelowFirstEntryReturnsZero(@TempDir Path dir) throws Exception {
    OffsetIndex index =
        OffsetIndex.open(dir.resolve("00000000000000000000.index"), 1000, 1, new LogStats());
    try {
      index.append(10, 500);
      assertEquals(0, index.lookup(0));
      assertEquals(0, index.lookup(9));
    } finally {
      index.close();
    }
  }

  @Test
  void lookupOnEmptyIndexReturnsZero(@TempDir Path dir) throws Exception {
    OffsetIndex index =
        OffsetIndex.open(dir.resolve("00000000000000000000.index"), 1000, 1, new LogStats());
    try {
      assertEquals(0, index.lookup(42));
    } finally {
      index.close();
    }
  }

  @Test
  void reopenFromFilePreservesEntries(@TempDir Path dir) throws Exception {
    Path file = dir.resolve("00000000000000000000.index");
    OffsetIndex index = OffsetIndex.open(file, 1000, 1, new LogStats());
    index.append(0, 0);
    index.append(10, 500);
    index.append(20, 1000);
    index.close();

    OffsetIndex reopened = OffsetIndex.open(file, 1000, 1, new LogStats());
    try {
      assertEquals(3, reopened.entryCount());
      assertEquals(500, reopened.lookup(15));
      assertEquals(1000, reopened.lookup(999));
    } finally {
      reopened.close();
    }
  }

  @Test
  void truncateToDiscardsEntriesAboveOffset(@TempDir Path dir) throws Exception {
    OffsetIndex index =
        OffsetIndex.open(dir.resolve("00000000000000000000.index"), 1000, 1, new LogStats());
    try {
      index.append(0, 0);
      index.append(10, 500);
      index.append(20, 1000);
      index.truncateTo(10);
      assertEquals(2, index.entryCount());
      assertEquals(500, index.lookup(999));
    } finally {
      index.close();
    }
  }
}
