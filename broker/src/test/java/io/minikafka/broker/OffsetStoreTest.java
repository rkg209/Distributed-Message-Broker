package io.minikafka.broker;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class OffsetStoreTest {

  @Test
  void appendThenReopenRecoversLastValuePerTopicPartition(@TempDir Path dir) throws IOException {
    try (OffsetStore store = new OffsetStore(dir, "group-a")) {
      store.append("orders", 0, 100L);
      store.append("orders", 1, 50L);
      store.append("orders", 0, 200L); // supersedes the first entry for (orders, 0)
    }

    try (OffsetStore reopened = new OffsetStore(dir, "group-a")) {
      Map<TopicPartition, Long> recovered = reopened.recoveredOffsets();
      assertEquals(200L, recovered.get(new TopicPartition("orders", 0)));
      assertEquals(50L, recovered.get(new TopicPartition("orders", 1)));
    }
  }

  @Test
  void freshStoreHasNoRecoveredOffsets(@TempDir Path dir) throws IOException {
    try (OffsetStore store = new OffsetStore(dir, "group-b")) {
      assertTrue(store.recoveredOffsets().isEmpty());
    }
  }

  @Test
  void truncatedTailRecoversToLastValidRecord(@TempDir Path dir) throws IOException {
    Path file;
    try (OffsetStore store = new OffsetStore(dir, "group-c")) {
      store.append("orders", 0, 10L);
      file = dir.resolve("group-c.offsets");
    }
    long fullLength = Files.size(file);
    // Torn write: chop off the last few bytes of the (only) record.
    try (RandomAccessFile raf = new RandomAccessFile(file.toFile(), "rw")) {
      raf.setLength(fullLength - 3);
    }

    try (OffsetStore reopened = new OffsetStore(dir, "group-c")) {
      assertTrue(reopened.recoveredOffsets().isEmpty());
    }
    assertEquals(0L, Files.size(file));
  }

  @Test
  void corruptCrcTruncatesToLastValidRecord(@TempDir Path dir) throws IOException {
    Path file;
    try (OffsetStore store = new OffsetStore(dir, "group-d")) {
      store.append("orders", 0, 10L);
      store.append("orders", 0, 20L);
      file = dir.resolve("group-d.offsets");
    }
    long fullLength = Files.size(file);
    // Flip the last byte (part of the second record's CRC field) to corrupt it.
    try (RandomAccessFile raf = new RandomAccessFile(file.toFile(), "rw")) {
      raf.seek(fullLength - 1);
      int original = raf.readByte();
      raf.seek(fullLength - 1);
      raf.writeByte(original ^ 0xFF);
    }

    try (OffsetStore reopened = new OffsetStore(dir, "group-d")) {
      Map<TopicPartition, Long> recovered = reopened.recoveredOffsets();
      assertEquals(10L, recovered.get(new TopicPartition("orders", 0)));
    }
  }

  @Test
  void rejectsGroupIdWithPathTraversal(@TempDir Path dir) {
    assertThrows(IllegalArgumentException.class, () -> new OffsetStore(dir, "../evil"));
    assertThrows(IllegalArgumentException.class, () -> new OffsetStore(dir, "a/b"));
  }
}
