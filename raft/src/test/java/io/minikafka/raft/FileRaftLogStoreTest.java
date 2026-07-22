package io.minikafka.raft;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** append/get/getFrom/truncateFrom, reopen recovery, and torn-tail discard on recovery. */
class FileRaftLogStoreTest {

  private static RaftEntry entry(long term, long index, String cmd) {
    return new RaftEntry(term, index, cmd.getBytes(StandardCharsets.UTF_8));
  }

  @Test
  void appendGetAndGetFrom(@TempDir Path dir) {
    try (FileRaftLogStore store = FileRaftLogStore.open(dir)) {
      store.append(entry(1, 1, "a"));
      store.append(entry(1, 2, "b"));
      store.append(entry(2, 3, "c"));

      assertEquals(3, store.lastIndex());
      assertEquals(2, store.lastTerm());
      assertArrayEquals("b".getBytes(StandardCharsets.UTF_8), store.get(2).command());
      assertNull(store.get(4));

      List<RaftEntry> from2 = store.getFrom(2, 10);
      assertEquals(2, from2.size());
      assertEquals(2, from2.get(0).index());
      assertEquals(3, from2.get(1).index());
    }
  }

  @Test
  void truncateFromDropsTailAndAllowsReappend(@TempDir Path dir) {
    try (FileRaftLogStore store = FileRaftLogStore.open(dir)) {
      store.append(entry(1, 1, "a"));
      store.append(entry(1, 2, "b"));
      store.append(entry(1, 3, "c"));

      store.truncateFrom(2);
      assertEquals(1, store.lastIndex());
      assertNull(store.get(2));
      assertNull(store.get(3));

      store.append(entry(2, 2, "b2"));
      assertEquals(2, store.lastIndex());
      assertArrayEquals("b2".getBytes(StandardCharsets.UTF_8), store.get(2).command());
    }
  }

  @Test
  void reopenRecoversPreviouslyAppendedEntries(@TempDir Path dir) {
    try (FileRaftLogStore store = FileRaftLogStore.open(dir)) {
      store.append(entry(1, 1, "a"));
      store.append(entry(1, 2, "b"));
    }

    try (FileRaftLogStore reopened = FileRaftLogStore.open(dir)) {
      assertEquals(2, reopened.lastIndex());
      assertArrayEquals("a".getBytes(StandardCharsets.UTF_8), reopened.get(1).command());
      assertArrayEquals("b".getBytes(StandardCharsets.UTF_8), reopened.get(2).command());
    }
  }

  @Test
  void tornTailIsDiscardedOnRecovery(@TempDir Path dir) throws Exception {
    try (FileRaftLogStore store = FileRaftLogStore.open(dir)) {
      store.append(entry(1, 1, "a"));
      store.append(entry(1, 2, "b"));
    }

    Path logFile = dir.resolve("raft.log");
    long size = java.nio.file.Files.size(logFile);
    // Append a few garbage bytes to simulate a torn write that never completed.
    try (var channel =
        java.nio.channels.FileChannel.open(
            logFile,
            java.nio.file.StandardOpenOption.WRITE,
            java.nio.file.StandardOpenOption.APPEND)) {
      channel.write(java.nio.ByteBuffer.wrap(new byte[] {1, 2, 3, 4, 5, 6, 7}));
    }
    assertEquals(size + 7, java.nio.file.Files.size(logFile));

    try (FileRaftLogStore recovered = FileRaftLogStore.open(dir)) {
      assertEquals(2, recovered.lastIndex());
      assertArrayEquals("b".getBytes(StandardCharsets.UTF_8), recovered.get(2).command());
    }

    // Recovery must have truncated the torn tail from disk, not just hidden it in memory.
    assertEquals(size, java.nio.file.Files.size(logFile));
  }
}
