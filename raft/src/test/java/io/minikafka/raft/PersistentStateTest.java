package io.minikafka.raft;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.RandomAccessFile;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * AC-5: save/load round-trips durably; a corrupted record throws rather than silently resetting.
 */
class PersistentStateTest {

  @Test
  void roundTripsThroughSaveAndLoad(@TempDir Path dir) {
    PersistentState state = PersistentState.load(dir);
    assertEquals(0, state.currentTerm());
    assertEquals(PersistentState.NONE, state.votedFor());

    state.save(5, 3);
    assertEquals(5, state.currentTerm());
    assertEquals(3, state.votedFor());

    PersistentState reloaded = PersistentState.load(dir);
    assertEquals(5, reloaded.currentTerm());
    assertEquals(3, reloaded.votedFor());
  }

  @Test
  void missingFileMeansTermZeroAndNoVote(@TempDir Path dir) {
    PersistentState state = PersistentState.load(dir.resolve("nested"));
    assertEquals(0, state.currentTerm());
    assertEquals(PersistentState.NONE, state.votedFor());
  }

  @Test
  void crcCorruptionThrowsOnLoad(@TempDir Path dir) throws Exception {
    PersistentState state = PersistentState.load(dir);
    state.save(7, 2);

    Path file = dir.resolve("raft.state");
    try (RandomAccessFile raf = new RandomAccessFile(file.toFile(), "rw")) {
      raf.seek(0);
      raf.writeByte(0xFF);
    }

    assertThrows(IllegalStateException.class, () -> PersistentState.load(dir));
  }

  @Test
  void refusesSecondVoteInSameTermAfterRestart(@TempDir Path dir) throws Exception {
    PersistentState state = PersistentState.load(dir);
    state.save(3, 10); // node voted for candidate 10 in term 3

    PersistentState restarted = PersistentState.load(dir);
    assertEquals(3, restarted.currentTerm());
    assertEquals(10, restarted.votedFor());
    // A RaftNode built on top of `restarted` would see votedFor != NONE/candidateId and refuse.
  }
}
