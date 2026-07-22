package io.minikafka.raft;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.zip.CRC32C;

/**
 * Durable storage for the two fields Raft §5.1 requires be persisted before responding to any RPC:
 * {@code currentTerm} and {@code votedFor}. Format is a fixed 24-byte record: {@code [8-byte
 * term][4-byte votedFor][4-byte CRC32C][8-byte magic]}. Every {@link #save} writes to a {@code
 * .tmp} file, fsyncs it, then atomically renames over {@code raft.state} so a crash mid-write can
 * never leave a torn record in place.
 */
public final class PersistentState {

  public static final int NONE = -1;
  private static final long MAGIC = 0x52414654535441L; // "RAFTSTA"
  private static final int RECORD_BYTES = 24;

  private final Path file;
  private final Path tmpFile;
  private long currentTerm;
  private int votedFor;

  private PersistentState(Path file, long currentTerm, int votedFor) {
    this.file = file;
    this.tmpFile = file.resolveSibling(file.getFileName() + ".tmp");
    this.currentTerm = currentTerm;
    this.votedFor = votedFor;
  }

  /** Loads state from {@code stateDir/raft.state}, or returns {@code (term=0, votedFor=NONE)}. */
  public static PersistentState load(Path stateDir) {
    try {
      Files.createDirectories(stateDir);
      Path file = stateDir.resolve("raft.state");
      if (!Files.exists(file)) {
        return new PersistentState(file, 0L, NONE);
      }
      byte[] bytes = Files.readAllBytes(file);
      if (bytes.length != RECORD_BYTES) {
        throw new IllegalStateException(
            "raft.state is corrupt: expected " + RECORD_BYTES + " bytes, got " + bytes.length);
      }
      ByteBuffer buf = ByteBuffer.wrap(bytes).order(ByteOrder.BIG_ENDIAN);
      long term = buf.getLong();
      int voted = buf.getInt();
      int crc = buf.getInt();
      long magic = buf.getLong();
      if (magic != MAGIC) {
        throw new IllegalStateException("raft.state is corrupt: bad magic");
      }
      CRC32C check = new CRC32C();
      check.update(bytes, 0, 12);
      if ((int) check.getValue() != crc) {
        throw new IllegalStateException("raft.state is corrupt: CRC mismatch");
      }
      return new PersistentState(file, term, voted);
    } catch (IOException e) {
      throw new UncheckedIOException("failed to load raft state from " + stateDir, e);
    }
  }

  public synchronized long currentTerm() {
    return currentTerm;
  }

  public synchronized int votedFor() {
    return votedFor;
  }

  /** Persists {@code (term, votedFor)} durably. Returns only after the bytes are fsynced. */
  public synchronized void save(long term, int votedFor) {
    ByteBuffer buf = ByteBuffer.allocate(RECORD_BYTES).order(ByteOrder.BIG_ENDIAN);
    buf.putLong(term);
    buf.putInt(votedFor);
    CRC32C crc = new CRC32C();
    crc.update(buf.array(), 0, 12);
    buf.putInt((int) crc.getValue());
    buf.putLong(MAGIC);
    buf.flip();

    try (FileChannel ch =
        FileChannel.open(
            tmpFile,
            StandardOpenOption.CREATE,
            StandardOpenOption.WRITE,
            StandardOpenOption.TRUNCATE_EXISTING)) {
      while (buf.hasRemaining()) {
        ch.write(buf);
      }
      ch.force(true);
    } catch (IOException e) {
      throw new UncheckedIOException("failed to write raft state to " + tmpFile, e);
    }

    try {
      Files.move(
          tmpFile, file, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
    } catch (IOException e) {
      throw new UncheckedIOException("failed to atomically install raft state at " + file, e);
    }

    this.currentTerm = term;
    this.votedFor = votedFor;
  }
}
