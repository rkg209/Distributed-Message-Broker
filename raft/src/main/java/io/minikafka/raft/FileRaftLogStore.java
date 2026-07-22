package io.minikafka.raft;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.CRC32C;

/**
 * Durable append-only {@link RaftLogStore} backed by a single file, {@code stateDir/raft.log}.
 * Record framing mirrors {@code log/RecordCodec}: {@code [4-byte length][8-byte term][8-byte
 * index][4-byte commandLen][command][4-byte CRC32C]}, all big-endian, where {@code length} covers
 * everything after itself. Recovery on open scans forward and discards the first short or
 * CRC-invalid record and everything after it — the same torn-tail policy {@code log/LogRecovery}
 * uses.
 *
 * <p>No log compaction (per CLAUDE.md): {@link #firstIndex()} is always 1.
 */
public final class FileRaftLogStore implements RaftLogStore {

  private static final int HEADER_BYTES = 8 + 8 + 4; // term + index + commandLen
  private static final int TRAILER_BYTES = 4; // CRC32C

  private final FileChannel channel;
  private final List<RaftEntry> entries = new ArrayList<>();
  private final List<Long> positions = new ArrayList<>(); // start of each record's length-prefix

  private FileRaftLogStore(FileChannel channel) {
    this.channel = channel;
  }

  public static FileRaftLogStore open(Path stateDir) {
    try {
      Files.createDirectories(stateDir);
      Path logFile = stateDir.resolve("raft.log");
      FileChannel channel =
          FileChannel.open(
              logFile,
              StandardOpenOption.CREATE,
              StandardOpenOption.READ,
              StandardOpenOption.WRITE);
      FileRaftLogStore store = new FileRaftLogStore(channel);
      store.recover();
      return store;
    } catch (IOException e) {
      throw new UncheckedIOException("failed to open raft log at " + stateDir, e);
    }
  }

  private void recover() throws IOException {
    long pos = 0;
    long size = channel.size();
    while (pos < size) {
      if (pos + 4 > size) {
        break;
      }
      ByteBuffer lenBuf = ByteBuffer.allocate(4).order(ByteOrder.BIG_ENDIAN);
      readFully(lenBuf, pos);
      lenBuf.flip();
      int length = lenBuf.getInt();
      long recordStart = pos + 4;
      if (length < HEADER_BYTES + TRAILER_BYTES || recordStart + length > size) {
        break; // torn tail
      }
      ByteBuffer body = ByteBuffer.allocate(length).order(ByteOrder.BIG_ENDIAN);
      readFully(body, recordStart);
      body.flip();
      long term = body.getLong();
      long index = body.getLong();
      int commandLen = body.getInt();
      if (commandLen < 0 || HEADER_BYTES + commandLen + TRAILER_BYTES != length) {
        break;
      }
      byte[] command = new byte[commandLen];
      body.get(command);
      int storedCrc = body.getInt();
      CRC32C crc = new CRC32C();
      crc.update(body.array(), 0, HEADER_BYTES + commandLen);
      if ((int) crc.getValue() != storedCrc) {
        break; // torn/corrupt tail
      }
      entries.add(new RaftEntry(term, index, command));
      positions.add(pos);
      pos = recordStart + length;
    }
    channel.truncate(pos);
    channel.position(pos);
  }

  private void readFully(ByteBuffer buf, long position) throws IOException {
    while (buf.hasRemaining()) {
      int n = channel.read(buf, position + (buf.position()));
      if (n < 0) {
        throw new IOException("unexpected EOF reading raft log");
      }
    }
  }

  @Override
  public synchronized void append(RaftEntry entry) {
    long expected = lastIndex() + 1;
    if (entry.index() != expected) {
      throw new IllegalArgumentException(
          "expected next index " + expected + " but got " + entry.index());
    }
    byte[] command = entry.command();
    int length = HEADER_BYTES + command.length + TRAILER_BYTES;
    ByteBuffer buf = ByteBuffer.allocate(4 + length).order(ByteOrder.BIG_ENDIAN);
    buf.putInt(length);
    int bodyStart = buf.position();
    buf.putLong(entry.term());
    buf.putLong(entry.index());
    buf.putInt(command.length);
    buf.put(command);
    CRC32C crc = new CRC32C();
    crc.update(buf.array(), bodyStart, HEADER_BYTES + command.length);
    buf.putInt((int) crc.getValue());
    buf.flip();

    try {
      long pos = channel.position();
      while (buf.hasRemaining()) {
        channel.write(buf);
      }
      channel.force(true);
      entries.add(entry);
      positions.add(pos);
    } catch (IOException e) {
      throw new UncheckedIOException("failed to append raft log entry", e);
    }
  }

  @Override
  public synchronized RaftEntry get(long index) {
    int pos = (int) (index - firstIndex());
    if (pos < 0 || pos >= entries.size()) {
      return null;
    }
    return entries.get(pos);
  }

  @Override
  public synchronized List<RaftEntry> getFrom(long index, int maxEntries) {
    List<RaftEntry> result = new ArrayList<>();
    for (long i = index; i < index + maxEntries; i++) {
      RaftEntry e = get(i);
      if (e == null) {
        break;
      }
      result.add(e);
    }
    return result;
  }

  @Override
  public synchronized long lastIndex() {
    return entries.isEmpty() ? 0 : entries.get(entries.size() - 1).index();
  }

  @Override
  public synchronized long lastTerm() {
    return entries.isEmpty() ? 0 : entries.get(entries.size() - 1).term();
  }

  @Override
  public synchronized long firstIndex() {
    return 1;
  }

  @Override
  public synchronized void truncateFrom(long fromIndex) {
    int pos = (int) (fromIndex - firstIndex());
    if (pos < 0) {
      pos = 0;
    }
    if (pos >= entries.size()) {
      return;
    }
    long truncateAt = positions.get(pos);
    try {
      channel.truncate(truncateAt);
      channel.position(truncateAt);
    } catch (IOException e) {
      throw new UncheckedIOException("failed to truncate raft log", e);
    }
    entries.subList(pos, entries.size()).clear();
    positions.subList(pos, positions.size()).clear();
  }

  @Override
  public synchronized void close() {
    try {
      channel.close();
    } catch (IOException e) {
      throw new UncheckedIOException("failed to close raft log", e);
    }
  }
}
