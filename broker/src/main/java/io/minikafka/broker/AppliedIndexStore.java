package io.minikafka.broker;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

/**
 * Durable single-value marker: the highest Raft log index whose {@code apply()} outcome (append,
 * duplicate, gap, or no-op) has already been decided in a prior run. {@code RaftNode} itself
 * persists no apply-progress marker and always replays every committed entry from index 1 on
 * restart, so this file is what lets {@link PartitionReplica#apply} skip re-deciding entries whose
 * outcome is already durable.
 *
 * <p>{@code PartitionLog#nextOffset()} alone is not enough for this: it only advances on an {@code
 * APPEND} outcome, so it undercounts whenever a committed {@code DUPLICATE} or {@code GAP} entry —
 * which occupies a Raft index but appends nothing — precedes a later entry in the same replay
 * window. Every {@link #record} is followed by {@code force(true)}, mirroring {@code OffsetStore}'s
 * EVERY_WRITE durability; the caller must only call it for an index whose associated {@code
 * PartitionLog} append (if any) is already itself durable.
 */
final class AppliedIndexStore implements AutoCloseable {

  private final FileChannel channel;
  private long lastAppliedIndex;

  AppliedIndexStore(Path file) {
    try {
      Files.createDirectories(file.getParent());
      this.channel =
          FileChannel.open(
              file, StandardOpenOption.CREATE, StandardOpenOption.READ, StandardOpenOption.WRITE);
      this.lastAppliedIndex = recover();
    } catch (IOException e) {
      throw new UncheckedIOException("Failed to open applied-index marker at " + file, e);
    }
  }

  private long recover() throws IOException {
    if (channel.size() < Long.BYTES) {
      return 0;
    }
    ByteBuffer buf = ByteBuffer.allocate(Long.BYTES);
    channel.position(0);
    int read = 0;
    while (read < Long.BYTES) {
      int n = channel.read(buf);
      if (n < 0) {
        return 0; // torn/short write from a crash mid-record; treat as "nothing applied yet"
      }
      read += n;
    }
    buf.flip();
    return buf.getLong();
  }

  /** The highest Raft index already durably applied in a prior run, or {@code 0} if none. */
  long lastAppliedIndexAtStartup() {
    return lastAppliedIndex;
  }

  /**
   * Durably records {@code index} as applied. Must be called with a monotonically increasing index.
   */
  void record(long index) {
    try {
      ByteBuffer buf = ByteBuffer.allocate(Long.BYTES);
      buf.putLong(index);
      buf.flip();
      channel.position(0);
      while (buf.hasRemaining()) {
        channel.write(buf);
      }
      channel.force(true);
      lastAppliedIndex = index;
    } catch (IOException e) {
      throw new UncheckedIOException("Failed to persist applied index " + index, e);
    }
  }

  @Override
  public void close() {
    try {
      channel.close();
    } catch (IOException e) {
      throw new UncheckedIOException("Failed to close applied-index marker", e);
    }
  }
}
