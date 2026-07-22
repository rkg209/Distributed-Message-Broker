package io.minikafka.protocol;

import java.util.Arrays;
import java.util.List;

/**
 * Broker → Broker: Raft AppendEntries RPC, routed to a specific partition's Raft group by {@code
 * (topic, partition)} — required for multi-Raft demultiplexing over one socket namespace.
 */
public record AppendEntriesReq(
    long correlationId,
    String topic,
    int partition,
    long term,
    int leaderId,
    long prevLogIndex,
    long prevLogTerm,
    List<Entry> entries,
    long leaderCommit)
    implements Message {

  /**
   * One replicated log entry: {@code term}, {@code index}, and the opaque state-machine command.
   */
  public record Entry(long term, long index, byte[] command) {

    public Entry {
      if (command == null) {
        throw new IllegalArgumentException("command must not be null");
      }
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (!(o instanceof Entry other)) {
        return false;
      }
      return term == other.term && index == other.index && Arrays.equals(command, other.command);
    }

    @Override
    public int hashCode() {
      int result = Long.hashCode(term);
      result = 31 * result + Long.hashCode(index);
      result = 31 * result + Arrays.hashCode(command);
      return result;
    }
  }

  public AppendEntriesReq {
    if (topic == null) {
      throw new IllegalArgumentException("topic must not be null");
    }
    if (entries == null) {
      throw new IllegalArgumentException("entries must not be null");
    }
    entries = List.copyOf(entries);
  }

  @Override
  public MessageType type() {
    return MessageType.APPEND_ENTRIES_REQ;
  }
}
