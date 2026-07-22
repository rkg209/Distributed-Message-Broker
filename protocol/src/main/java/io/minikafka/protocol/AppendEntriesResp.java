package io.minikafka.protocol;

/**
 * Broker → Broker: Raft AppendEntries reply, mirroring {@code raft.AppendEntriesResponse}. {@code
 * conflictIndex}/{@code conflictTerm} let the leader back off {@code nextIndex} by a whole term per
 * round-trip; {@code followerLastIndex} lets the leader set {@code matchIndex} directly.
 */
public record AppendEntriesResp(
    long correlationId,
    String topic,
    int partition,
    long term,
    boolean success,
    long conflictIndex,
    long conflictTerm,
    long followerLastIndex)
    implements Message {

  public AppendEntriesResp {
    if (topic == null) {
      throw new IllegalArgumentException("topic must not be null");
    }
  }

  @Override
  public MessageType type() {
    return MessageType.APPEND_ENTRIES_RESP;
  }
}
