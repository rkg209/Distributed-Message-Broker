package io.minikafka.protocol;

/** Broker → Broker: Raft AppendEntries reply. */
public record AppendEntriesResp(long correlationId, long term, boolean success) implements Message {

  @Override
  public MessageType type() {
    return MessageType.APPEND_ENTRIES_RESP;
  }
}
