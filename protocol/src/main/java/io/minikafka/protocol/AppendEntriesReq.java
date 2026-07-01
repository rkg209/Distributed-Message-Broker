package io.minikafka.protocol;

/**
 * Broker → Broker: Raft AppendEntries RPC. Spec 01 carries only the fields needed to exercise the
 * codec; the full entry list and log-matching fields are added in the Raft specs (06+).
 */
public record AppendEntriesReq(long correlationId, long term, int leaderId) implements Message {

  @Override
  public MessageType type() {
    return MessageType.APPEND_ENTRIES_REQ;
  }
}
