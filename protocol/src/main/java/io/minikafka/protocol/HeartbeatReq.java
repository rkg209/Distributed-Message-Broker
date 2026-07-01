package io.minikafka.protocol;

/** Broker → Broker: leader liveness heartbeat (empty AppendEntries in Raft terms). */
public record HeartbeatReq(long correlationId, long term, int leaderId) implements Message {

  @Override
  public MessageType type() {
    return MessageType.HEARTBEAT_REQ;
  }
}
