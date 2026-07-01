package io.minikafka.protocol;

/** Broker → Broker: heartbeat acknowledgement. */
public record HeartbeatResp(long correlationId, long term) implements Message {

  @Override
  public MessageType type() {
    return MessageType.HEARTBEAT_RESP;
  }
}
