package io.minikafka.protocol;

/** Coordinator → Client: acknowledges a {@link LeaveGroupReq}. */
public record LeaveGroupResp(long correlationId, boolean ok) implements Message {

  @Override
  public MessageType type() {
    return MessageType.LEAVE_GROUP_RESP;
  }
}
