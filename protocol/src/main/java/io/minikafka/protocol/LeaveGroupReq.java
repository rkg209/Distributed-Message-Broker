package io.minikafka.protocol;

/** Client → Coordinator: a clean, voluntary departure from a consumer group. */
public record LeaveGroupReq(long correlationId, String group, String memberId) implements Message {

  public LeaveGroupReq {
    if (group == null) {
      throw new IllegalArgumentException("group must not be null");
    }
    if (memberId == null) {
      throw new IllegalArgumentException("memberId must not be null");
    }
  }

  @Override
  public MessageType type() {
    return MessageType.LEAVE_GROUP_REQ;
  }
}
