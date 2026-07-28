package io.minikafka.protocol;

/**
 * Client → Coordinator: join (or re-join) a consumer group. An empty {@code memberId} means "first
 * join, assign me an id"; a non-empty one is used by a member re-joining after eviction to request
 * the same identity (the coordinator mints a fresh id regardless, since membership is in-memory
 * only and a stale id cannot be trusted after a coordinator restart).
 */
public record JoinGroupReq(long correlationId, String group, String memberId, String topic)
    implements Message {

  public JoinGroupReq {
    if (group == null) {
      throw new IllegalArgumentException("group must not be null");
    }
    if (memberId == null) {
      throw new IllegalArgumentException("memberId must not be null");
    }
    if (topic == null) {
      throw new IllegalArgumentException("topic must not be null");
    }
  }

  @Override
  public MessageType type() {
    return MessageType.JOIN_GROUP_REQ;
  }
}
