package io.minikafka.protocol;

/**
 * Client → Coordinator: liveness ping that doubles as the rebalance acknowledgement — a member
 * re-heartbeats with the coordinator-supplied {@code generation} once it has paused and committed
 * its revoked partitions, which is what un-blocks the barrier in {@link GroupHeartbeatResp}.
 */
public record GroupHeartbeatReq(long correlationId, String group, String memberId, int generation)
    implements Message {

  public GroupHeartbeatReq {
    if (group == null) {
      throw new IllegalArgumentException("group must not be null");
    }
    if (memberId == null) {
      throw new IllegalArgumentException("memberId must not be null");
    }
  }

  @Override
  public MessageType type() {
    return MessageType.GROUP_HEARTBEAT_REQ;
  }
}
