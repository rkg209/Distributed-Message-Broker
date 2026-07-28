package io.minikafka.protocol;

import java.util.List;

/**
 * Coordinator → Client: the piggybacked rebalance notification. There is no server-push channel
 * (the wire protocol is strictly request/response), so a stale-generation heartbeat is how a member
 * learns a rebalance is underway, and a repeated heartbeat is how the coordinator learns the member
 * has acked it.
 */
public record GroupHeartbeatResp(
    long correlationId, byte status, int generation, List<Integer> assignment) implements Message {

  /** Member is on the current generation with its full assignment; free to poll. */
  public static final byte OK = 0;

  /**
   * Member's generation is stale. The member must stop polling, commit offsets for every partition
   * in {@code assignment} (its previous, now-revoked assignment), then heartbeat again at {@code
   * generation} to acknowledge.
   */
  public static final byte REBALANCE_IN_PROGRESS = 1;

  /** Member has acked but the barrier has not yet cleared for every member; stay paused. */
  public static final byte AWAITING_SYNC = 2;

  /** The coordinator has no record of this {@code memberId} (evicted, or coordinator restarted). */
  public static final byte UNKNOWN_MEMBER = 3;

  public GroupHeartbeatResp {
    if (assignment == null) {
      throw new IllegalArgumentException("assignment must not be null");
    }
    assignment = List.copyOf(assignment);
  }

  @Override
  public MessageType type() {
    return MessageType.GROUP_HEARTBEAT_RESP;
  }
}
