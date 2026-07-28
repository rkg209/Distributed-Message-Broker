package io.minikafka.protocol;

/**
 * Coordinator → Client: the minted membership identity and session tunables. Carries no assignment
 * — the member learns its partitions from the next {@link GroupHeartbeatResp} once the rebalance
 * barrier clears.
 */
public record JoinGroupResp(
    long correlationId,
    String memberId,
    int generation,
    long heartbeatIntervalMs,
    long sessionTimeoutMs)
    implements Message {

  public JoinGroupResp {
    if (memberId == null) {
      throw new IllegalArgumentException("memberId must not be null");
    }
  }

  @Override
  public MessageType type() {
    return MessageType.JOIN_GROUP_RESP;
  }
}
