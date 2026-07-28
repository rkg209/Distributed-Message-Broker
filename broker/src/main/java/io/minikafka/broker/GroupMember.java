package io.minikafka.broker;

import java.util.List;

/**
 * A consumer group member as tracked by {@link GroupCoordinator}. {@code assignment} is always the
 * member's last-acknowledged (currently owned) partitions — it is only replaced once the member
 * acks the generation that reassigned it, never eagerly at rebalance time — so a member mid-barrier
 * still reports the partitions it must revoke.
 */
final class GroupMember {

  final String memberId;
  List<Integer> assignment;
  int ackedGeneration;
  long lastHeartbeatNanos;

  GroupMember(
      String memberId, List<Integer> assignment, int ackedGeneration, long lastHeartbeatNanos) {
    this.memberId = memberId;
    this.assignment = assignment;
    this.ackedGeneration = ackedGeneration;
    this.lastHeartbeatNanos = lastHeartbeatNanos;
  }
}
