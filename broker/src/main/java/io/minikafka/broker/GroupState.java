package io.minikafka.broker;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * In-memory coordinator state for one consumer group. All mutation happens with this object held as
 * the lock, so join/heartbeat/leave/reap on the same group serialize against each other while
 * different groups proceed independently.
 */
final class GroupState {

  enum Phase {
    STABLE,
    REBALANCING
  }

  final String group;
  final String topic;
  final Map<String, GroupMember> members = new LinkedHashMap<>();
  int generation;
  Phase phase = Phase.STABLE;
  Map<String, List<Integer>> pendingAssignments = Map.of();
  long rebalanceStartNanos;

  GroupState(String group, String topic) {
    this.group = group;
    this.topic = topic;
  }
}
