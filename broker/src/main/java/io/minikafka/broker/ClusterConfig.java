package io.minikafka.broker;

import io.minikafka.protocol.BrokerInfo;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Static cluster membership and partition assignment, parsed from environment variables.
 *
 * <p>{@code BROKER_LIST} — {@code id@host:port} pairs, comma-separated, e.g. {@code
 * "1@broker-1:9092,2@broker-2:9092,3@broker-3:9092"}. Must include the broker's own {@code
 * BROKER_ID}.
 *
 * <p>{@code PARTITION_ASSIGNMENTS} — {@code topic:partition=leaderId,replicaId...}, entries
 * separated by {@code ;}, e.g. {@code "orders:0=1,2,3;orders:1=2,3,1"}. The first id after {@code
 * =} is the leader and the full comma list is the replica set. Absent means every partition
 * assigned to this config falls back to single-broker behaviour via {@link #assignmentFor}
 * returning empty.
 *
 * <p>{@code REPLICATION_FACTOR} — every assignment's replica count must equal this (default 1).
 *
 * <p>{@code CONTROLLER_ID} — must name a listed broker (default: the lowest broker id).
 */
public record ClusterConfig(
    List<BrokerInfo> brokers,
    Map<TopicPartition, PartitionAssignment> assignments,
    int replicationFactor,
    int controllerId) {

  public ClusterConfig {
    if (brokers == null) {
      throw new IllegalArgumentException("brokers must not be null");
    }
    if (assignments == null) {
      throw new IllegalArgumentException("assignments must not be null");
    }
    brokers = List.copyOf(brokers);
    assignments = Map.copyOf(assignments);
  }

  /** One partition's leader and full replica set (leader included). */
  public record PartitionAssignment(int leaderId, List<Integer> replicaIds) {

    public PartitionAssignment {
      if (replicaIds == null) {
        throw new IllegalArgumentException("replicaIds must not be null");
      }
      replicaIds = List.copyOf(replicaIds);
    }
  }

  /** Looks up a broker by id. */
  public Optional<BrokerInfo> broker(int id) {
    return brokers.stream().filter(b -> b.brokerId() == id).findFirst();
  }

  /** Every configured broker other than {@code selfId}. */
  public List<BrokerInfo> peersOf(int selfId) {
    return brokers.stream().filter(b -> b.brokerId() != selfId).toList();
  }

  /** The configured leader/replicas for {@code tp}, if any. */
  public Optional<PartitionAssignment> assignmentFor(TopicPartition tp) {
    return Optional.ofNullable(assignments.get(tp));
  }

  /** Whether {@code id} is the static controller. */
  public boolean isController(int id) {
    return controllerId == id;
  }

  /** A degenerate single-broker cluster: used when {@code BROKER_LIST} is unset. */
  public static ClusterConfig singleBroker(BrokerInfo self) {
    return new ClusterConfig(List.of(self), Map.of(), 1, self.brokerId());
  }

  /**
   * Parses cluster configuration from the raw environment values, cross-checking assignments
   * against {@code topicConfig}'s partition counts. Throws {@link IllegalStateException} on any
   * malformed or inconsistent input — configuration errors must never fail silently.
   */
  public static ClusterConfig parse(
      String brokerListSpec,
      String assignmentsSpec,
      Integer replicationFactor,
      Integer controllerId,
      int selfBrokerId,
      TopicConfig topicConfig) {
    List<BrokerInfo> brokers = parseBrokerList(brokerListSpec);

    if (brokers.stream().noneMatch(b -> b.brokerId() == selfBrokerId)) {
      throw new IllegalStateException(
          "BROKER_ID " + selfBrokerId + " is not present in BROKER_LIST");
    }

    int factor = replicationFactor == null ? 1 : replicationFactor;
    if (factor <= 0) {
      throw new IllegalStateException("REPLICATION_FACTOR must be positive: " + factor);
    }

    int resolvedController;
    if (controllerId == null) {
      resolvedController =
          brokers.stream()
              .mapToInt(BrokerInfo::brokerId)
              .min()
              .orElseThrow(() -> new IllegalStateException("BROKER_LIST must not be empty"));
    } else {
      resolvedController = controllerId;
      if (brokers.stream().noneMatch(b -> b.brokerId() == resolvedController)) {
        throw new IllegalStateException(
            "CONTROLLER_ID " + resolvedController + " is not present in BROKER_LIST");
      }
    }

    Map<TopicPartition, PartitionAssignment> assignments =
        parseAssignments(assignmentsSpec, brokers, factor, topicConfig);

    return new ClusterConfig(brokers, assignments, factor, resolvedController);
  }

  private static List<BrokerInfo> parseBrokerList(String spec) {
    if (spec == null || spec.isBlank()) {
      throw new IllegalStateException("BROKER_LIST must not be blank");
    }
    List<BrokerInfo> brokers = new ArrayList<>();
    for (String entry : spec.split(",")) {
      String trimmed = entry.trim();
      if (trimmed.isEmpty()) {
        continue;
      }
      int at = trimmed.indexOf('@');
      int colon = trimmed.lastIndexOf(':');
      if (at <= 0 || colon <= at + 1 || colon == trimmed.length() - 1) {
        throw new IllegalStateException(
            "Malformed BROKER_LIST entry (expected id@host:port): " + trimmed);
      }
      int id;
      int port;
      try {
        id = Integer.parseInt(trimmed.substring(0, at));
      } catch (NumberFormatException e) {
        throw new IllegalStateException("Malformed BROKER_LIST broker id: " + trimmed, e);
      }
      String host = trimmed.substring(at + 1, colon);
      try {
        port = Integer.parseInt(trimmed.substring(colon + 1));
      } catch (NumberFormatException e) {
        throw new IllegalStateException("Malformed BROKER_LIST port: " + trimmed, e);
      }
      brokers.add(new BrokerInfo(id, host, port));
    }
    Map<Integer, Long> counts = new HashMap<>();
    for (BrokerInfo b : brokers) {
      counts.merge(b.brokerId(), 1L, Long::sum);
    }
    counts.forEach(
        (id, count) -> {
          if (count > 1) {
            throw new IllegalStateException("Duplicate broker id in BROKER_LIST: " + id);
          }
        });
    return brokers;
  }

  private static Map<TopicPartition, PartitionAssignment> parseAssignments(
      String spec, List<BrokerInfo> brokers, int replicationFactor, TopicConfig topicConfig) {
    Map<TopicPartition, PartitionAssignment> assignments = new HashMap<>();
    if (spec == null || spec.isBlank()) {
      return assignments;
    }
    for (String entry : spec.split(";")) {
      String trimmed = entry.trim();
      if (trimmed.isEmpty()) {
        continue;
      }
      int colon = trimmed.indexOf(':');
      int equals = trimmed.indexOf('=');
      if (colon <= 0 || equals <= colon + 1 || equals == trimmed.length() - 1) {
        throw new IllegalStateException(
            "Malformed PARTITION_ASSIGNMENTS entry (expected topic:partition=leader,replicas): "
                + trimmed);
      }
      String topic = trimmed.substring(0, colon);
      int partitionId;
      try {
        partitionId = Integer.parseInt(trimmed.substring(colon + 1, equals));
      } catch (NumberFormatException e) {
        throw new IllegalStateException(
            "Malformed PARTITION_ASSIGNMENTS partition id: " + trimmed, e);
      }
      int partitionCount = topicConfig.partitionCountFor(topic);
      if (partitionId >= partitionCount) {
        throw new IllegalStateException(
            "PARTITION_ASSIGNMENTS partition "
                + partitionId
                + " for topic "
                + topic
                + " is out of range (topic has "
                + partitionCount
                + " partitions)");
      }
      List<Integer> replicaIds = new ArrayList<>();
      for (String idStr : trimmed.substring(equals + 1).split(",")) {
        try {
          replicaIds.add(Integer.parseInt(idStr.trim()));
        } catch (NumberFormatException e) {
          throw new IllegalStateException(
              "Malformed PARTITION_ASSIGNMENTS replica id: " + trimmed, e);
        }
      }
      if (replicaIds.isEmpty()) {
        throw new IllegalStateException("PARTITION_ASSIGNMENTS entry has no replicas: " + trimmed);
      }
      if (replicaIds.size() != replicationFactor) {
        throw new IllegalStateException(
            "PARTITION_ASSIGNMENTS entry "
                + trimmed
                + " has "
                + replicaIds.size()
                + " replicas, expected REPLICATION_FACTOR="
                + replicationFactor);
      }
      int leaderId = replicaIds.get(0);
      if (!replicaIds.contains(leaderId)) {
        throw new IllegalStateException(
            "PARTITION_ASSIGNMENTS leader " + leaderId + " not in replica list: " + trimmed);
      }
      for (int replicaId : replicaIds) {
        if (brokers.stream().noneMatch(b -> b.brokerId() == replicaId)) {
          throw new IllegalStateException(
              "PARTITION_ASSIGNMENTS references unknown broker id "
                  + replicaId
                  + " in entry: "
                  + trimmed);
        }
      }
      TopicPartition tp = new TopicPartition(topic, partitionId);
      if (assignments.containsKey(tp)) {
        throw new IllegalStateException("Duplicate PARTITION_ASSIGNMENTS entry for " + tp);
      }
      assignments.put(tp, new PartitionAssignment(leaderId, replicaIds));
    }
    return assignments;
  }
}
