package io.minikafka.broker;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.minikafka.protocol.BrokerInfo;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.Test;

class ClusterConfigTest {

  private static final String BROKER_LIST = "1@broker-1:9092,2@broker-2:9093,3@broker-3:9094";

  @Test
  void parsesBrokerList() {
    ClusterConfig config = ClusterConfig.parse(BROKER_LIST, null, null, null, 1, defaultTopics());

    assertEquals(
        List.of(
            new BrokerInfo(1, "broker-1", 9092),
            new BrokerInfo(2, "broker-2", 9093),
            new BrokerInfo(3, "broker-3", 9094)),
        config.brokers());
  }

  @Test
  void peersOfExcludesSelf() {
    ClusterConfig config = ClusterConfig.parse(BROKER_LIST, null, null, null, 1, defaultTopics());

    List<Integer> peerIds = config.peersOf(1).stream().map(BrokerInfo::brokerId).toList();

    assertEquals(List.of(2, 3), peerIds);
  }

  @Test
  void defaultsControllerToLowestBrokerId() {
    ClusterConfig config = ClusterConfig.parse(BROKER_LIST, null, null, null, 2, defaultTopics());

    assertEquals(1, config.controllerId());
    assertTrue(config.isController(1));
    assertFalse(config.isController(2));
  }

  @Test
  void explicitControllerIdHonored() {
    ClusterConfig config = ClusterConfig.parse(BROKER_LIST, null, null, 3, 1, defaultTopics());

    assertEquals(3, config.controllerId());
  }

  @Test
  void throwsWhenControllerIdNotListed() {
    assertThrows(
        IllegalStateException.class,
        () -> ClusterConfig.parse(BROKER_LIST, null, null, 99, 1, defaultTopics()));
  }

  @Test
  void throwsWhenSelfBrokerIdNotInBrokerList() {
    assertThrows(
        IllegalStateException.class,
        () -> ClusterConfig.parse(BROKER_LIST, null, null, null, 99, defaultTopics()));
  }

  @Test
  void throwsOnDuplicateBrokerId() {
    assertThrows(
        IllegalStateException.class,
        () ->
            ClusterConfig.parse(
                "1@broker-1:9092,1@broker-2:9093", null, null, null, 1, defaultTopics()));
  }

  @Test
  void throwsOnMalformedBrokerListEntry() {
    assertThrows(
        IllegalStateException.class,
        () -> ClusterConfig.parse("broker-1:9092", null, null, null, 1, defaultTopics()));
  }

  @Test
  void parsesPartitionAssignments() {
    TopicConfig topics = TopicConfig.parse("orders:2", 1);
    ClusterConfig config =
        ClusterConfig.parse(BROKER_LIST, "orders:0=1,2,3;orders:1=2,3,1", 3, null, 1, topics);

    Optional<ClusterConfig.PartitionAssignment> a0 =
        config.assignmentFor(new TopicPartition("orders", 0));
    assertTrue(a0.isPresent());
    assertEquals(1, a0.get().leaderId());
    assertEquals(List.of(1, 2, 3), a0.get().replicaIds());

    Optional<ClusterConfig.PartitionAssignment> a1 =
        config.assignmentFor(new TopicPartition("orders", 1));
    assertTrue(a1.isPresent());
    assertEquals(2, a1.get().leaderId());
  }

  @Test
  void assignmentForUnassignedPartitionIsEmpty() {
    ClusterConfig config = ClusterConfig.parse(BROKER_LIST, null, null, null, 1, defaultTopics());

    assertTrue(config.assignmentFor(new TopicPartition("orders", 0)).isEmpty());
  }

  @Test
  void throwsWhenReplicaCountDoesNotMatchReplicationFactor() {
    TopicConfig topics = TopicConfig.parse("orders:1", 1);
    assertThrows(
        IllegalStateException.class,
        () -> ClusterConfig.parse(BROKER_LIST, "orders:0=1,2,3", 2, null, 1, topics));
  }

  @Test
  void throwsWhenAssignmentReferencesUnknownBroker() {
    TopicConfig topics = TopicConfig.parse("orders:1", 1);
    assertThrows(
        IllegalStateException.class,
        () -> ClusterConfig.parse(BROKER_LIST, "orders:0=1,2,99", 3, null, 1, topics));
  }

  @Test
  void throwsWhenAssignmentPartitionIdOutOfRange() {
    TopicConfig topics = TopicConfig.parse("orders:1", 1);
    assertThrows(
        IllegalStateException.class,
        () -> ClusterConfig.parse(BROKER_LIST, "orders:5=1,2,3", 3, null, 1, topics));
  }

  @Test
  void throwsOnDuplicatePartitionAssignment() {
    TopicConfig topics = TopicConfig.parse("orders:1", 1);
    assertThrows(
        IllegalStateException.class,
        () ->
            ClusterConfig.parse(BROKER_LIST, "orders:0=1,2,3;orders:0=2,3,1", 3, null, 1, topics));
  }

  @Test
  void singleBrokerFallback() {
    BrokerInfo self = new BrokerInfo(1, "localhost", 9092);
    ClusterConfig config = ClusterConfig.singleBroker(self);

    assertEquals(List.of(self), config.brokers());
    assertEquals(1, config.controllerId());
    assertTrue(config.isController(1));
    assertTrue(config.assignmentFor(new TopicPartition("orders", 0)).isEmpty());
  }

  private static TopicConfig defaultTopics() {
    return TopicConfig.parse(null, 4);
  }
}
