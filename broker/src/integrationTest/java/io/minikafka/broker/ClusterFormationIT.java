package io.minikafka.broker;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.minikafka.client.BrokerConnection;
import io.minikafka.client.MetadataClient;
import io.minikafka.protocol.BrokerInfo;
import io.minikafka.protocol.PartitionMetadata;
import io.minikafka.protocol.ProtocolConfig;
import io.minikafka.protocol.TopicMetadata;
import java.io.File;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.testcontainers.containers.ComposeContainer;
import org.testcontainers.containers.ContainerLaunchException;
import org.testcontainers.containers.wait.strategy.Wait;

/**
 * AC-5: brings up the real 3-broker Docker Compose cluster (docker/docker-compose.yml), waits for
 * every broker's "joined cluster" log line, verifies each partition has a leader from its replica
 * set over the real wire protocol, then stops broker-2 and asserts its peers log SUSPECTED.
 * Requires Docker; run via {@code ./gradlew :broker:integrationTest}, never as part of {@code
 * ./gradlew test}.
 *
 * <p>{@code environment.start()} spins up a Testcontainers "ambassador" container per exposed
 * service that links to the target container by name immediately after Compose reports it started;
 * that link attempt races the target container's own startup and can intermittently fail with
 * "Aborting attempt to link to container ... as it is not running" even though the container comes
 * up fine a moment later. {@link #START_ATTEMPTS} absorbs that narrow race by retrying the whole
 * Compose bring-up rather than failing the suite on a transient timing issue.
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class ClusterFormationIT {

  private static ComposeContainer environment;

  private static final Duration STARTUP_TIMEOUT = Duration.ofMinutes(5);
  private static final int START_ATTEMPTS = 3;
  private static final int BROKER_1_HOST_PORT = 9092;

  @BeforeAll
  static void startCluster() {
    ContainerLaunchException lastFailure = null;
    for (int attempt = 1; attempt <= START_ATTEMPTS; attempt++) {
      environment =
          new ComposeContainer(new File("../docker/docker-compose.yml"))
              .withExposedService(
                  "broker-1",
                  9092,
                  Wait.forLogMessage(".*joined cluster.*\\n", 1)
                      .withStartupTimeout(STARTUP_TIMEOUT))
              .withExposedService(
                  "broker-2",
                  9092,
                  Wait.forLogMessage(".*joined cluster.*\\n", 1)
                      .withStartupTimeout(STARTUP_TIMEOUT))
              .withExposedService(
                  "broker-3",
                  9092,
                  Wait.forLogMessage(".*joined cluster.*\\n", 1)
                      .withStartupTimeout(STARTUP_TIMEOUT))
              .withStartupTimeout(STARTUP_TIMEOUT);
      try {
        environment.start();
        return;
      } catch (ContainerLaunchException e) {
        lastFailure = e;
        environment.stop();
      }
    }
    throw new IllegalStateException(
        "Compose cluster failed to start after " + START_ATTEMPTS + " attempts", lastFailure);
  }

  @AfterAll
  static void stopCluster() {
    if (environment != null) {
      environment.stop();
    }
  }

  @Test
  @Order(1)
  void allThreeBrokersJoinAndEveryPartitionHasAnElectedLeader() throws Exception {
    try (BrokerConnection conn =
        new BrokerConnection(
            "localhost", BROKER_1_HOST_PORT, ProtocolConfig.DEFAULT_MAX_FRAME_BYTES)) {
      MetadataClient metadataClient = new MetadataClient(conn);
      List<BrokerInfo> brokers = metadataClient.fetchMetadata();
      assertEquals(3, brokers.size());

      TopicMetadata orders =
          metadataClient.cachedTopics().stream()
              .filter(t -> t.topic().equals("orders"))
              .findFirst()
              .orElseThrow();
      Map<Integer, PartitionMetadata> byPartition =
          orders.partitions().stream()
              .collect(Collectors.toMap(PartitionMetadata::partitionId, p -> p));
      // Leadership is Raft-elected (Spec 06+), not fixed by the static PARTITION_ASSIGNMENTS
      // ordering in docker-compose.yml — asserting a specific rotation (e.g. leader 1/2/3 for
      // partitions 0/1/2) is asserting an implementation detail of election timing, not a real
      // guarantee. What's actually guaranteed: every partition has a leader, and that leader is
      // one of its own replicas.
      for (int partition = 0; partition < 3; partition++) {
        PartitionMetadata metadata = byPartition.get(partition);
        assertTrue(
            metadata.replicaIds().contains(metadata.leaderId()),
            "partition "
                + partition
                + " leader "
                + metadata.leaderId()
                + " is not one of its replicas "
                + metadata.replicaIds());
      }
    }
  }

  @Test
  @Order(2)
  void stoppingBrokerTriggersSuspectedOnPeers() throws Exception {
    environment
        .getContainerByServiceName("broker-2")
        .ifPresent(c -> c.getDockerClient().stopContainerCmd(c.getContainerId()).exec());

    long deadline = System.nanoTime() + 15_000_000_000L;
    boolean broker1Suspected = false;
    boolean broker3Suspected = false;
    while (System.nanoTime() < deadline && !(broker1Suspected && broker3Suspected)) {
      broker1Suspected =
          environment
              .getContainerByServiceName("broker-1")
              .map(c -> c.getLogs().contains("SUSPECTED"))
              .orElse(false);
      broker3Suspected =
          environment
              .getContainerByServiceName("broker-3")
              .map(c -> c.getLogs().contains("SUSPECTED"))
              .orElse(false);
      Thread.sleep(500);
    }
    assertTrue(broker1Suspected, "broker-1 never logged SUSPECTED for broker-2");
    assertTrue(broker3Suspected, "broker-3 never logged SUSPECTED for broker-2");
  }
}
