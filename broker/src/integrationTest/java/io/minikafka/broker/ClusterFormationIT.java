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
import org.testcontainers.containers.wait.strategy.Wait;

/**
 * AC-5: brings up the real 3-broker Docker Compose cluster (docker/docker-compose.yml), waits for
 * every broker's "joined cluster" log line, verifies the rotating leader assignment over the real
 * wire protocol, then stops broker-2 and asserts its peers log SUSPECTED. Requires Docker; run via
 * {@code ./gradlew :broker:integrationTest}, never as part of {@code ./gradlew test}.
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class ClusterFormationIT {

  private static ComposeContainer environment;

  @BeforeAll
  static void startCluster() {
    environment =
        new ComposeContainer(new File("../docker/docker-compose.yml"))
            .withExposedService("broker-1", 9092, Wait.forLogMessage(".*joined cluster.*\\n", 1))
            .withExposedService("broker-2", 9092, Wait.forLogMessage(".*joined cluster.*\\n", 1))
            .withExposedService("broker-3", 9092, Wait.forLogMessage(".*joined cluster.*\\n", 1))
            .withLocalCompose(true);
    environment.start();
  }

  @AfterAll
  static void stopCluster() {
    if (environment != null) {
      environment.stop();
    }
  }

  @Test
  @Order(1)
  void allThreeBrokersJoinAndReportRotatingLeaders() throws Exception {
    String host = environment.getServiceHost("broker-1", 9092);
    int port = environment.getServicePort("broker-1", 9092);

    try (BrokerConnection conn =
        new BrokerConnection(host, port, ProtocolConfig.DEFAULT_MAX_FRAME_BYTES)) {
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
      assertEquals(1, byPartition.get(0).leaderId());
      assertEquals(2, byPartition.get(1).leaderId());
      assertEquals(3, byPartition.get(2).leaderId());
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
