package io.minikafka.broker;

import static org.junit.jupiter.api.Assertions.assertEquals;

import io.minikafka.client.BrokerConnection;
import io.minikafka.client.MetadataClient;
import io.minikafka.protocol.BrokerInfo;
import io.minikafka.protocol.PartitionMetadata;
import io.minikafka.protocol.ProtocolConfig;
import io.minikafka.protocol.TopicMetadata;
import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * AC-3: real client connections against each broker in an in-process 3-broker cluster all return
 * the identical partition -> (leader, replicas) map, matching the configured assignments, and list
 * every broker.
 */
class MetadataE2ETest {

  private static final String TOPIC = "orders";

  private TestCluster cluster;

  @AfterEach
  void stopCluster() {
    if (cluster != null) {
      cluster.close();
    }
  }

  @Test
  void allBrokersReportIdenticalPartitionAssignments(@TempDir Path tempDir) throws IOException {
    TopicConfig topicConfig = TopicConfig.parse("orders:3", 1);
    String assignments = "orders:0=1,2,3;orders:1=2,3,1;orders:2=3,1,2";
    cluster = TestCluster.start(3, assignments, 3, topicConfig, 200, 2000, 200, tempDir);

    for (TestCluster.BrokerNode node : cluster.nodes()) {
      try (BrokerConnection conn =
          new BrokerConnection(
              "localhost", node.acceptor().boundPort(), ProtocolConfig.DEFAULT_MAX_FRAME_BYTES)) {
        MetadataClient metadataClient = new MetadataClient(conn);
        List<BrokerInfo> brokers = metadataClient.fetchMetadata();

        Set<Integer> brokerIds =
            brokers.stream().map(BrokerInfo::brokerId).collect(java.util.stream.Collectors.toSet());
        assertEquals(Set.of(1, 2, 3), brokerIds);

        TopicMetadata ordersMeta =
            metadataClient.cachedTopics().stream()
                .filter(t -> t.topic().equals(TOPIC))
                .findFirst()
                .orElseThrow();

        Map<Integer, PartitionMetadata> byPartition =
            ordersMeta.partitions().stream()
                .collect(java.util.stream.Collectors.toMap(PartitionMetadata::partitionId, p -> p));

        assertEquals(1, byPartition.get(0).leaderId());
        assertEquals(List.of(1, 2, 3), byPartition.get(0).replicaIds());
        assertEquals(2, byPartition.get(1).leaderId());
        assertEquals(List.of(2, 3, 1), byPartition.get(1).replicaIds());
        assertEquals(3, byPartition.get(2).leaderId());
        assertEquals(List.of(3, 1, 2), byPartition.get(2).replicaIds());
      }
    }
  }
}
