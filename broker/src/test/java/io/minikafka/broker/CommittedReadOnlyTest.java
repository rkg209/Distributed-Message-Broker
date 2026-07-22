package io.minikafka.broker;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.minikafka.client.BrokerConnection;
import io.minikafka.client.ProducerClient;
import io.minikafka.log.PartitionLog;
import io.minikafka.protocol.ProtocolConfig;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.List;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Spec 07 test-plan criterion 3 (INV-5): a broker cut off from the rest of a 3-broker, RF=3 group
 * receives no {@code AppendEntries} at all while partitioned, so it structurally cannot expose any
 * record the (still-reachable) majority commits while it's gone — {@link PartitionReplica#apply} is
 * the only writer of its {@link PartitionLog}, and only committed entries ever reach {@code apply}.
 */
class CommittedReadOnlyTest {

  private static final String TOPIC = "orders";
  private static final int PARTITION = 0;
  private static final int RECORD_COUNT = 100;
  private static final TopicPartition TP = new TopicPartition(TOPIC, PARTITION);

  private TestCluster cluster;

  @AfterEach
  void stopCluster() {
    if (cluster != null) {
      cluster.close();
    }
  }

  @Test
  void partitionedFollowerNeverSeesMajorityOnlyCommittedRecords(@TempDir Path tempDir)
      throws Exception {
    TopicConfig topicConfig = TopicConfig.parse(TOPIC + ":1", 1);
    cluster = TestCluster.start(3, TOPIC + ":0=1,2,3", 3, topicConfig, 200, 2000, 200, tempDir);

    int leaderId = RaftTestSupport.awaitLeader(cluster, TP);
    int isolatedId =
        List.of(1, 2, 3).stream().filter(id -> id != leaderId).findFirst().orElseThrow();

    long isolatedSizeBeforePartition = cluster.partitionLogOf(isolatedId, TP).nextOffset();
    cluster.killBroker(isolatedId);

    try (BrokerConnection conn = connectTo(leaderId)) {
      ProducerClient producer = new ProducerClient(conn);
      for (int i = 0; i < RECORD_COUNT; i++) {
        long offset = producer.publish(TOPIC, PARTITION, value(i));
        assertEquals(i, offset);
      }
    }

    int survivingFollowerId =
        List.of(1, 2, 3).stream()
            .filter(id -> id != leaderId && id != isolatedId)
            .findFirst()
            .orElseThrow();
    RaftTestSupport.awaitLogSize(cluster, survivingFollowerId, TP, RECORD_COUNT);
    RaftTestSupport.awaitLogSize(cluster, leaderId, TP, RECORD_COUNT);

    PartitionLog isolatedLog = cluster.partitionLogOf(isolatedId, TP);
    assertTrue(
        isolatedLog.nextOffset() == isolatedSizeBeforePartition,
        "Isolated broker's log grew from "
            + isolatedSizeBeforePartition
            + " to "
            + isolatedLog.nextOffset()
            + " while cut off from the majority");
  }

  private BrokerConnection connectTo(int brokerId) throws IOException {
    TestCluster.BrokerNode node = cluster.node(brokerId);
    return new BrokerConnection(
        "localhost", node.acceptor().boundPort(), ProtocolConfig.DEFAULT_MAX_FRAME_BYTES);
  }

  private static byte[] value(int i) {
    return ("record-" + i).getBytes(StandardCharsets.UTF_8);
  }
}
