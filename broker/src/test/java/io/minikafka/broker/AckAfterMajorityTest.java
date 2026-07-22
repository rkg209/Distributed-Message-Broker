package io.minikafka.broker;

import static org.junit.jupiter.api.Assertions.assertEquals;

import io.minikafka.client.BrokerConnection;
import io.minikafka.client.ProducerClient;
import io.minikafka.protocol.ProtocolConfig;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.List;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Spec 07 test-plan criterion 4 (INV-1): {@link ProducerClient#publish} only returns once {@code
 * propose()}'s future is complete, which {@code RaftNode} only completes after the entry is
 * majority-committed — so by the time the client sees an ack, at least one follower's Raft log
 * (and, within one heartbeat interval, its applied {@link io.minikafka.log.PartitionLog}) already
 * durably holds the record too.
 */
class AckAfterMajorityTest {

  private static final String TOPIC = "orders";
  private static final int PARTITION = 0;
  private static final TopicPartition TP = new TopicPartition(TOPIC, PARTITION);

  private TestCluster cluster;

  @AfterEach
  void stopCluster() {
    if (cluster != null) {
      cluster.close();
    }
  }

  @Test
  void secondReplicaHasRecordShortlyAfterAck(@TempDir Path tempDir) throws Exception {
    TopicConfig topicConfig = TopicConfig.parse(TOPIC + ":1", 1);
    cluster = TestCluster.start(3, TOPIC + ":0=1,2,3", 3, topicConfig, 200, 2000, 200, tempDir);

    int leaderId = RaftTestSupport.awaitLeader(cluster, TP);
    int followerId =
        List.of(1, 2, 3).stream().filter(id -> id != leaderId).findFirst().orElseThrow();

    try (BrokerConnection conn = connectTo(leaderId)) {
      ProducerClient producer = new ProducerClient(conn);
      long offset = producer.publish(TOPIC, PARTITION, value(0));
      assertEquals(0, offset);
    }

    // The ack already implies majority durability at the Raft-log level; give the follower at
    // most one heartbeat interval's worth of slack to have applied it to its PartitionLog too.
    RaftTestSupport.awaitLogSize(cluster, followerId, TP, 1);
    assertEquals(1, cluster.partitionLogOf(followerId, TP).nextOffset());
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
