package io.minikafka.broker;

import static org.junit.jupiter.api.Assertions.assertEquals;

import io.minikafka.client.BrokerConnection;
import io.minikafka.client.ProducerClient;
import io.minikafka.log.LogRecord;
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
 * Spec 07 test-plan criterion 1 (INV-1, INV-2): a 3-broker, RF=3 cluster replicates every publish
 * to a majority before acking, and every replica's durable {@link PartitionLog} ends up
 * byte-identical, in identical order.
 */
class ReplicationE2ETest {

  private static final String TOPIC = "orders";
  private static final int PARTITION = 0;
  private static final int RECORD_COUNT = 200;
  private static final TopicPartition TP = new TopicPartition(TOPIC, PARTITION);

  private TestCluster cluster;

  @AfterEach
  void stopCluster() {
    if (cluster != null) {
      cluster.close();
    }
  }

  @Test
  void allReplicasConvergeToIdenticalLogs(@TempDir Path tempDir) throws Exception {
    TopicConfig topicConfig = TopicConfig.parse(TOPIC + ":1", 1);
    cluster = TestCluster.start(3, TOPIC + ":0=1,2,3", 3, topicConfig, 200, 2000, 200, tempDir);

    int leaderId = RaftTestSupport.awaitLeader(cluster, TP);

    try (BrokerConnection conn = connectTo(leaderId)) {
      ProducerClient producer = new ProducerClient(conn);
      for (int i = 0; i < RECORD_COUNT; i++) {
        long offset = producer.publish(TOPIC, PARTITION, value(i));
        assertEquals(i, offset);
      }
    }

    for (int brokerId : List.of(1, 2, 3)) {
      RaftTestSupport.awaitLogSize(cluster, brokerId, TP, RECORD_COUNT);
    }

    List<LogRecord> reference = readAll(1);
    assertEquals(RECORD_COUNT, reference.size());
    for (int brokerId : List.of(2, 3)) {
      List<LogRecord> other = readAll(brokerId);
      assertEquals(reference.size(), other.size());
      for (int i = 0; i < reference.size(); i++) {
        assertEquals(reference.get(i).offset(), other.get(i).offset());
        assertEquals(reference.get(i).timestamp(), other.get(i).timestamp());
        assertEquals(
            new String(reference.get(i).value(), StandardCharsets.UTF_8),
            new String(other.get(i).value(), StandardCharsets.UTF_8));
      }
    }
  }

  private List<LogRecord> readAll(int brokerId) {
    PartitionLog log = cluster.partitionLogOf(brokerId, TP);
    return log.read(0, Integer.MAX_VALUE);
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
