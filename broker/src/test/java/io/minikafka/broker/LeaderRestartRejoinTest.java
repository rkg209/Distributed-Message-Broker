package io.minikafka.broker;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.minikafka.client.BrokerConnection;
import io.minikafka.client.ProducerClient;
import io.minikafka.protocol.ProtocolConfig;
import io.minikafka.raft.RaftRole;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

/**
 * Spec 08 acceptance criterion 4: after a leader is killed and a new one elected, publishing more
 * records, the killed broker restarts and rejoins by receiving {@code AppendEntries} from the new
 * leader (a higher term). It must transition to {@code FOLLOWER} with {@code currentTerm >=
 * newTerm} and its {@code PartitionLog} must catch up to the new leader's.
 */
class LeaderRestartRejoinTest {

  private static final String TOPIC = "orders";
  private static final int PARTITION = 0;
  private static final TopicPartition TP = new TopicPartition(TOPIC, PARTITION);
  private static final int FIRST_BATCH = 100;
  private static final int SECOND_BATCH = 100;

  private TestCluster cluster;

  @AfterEach
  void stopCluster() {
    if (cluster != null) {
      cluster.close();
    }
  }

  @Test
  @Timeout(90)
  void killedLeaderRejoinsAsFollowerAndCatchesUp(@TempDir Path tempDir) throws Exception {
    TopicConfig topicConfig = TopicConfig.parse(TOPIC + ":1", 1);
    cluster = TestCluster.start(3, TOPIC + ":0=1,2,3", 3, topicConfig, 200, 2000, 200, tempDir);
    int oldLeaderId = RaftTestSupport.awaitLeader(cluster, TP);

    try (BrokerConnection conn = connectTo(oldLeaderId)) {
      ProducerClient producer = new ProducerClient(conn);
      for (int i = 0; i < FIRST_BATCH; i++) {
        producer.publish(TOPIC, PARTITION, value(i));
      }
    }

    cluster.killBroker(oldLeaderId);
    int newLeaderId = RaftTestSupport.awaitNewLeader(cluster, TP, oldLeaderId);
    long newTerm = cluster.leaderEpochOf(newLeaderId, TP);

    try (BrokerConnection conn = connectTo(newLeaderId)) {
      ProducerClient producer = new ProducerClient(conn);
      for (int i = 0; i < SECOND_BATCH; i++) {
        producer.publish(TOPIC, PARTITION, value(FIRST_BATCH + i));
      }
    }

    cluster.restartBroker(oldLeaderId);
    RaftTestSupport.awaitTermAtLeast(cluster, oldLeaderId, TP, newTerm);
    RaftTestSupport.awaitRole(cluster, oldLeaderId, TP, RaftRole.FOLLOWER);
    assertTrue(
        cluster.leaderEpochOf(oldLeaderId, TP) >= newTerm,
        "restarted broker's term must catch up to the new leader's");

    RaftTestSupport.awaitLogSize(cluster, oldLeaderId, TP, FIRST_BATCH + SECOND_BATCH);
    long restartedLogSize = cluster.partitionLogOf(oldLeaderId, TP).nextOffset();
    long newLeaderLogSize = cluster.partitionLogOf(newLeaderId, TP).nextOffset();
    assertEquals(newLeaderLogSize, restartedLogSize);
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
