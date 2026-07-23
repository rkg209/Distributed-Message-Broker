package io.minikafka.broker;

import static org.junit.jupiter.api.Assertions.assertEquals;

import io.minikafka.client.BrokerConnection;
import io.minikafka.client.ConsumerClient;
import io.minikafka.client.ProducerClient;
import io.minikafka.protocol.PollResp;
import io.minikafka.protocol.ProtocolConfig;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

/**
 * Spec 08 acceptance criterion 5: kill the leader 10 times in succession, restarting each killed
 * broker before killing the next leader so the cluster never drops below quorum. Zero
 * committed-message loss: the union of every acked offset across all 10 rounds must be present
 * exactly once in a full consumer drain at the end.
 */
class RepeatedFailoverTest {

  private static final String TOPIC = "orders";
  private static final int PARTITION = 0;
  private static final TopicPartition TP = new TopicPartition(TOPIC, PARTITION);
  private static final int ROUNDS = 10;
  private static final int BATCH_SIZE = 20;

  private TestCluster cluster;

  @AfterEach
  void stopCluster() {
    if (cluster != null) {
      cluster.close();
    }
  }

  @Test
  @Timeout(240)
  void survivesTenSuccessiveFailoversWithZeroLoss(@TempDir Path tempDir) throws Exception {
    TopicConfig topicConfig = TopicConfig.parse(TOPIC + ":1", 1);
    cluster = TestCluster.start(3, TOPIC + ":0=1,2,3", 3, topicConfig, 200, 2000, 200, tempDir);

    Set<Long> ackedOffsets = new TreeSet<>();
    int recordCounter = 0;
    int currentLeader = RaftTestSupport.awaitLeader(cluster, TP);

    for (int round = 0; round < ROUNDS; round++) {
      try (BrokerConnection conn = connectTo(currentLeader)) {
        ProducerClient producer = new ProducerClient(conn);
        for (int i = 0; i < BATCH_SIZE; i++, recordCounter++) {
          long offset = producer.publish(TOPIC, PARTITION, value(recordCounter));
          ackedOffsets.add(offset);
        }
      }

      int deadLeader = currentLeader;
      cluster.killBroker(deadLeader);
      currentLeader = RaftTestSupport.awaitNewLeader(cluster, TP, deadLeader);
      long newTerm = cluster.leaderEpochOf(currentLeader, TP);
      cluster.restartBroker(deadLeader);
      // Wait for the restarted broker to fully rejoin (role, term, and log) before the next
      // round's kill: no PreVote is implemented, so a still-catching-up node whose election timer
      // fires prematurely can win no votes yet keep bumping the term, destabilizing whichever
      // healthy node would otherwise become the next leader.
      RaftTestSupport.awaitRole(cluster, deadLeader, TP, io.minikafka.raft.RaftRole.FOLLOWER);
      RaftTestSupport.awaitTermAtLeast(cluster, deadLeader, TP, newTerm);
      RaftTestSupport.awaitLogSize(cluster, deadLeader, TP, recordCounter);
    }

    RaftTestSupport.awaitLogSize(cluster, currentLeader, TP, recordCounter);
    try (BrokerConnection conn = connectTo(currentLeader)) {
      ConsumerClient consumer = new ConsumerClient(conn, TOPIC, PARTITION, 0);
      List<PollResp.Record> drained = drain(consumer);

      Set<Long> drainedOffsets = new HashSet<>();
      for (PollResp.Record record : drained) {
        assertEquals(true, drainedOffsets.add(record.offset()), "duplicate offset in drain");
      }
      for (long acked : ackedOffsets) {
        assertEquals(true, drainedOffsets.contains(acked), "acked offset " + acked + " missing");
      }
    }
  }

  private BrokerConnection connectTo(int brokerId) throws IOException {
    TestCluster.BrokerNode node = cluster.node(brokerId);
    return new BrokerConnection(
        "localhost", node.acceptor().boundPort(), ProtocolConfig.DEFAULT_MAX_FRAME_BYTES);
  }

  private static byte[] value(int i) {
    return ("record-" + i).getBytes(StandardCharsets.UTF_8);
  }

  private static List<PollResp.Record> drain(ConsumerClient consumer) throws IOException {
    List<PollResp.Record> all = new ArrayList<>();
    List<PollResp.Record> batch;
    while (!(batch = consumer.poll()).isEmpty()) {
      all.addAll(batch);
    }
    return all;
  }
}
