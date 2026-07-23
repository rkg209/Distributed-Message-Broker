package io.minikafka.broker;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.minikafka.client.ClusterClient;
import io.minikafka.client.ConsumerClient;
import io.minikafka.client.ProducerClient;
import io.minikafka.protocol.PollResp;
import io.minikafka.protocol.ProtocolConfig;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

/**
 * Spec 08 acceptance criterion 1 (INV-1): a producer streams through a {@link ClusterClient} while
 * the leader is killed mid-stream. A new leader is elected within {@code 2 x electionTimeoutMax}
 * and a subsequent consumer drain sees every acked offset, contiguous and in order. Records that
 * failed to ack (because the kill landed mid-propose) are excluded from the zero-loss assertion,
 * per the Spec 08 plan's risk note — their fate is genuinely unknown.
 */
class LeaderFailoverTest {

  private static final String TOPIC = "orders";
  private static final int PARTITION = 0;
  private static final TopicPartition TP = new TopicPartition(TOPIC, PARTITION);
  private static final int RECORD_COUNT = 300;

  private TestCluster cluster;

  @AfterEach
  void stopCluster() {
    if (cluster != null) {
      cluster.close();
    }
  }

  @Test
  @Timeout(90)
  void survivesLeaderKillMidStreamWithZeroLoss(@TempDir Path tempDir) throws Exception {
    TopicConfig topicConfig = TopicConfig.parse(TOPIC + ":1", 1);
    cluster = TestCluster.start(3, TOPIC + ":0=1,2,3", 3, topicConfig, 200, 2000, 200, tempDir);
    int initialLeader = RaftTestSupport.awaitLeader(cluster, TP);

    List<Long> ackedOffsets = new ArrayList<>();
    AtomicLong killedAt = new AtomicLong(-1);

    try (ClusterClient clusterClient = bootstrapAny(-1)) {
      ProducerClient producer = new ProducerClient(clusterClient);
      for (int i = 0; i < RECORD_COUNT; i++) {
        if (i == RECORD_COUNT / 2) {
          cluster.killBroker(initialLeader);
          killedAt.set(i);
        }
        try {
          long offset = producer.publish(TOPIC, PARTITION, value(i));
          ackedOffsets.add(offset);
        } catch (Exception e) {
          // A publish that lands exactly during the kill may genuinely fail; its fate is
          // unknown and it must not be asserted either present or absent (Spec 08 plan risk).
        }
      }
    }

    int newLeader = RaftTestSupport.awaitNewLeader(cluster, TP, initialLeader);
    assertTrue(newLeader != initialLeader);

    // Bootstrap from a broker other than the one just killed: unlike a real crash, the killed
    // broker's acceptor is simply closed in-process, so connecting to it refuses immediately.
    try (ClusterClient clusterClient = bootstrapAny(initialLeader)) {
      ConsumerClient consumer = new ConsumerClient(clusterClient, TOPIC, PARTITION, 0L, 5, 100);
      List<PollResp.Record> drained = drain(consumer);
      List<Long> drainedOffsets = drained.stream().map(PollResp.Record::offset).toList();

      for (long acked : ackedOffsets) {
        assertTrue(drainedOffsets.contains(acked), "acked offset " + acked + " missing from drain");
      }
      for (int i = 1; i < drainedOffsets.size(); i++) {
        assertEquals(drainedOffsets.get(i - 1) + 1, drainedOffsets.get(i), "gap in offsets");
      }
    }
  }

  /** Bootstraps from any broker other than {@code excludedId} (pass -1 to allow any broker). */
  private ClusterClient bootstrapAny(int excludedId) throws Exception {
    TestCluster.BrokerNode node =
        cluster.nodes().stream()
            .filter(n -> n.info().brokerId() != excludedId)
            .findFirst()
            .orElseThrow();
    return new ClusterClient(
        "localhost", node.acceptor().boundPort(), ProtocolConfig.DEFAULT_MAX_FRAME_BYTES);
  }

  private static byte[] value(int i) {
    return ("record-" + i).getBytes(StandardCharsets.UTF_8);
  }

  private static List<PollResp.Record> drain(ConsumerClient consumer) throws Exception {
    List<PollResp.Record> all = new ArrayList<>();
    List<PollResp.Record> batch;
    while (!(batch = consumer.poll()).isEmpty()) {
      all.addAll(batch);
    }
    return all;
  }
}
