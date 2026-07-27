package io.minikafka.broker;

import static org.junit.jupiter.api.Assertions.assertEquals;

import io.minikafka.client.BrokerConnection;
import io.minikafka.client.ConsumerClient;
import io.minikafka.client.ProducerClient;
import io.minikafka.protocol.CommitOffsetReq;
import io.minikafka.protocol.CommitOffsetResp;
import io.minikafka.protocol.Message;
import io.minikafka.protocol.PollResp;
import io.minikafka.protocol.ProtocolConfig;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

/**
 * Spec 09 acceptance criterion 4: a consumer commits offset 300 against one broker in a live 3-node
 * cluster; that broker is killed (a failover elects a new partition leader while it's down) and
 * then restarted. Reconnecting to the restarted broker resumes from exactly offset 300, no
 * re-delivery of 0–299 — {@code OffsetStore} durability re-verified when the commit happens under
 * cluster/failover conditions, not just a lone-broker clean restart (see {@link
 * ConsumerGroupOffsetDurabilityTest} for that simpler shape). Consumer-group offsets are a
 * per-broker local {@code OffsetStore}, not Raft-replicated, so the resuming consumer must
 * reconnect to the same broker that took the commit — a fresh leader's own offset store never saw
 * it.
 */
class OffsetDurabilityAcrossFailoverTest {

  private static final String TOPIC = "orders";
  private static final int PARTITION = 0;
  private static final TopicPartition TP = new TopicPartition(TOPIC, PARTITION);
  private static final int RECORD_COUNT = 1000;
  private static final long COMMITTED_OFFSET = 300L;
  private static final String GROUP = "group-failover";

  private TestCluster cluster;

  @AfterEach
  void stopCluster() {
    if (cluster != null) {
      cluster.close();
    }
  }

  @Test
  @Timeout(90)
  void consumerResumesFromCommittedOffsetAfterLeaderFailover(@TempDir Path tempDir)
      throws Exception {
    TopicConfig topicConfig = TopicConfig.parse(TOPIC + ":1", 1);
    cluster = TestCluster.start(3, TOPIC + ":0=1,2,3", 3, topicConfig, 200, 2000, 200, tempDir);
    int leaderId = RaftTestSupport.awaitLeader(cluster, TP);

    try (BrokerConnection conn = connectTo(leaderId)) {
      ProducerClient producer = new ProducerClient(conn);
      for (int i = 0; i < RECORD_COUNT; i++) {
        producer.publish(TOPIC, PARTITION, value(i));
      }
    }

    try (BrokerConnection conn = connectTo(leaderId)) {
      for (int i = 0; i < COMMITTED_OFFSET; i++) {
        conn.request(
            new io.minikafka.protocol.PollReq(conn.nextCorrelationId(), TOPIC, PARTITION, i));
      }
      Message commitResponse =
          conn.request(
              new CommitOffsetReq(
                  conn.nextCorrelationId(), GROUP, TOPIC, PARTITION, COMMITTED_OFFSET));
      assertEquals(true, ((CommitOffsetResp) commitResponse).ok());
    }

    cluster.killBroker(leaderId);
    int newLeaderId = RaftTestSupport.awaitNewLeader(cluster, TP, leaderId);
    RaftTestSupport.awaitLogSize(cluster, newLeaderId, TP, RECORD_COUNT);
    cluster.restartBroker(leaderId);
    RaftTestSupport.awaitLogSize(cluster, leaderId, TP, RECORD_COUNT);

    try (BrokerConnection conn = connectTo(leaderId)) {
      ConsumerClient consumer = new ConsumerClient(conn, TOPIC, PARTITION, GROUP);
      assertEquals(COMMITTED_OFFSET, consumer.currentOffset());

      java.util.List<PollResp.Record> remainder = new java.util.ArrayList<>();
      java.util.List<PollResp.Record> batch;
      while (!(batch = consumer.poll()).isEmpty()) {
        remainder.addAll(batch);
      }
      assertEquals(RECORD_COUNT - COMMITTED_OFFSET, remainder.size());
      assertEquals(COMMITTED_OFFSET, remainder.get(0).offset());
      assertEquals(RECORD_COUNT - 1, remainder.get(remainder.size() - 1).offset());
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
}
