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
import java.util.List;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Spec 07 test-plan criterion 2: killing one follower out of a 3-broker, RF=3 group doesn't stop
 * the leader from continuing to accept and majority-commit publishes (2 of 3 is still a majority),
 * and a consumer reading from the leader sees every record.
 */
class FollowerKillTest {

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
  void publishingContinuesAfterAFollowerIsKilled(@TempDir Path tempDir) throws Exception {
    TopicConfig topicConfig = TopicConfig.parse(TOPIC + ":1", 1);
    cluster = TestCluster.start(3, TOPIC + ":0=1,2,3", 3, topicConfig, 200, 2000, 200, tempDir);

    int leaderId = RaftTestSupport.awaitLeader(cluster, TP);
    int followerId =
        List.of(1, 2, 3).stream().filter(id -> id != leaderId).findFirst().orElseThrow();

    cluster.killBroker(followerId);

    try (BrokerConnection conn = connectTo(leaderId)) {
      ProducerClient producer = new ProducerClient(conn);
      for (int i = 0; i < RECORD_COUNT; i++) {
        long offset = producer.publish(TOPIC, PARTITION, value(i));
        assertEquals(i, offset);
      }
    }

    try (BrokerConnection conn = connectTo(leaderId)) {
      ConsumerClient consumer = new ConsumerClient(conn, TOPIC, PARTITION, 0);
      List<PollResp.Record> drained = drain(consumer);
      assertEquals(RECORD_COUNT, drained.size());
      for (int i = 0; i < RECORD_COUNT; i++) {
        assertEquals(i, drained.get(i).offset());
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
