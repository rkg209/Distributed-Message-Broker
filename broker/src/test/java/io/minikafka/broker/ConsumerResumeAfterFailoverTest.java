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
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

/**
 * Spec 08 acceptance criterion 3: a consumer group commits offset 500 before the leader dies; after
 * failover, a new {@link ConsumerClient} on the same group resumes exactly at 500 with no duplicate
 * and no gap through 999. Per the Spec 08 plan, {@code ConsumerGroupManager}'s {@code OffsetStore}
 * is broker-local, so the resuming consumer must reconnect to the *same surviving broker* that
 * holds the committed offset — this is a documented limitation (no cross-broker offset replication
 * yet), not a bug this test tries to break.
 */
class ConsumerResumeAfterFailoverTest {

  private static final String TOPIC = "orders";
  private static final int PARTITION = 0;
  private static final String GROUP = "resume-group";
  private static final TopicPartition TP = new TopicPartition(TOPIC, PARTITION);
  private static final int RECORD_COUNT = 1000;
  private static final int COMMIT_AT = 500;

  private TestCluster cluster;

  @AfterEach
  void stopCluster() {
    if (cluster != null) {
      cluster.close();
    }
  }

  @Test
  @Timeout(90)
  void consumerResumesFromCommittedOffsetAfterFailover(@TempDir Path tempDir) throws Exception {
    TopicConfig topicConfig = TopicConfig.parse(TOPIC + ":1", 1);
    cluster = TestCluster.start(3, TOPIC + ":0=1,2,3", 3, topicConfig, 200, 2000, 200, tempDir);
    int leaderId = RaftTestSupport.awaitLeader(cluster, TP);

    try (BrokerConnection conn = connectTo(leaderId)) {
      ProducerClient producer = new ProducerClient(conn);
      for (int i = 0; i < COMMIT_AT; i++) {
        producer.publish(TOPIC, PARTITION, value(i));
      }
    }

    int survivorId =
        List.of(1, 2, 3).stream().filter(id -> id != leaderId).findFirst().orElseThrow();
    RaftTestSupport.awaitLogSize(cluster, survivorId, TP, COMMIT_AT);

    // Only COMMIT_AT records exist so far, so a full drain naturally stops exactly at that
    // boundary regardless of how many records one poll() batch can hold.
    try (BrokerConnection conn = connectTo(survivorId)) {
      ConsumerClient consumer = new ConsumerClient(conn, TOPIC, PARTITION, GROUP);
      drain(consumer);
      assertEquals(COMMIT_AT, consumer.currentOffset());
      consumer.commitOffset();
    }

    cluster.killBroker(leaderId);
    int newLeaderId = RaftTestSupport.awaitNewLeader(cluster, TP, leaderId);

    try (BrokerConnection conn = connectTo(newLeaderId)) {
      ProducerClient producer = new ProducerClient(conn);
      for (int i = COMMIT_AT; i < RECORD_COUNT; i++) {
        producer.publish(TOPIC, PARTITION, value(i));
      }
    }
    RaftTestSupport.awaitLogSize(cluster, survivorId, TP, RECORD_COUNT);

    try (BrokerConnection conn = connectTo(survivorId)) {
      ConsumerClient resumed = new ConsumerClient(conn, TOPIC, PARTITION, GROUP);
      assertEquals(COMMIT_AT, resumed.currentOffset());
      List<PollResp.Record> rest = drain(resumed);
      assertEquals(RECORD_COUNT - COMMIT_AT, rest.size());
      for (int i = 0; i < rest.size(); i++) {
        assertEquals(COMMIT_AT + i, rest.get(i).offset());
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
