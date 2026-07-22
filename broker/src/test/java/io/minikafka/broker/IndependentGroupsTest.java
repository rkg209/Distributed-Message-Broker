package io.minikafka.broker;

import static org.junit.jupiter.api.Assertions.assertEquals;

import io.minikafka.client.BrokerConnection;
import io.minikafka.client.ConsumerClient;
import io.minikafka.client.ProducerClient;
import io.minikafka.log.InMemoryPartitionLog;
import io.minikafka.protocol.BrokerInfo;
import io.minikafka.protocol.ProtocolConfig;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** AC-4: two consumer groups reading the same topic maintain independent committed offsets. */
class IndependentGroupsTest {

  private static final String TOPIC = "orders";
  private static final int PARTITION = 0;

  private ConnectionAcceptor acceptor;
  private PartitionManager partitionManager;
  private ConsumerGroupManager consumerGroupManager;

  @BeforeEach
  void startBroker(@TempDir Path tempDir) throws IOException {
    BrokerConfig config =
        TestBrokerConfig.singleBroker(
            tempDir.resolve("logs"), tempDir.resolve("offsets"), TopicConfig.parse(null, 1));
    BrokerInfo self = new BrokerInfo(config.brokerId(), config.brokerHost(), config.brokerPort());
    MetadataService metadataService =
        new MetadataService(self, config.topicConfig(), config.clusterConfig());
    partitionManager =
        new PartitionManager(
            config, metadataService, config.clusterConfig(), tp -> new InMemoryPartitionLog());
    metadataService.attachPartitionManager(partitionManager);
    partitionManager.start();
    consumerGroupManager = new ConsumerGroupManager(tempDir.resolve("offsets"));
    BrokerRequestHandler handler =
        new BrokerRequestHandler(
            metadataService, partitionManager, consumerGroupManager, 1024 * 1024);
    acceptor = new ConnectionAcceptor(0, ProtocolConfig.DEFAULT_MAX_FRAME_BYTES, handler);
    acceptor.start();
  }

  @AfterEach
  void stopBroker() {
    acceptor.close();
    partitionManager.close();
    consumerGroupManager.close();
  }

  @Test
  void g1CommitDoesNotMoveG2Offset() throws IOException {
    try (BrokerConnection producerConn = connect()) {
      ProducerClient producer = new ProducerClient(producerConn);
      for (int i = 0; i < 100; i++) {
        producer.publish(TOPIC, PARTITION, ("r-" + i).getBytes(StandardCharsets.UTF_8));
      }
    }

    try (BrokerConnection g1Conn = connect();
        BrokerConnection g2Conn = connect()) {
      ConsumerClient g1 = new ConsumerClient(g1Conn, TOPIC, PARTITION, "group-1");
      ConsumerClient g2 = new ConsumerClient(g2Conn, TOPIC, PARTITION, "group-2");
      assertEquals(0L, g1.currentOffset());
      assertEquals(0L, g2.currentOffset());

      g1.poll(); // advances g1's in-memory offset past the batch
      g1.commitOffset();

      assertEquals(0L, g2.currentOffset());
    }

    // Fresh connections for both groups: g1 resumes past its commit, g2 still starts at 0.
    try (BrokerConnection g1Conn = connect()) {
      ConsumerClient g1 = new ConsumerClient(g1Conn, TOPIC, PARTITION, "group-1");
      assertEquals(100L, g1.currentOffset());
    }
    try (BrokerConnection g2Conn = connect()) {
      ConsumerClient g2 = new ConsumerClient(g2Conn, TOPIC, PARTITION, "group-2");
      assertEquals(0L, g2.currentOffset());
    }
  }

  private BrokerConnection connect() throws IOException {
    return new BrokerConnection(
        "localhost", acceptor.boundPort(), ProtocolConfig.DEFAULT_MAX_FRAME_BYTES);
  }
}
