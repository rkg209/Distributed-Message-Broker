package io.minikafka.broker;

import static org.junit.jupiter.api.Assertions.assertEquals;

import io.minikafka.client.BrokerConnection;
import io.minikafka.client.ConsumerClient;
import io.minikafka.client.ProducerClient;
import io.minikafka.log.InMemoryPartitionLog;
import io.minikafka.protocol.BrokerInfo;
import io.minikafka.protocol.PollResp;
import io.minikafka.protocol.ProtocolConfig;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * AC-2: two producers writing to different partitions of the same topic concurrently each observe
 * their own per-partition order preserved; there's no cross-partition ordering guarantee to check.
 */
class PartitionOrderingTest {

  private static final String TOPIC = "orders";
  private static final int RECORD_COUNT = 2_000;

  private ConnectionAcceptor acceptor;
  private PartitionManager partitionManager;
  private ConsumerGroupManager consumerGroupManager;

  @BeforeEach
  void startBroker(@TempDir Path tempDir) throws IOException {
    BrokerConfig config =
        TestBrokerConfig.singleBroker(
            tempDir.resolve("logs"),
            tempDir.resolve("offsets"),
            TopicConfig.parse(TOPIC + ":2", 1));
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
            metadataService, partitionManager, consumerGroupManager, 4 * 1024 * 1024);
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
  void concurrentProducersOnDifferentPartitionsPreserveEachOthersOrder() throws Exception {
    ExecutorService pool = Executors.newFixedThreadPool(2);
    CountDownLatch start = new CountDownLatch(1);
    try {
      var future0 = pool.submit(() -> produce(0, start));
      var future1 = pool.submit(() -> produce(1, start));
      start.countDown();
      future0.get(30, TimeUnit.SECONDS);
      future1.get(30, TimeUnit.SECONDS);
    } finally {
      pool.shutdownNow();
    }

    assertPartitionInOrder(0);
    assertPartitionInOrder(1);
  }

  private Void produce(int partition, CountDownLatch start) throws IOException {
    try (BrokerConnection conn = connect()) {
      ProducerClient producer = new ProducerClient(conn);
      start.await(30, TimeUnit.SECONDS);
      for (int i = 0; i < RECORD_COUNT; i++) {
        producer.publish(TOPIC, partition, value(partition, i));
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
    return null;
  }

  private void assertPartitionInOrder(int partition) throws IOException {
    try (BrokerConnection conn = connect()) {
      ConsumerClient consumer = new ConsumerClient(conn, TOPIC, partition, 0L);
      List<PollResp.Record> all = new ArrayList<>();
      List<PollResp.Record> batch;
      while (!(batch = consumer.poll()).isEmpty()) {
        all.addAll(batch);
      }
      assertEquals(RECORD_COUNT, all.size());
      for (int i = 0; i < RECORD_COUNT; i++) {
        assertEquals(i, all.get(i).offset());
        assertEquals(
            new String(value(partition, i), StandardCharsets.UTF_8),
            new String(all.get(i).payload(), StandardCharsets.UTF_8));
      }
    }
  }

  private BrokerConnection connect() throws IOException {
    return new BrokerConnection(
        "localhost", acceptor.boundPort(), ProtocolConfig.DEFAULT_MAX_FRAME_BYTES);
  }

  private static byte[] value(int partition, int i) {
    return ("p" + partition + "-r" + i).getBytes(StandardCharsets.UTF_8);
  }
}
