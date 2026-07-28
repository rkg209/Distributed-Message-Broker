package io.minikafka.broker;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.minikafka.client.BrokerConnection;
import io.minikafka.log.DiskPartitionLog;
import io.minikafka.protocol.BrokerInfo;
import io.minikafka.protocol.Message;
import io.minikafka.protocol.ProtocolConfig;
import io.minikafka.protocol.PublishReq;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

/**
 * AC-4: once a publish burst drains, the admission gate returns to zero in-flight and a subsequent
 * publish succeeds on its first attempt — backpressure is a transient throttle, not a permanent
 * capacity loss.
 */
class BackpressureRecoveryTest {

  private static final String TOPIC = "orders";
  private static final int PARTITION = 0;
  private static final TopicPartition TP = new TopicPartition(TOPIC, PARTITION);
  private static final int CAPACITY = 5;

  private ConnectionAcceptor acceptor;
  private PartitionManager partitionManager;

  @BeforeEach
  void startBroker(@TempDir Path tempDir) throws IOException {
    BrokerConfig config =
        TestBrokerConfig.singleBroker(
            tempDir.resolve("logs"),
            tempDir.resolve("offsets"),
            TopicConfig.parse(TOPIC + ":1", 1),
            CAPACITY,
            50);
    BrokerInfo self = new BrokerInfo(config.brokerId(), config.brokerHost(), config.brokerPort());
    MetadataService metadataService =
        new MetadataService(self, config.topicConfig(), config.clusterConfig());
    partitionManager =
        new PartitionManager(
            config,
            metadataService,
            config.clusterConfig(),
            tp -> new DiskPartitionLog(config.logConfigFor(tp)));
    metadataService.attachPartitionManager(partitionManager);
    partitionManager.start();
    ConsumerGroupManager groupManager = new ConsumerGroupManager(tempDir.resolve("offsets"));
    BrokerRequestHandler handler =
        new BrokerRequestHandler(
            metadataService,
            partitionManager,
            groupManager,
            new GroupCoordinator(metadataService, 3_000, 10_000, 5_000),
            1024 * 1024);
    acceptor = new ConnectionAcceptor(0, ProtocolConfig.DEFAULT_MAX_FRAME_BYTES, handler);
    acceptor.start();
  }

  @AfterEach
  void stopBroker() {
    acceptor.close();
    partitionManager.close();
  }

  @Test
  @Timeout(30)
  void inFlightReturnsToZeroAndTheNextPublishSucceedsFirstTry() throws Exception {
    int burst = 30;
    CountDownLatch start = new CountDownLatch(1);
    CountDownLatch done = new CountDownLatch(burst);
    for (int i = 0; i < burst; i++) {
      final long seq = i;
      Thread.ofVirtual()
          .start(
              () -> {
                try (BrokerConnection conn =
                    new BrokerConnection(
                        "localhost",
                        acceptor.boundPort(),
                        ProtocolConfig.DEFAULT_MAX_FRAME_BYTES)) {
                  start.await();
                  conn.request(
                      new PublishReq(
                          conn.nextCorrelationId(), TOPIC, PARTITION, 1L, seq, null, value(seq)));
                } catch (Exception ignored) {
                  // Some may be rejected as BROKER_BUSY; this test only cares that the gate
                  // drains afterwards, not that every burst message lands.
                } finally {
                  done.countDown();
                }
              });
    }
    start.countDown();
    assertTrue(done.await(20, TimeUnit.SECONDS), "burst should finish");

    assertEquals(0, partitionManager.inFlightPublishes(TP));

    try (BrokerConnection conn =
        new BrokerConnection(
            "localhost", acceptor.boundPort(), ProtocolConfig.DEFAULT_MAX_FRAME_BYTES)) {
      long startNanos = System.nanoTime();
      Message response =
          conn.request(
              new PublishReq(conn.nextCorrelationId(), TOPIC, PARTITION, 2L, 0L, null, value(0)));
      long elapsedMs = (System.nanoTime() - startNanos) / 1_000_000;

      assertInstanceOf(io.minikafka.protocol.PublishResp.class, response);
      assertTrue(elapsedMs < 1000, "post-recovery publish should not wait on the admission gate");
    }
  }

  private static byte[] value(long i) {
    return ("record-" + i).getBytes(StandardCharsets.UTF_8);
  }
}
