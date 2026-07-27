package io.minikafka.broker;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.minikafka.client.BrokerConnection;
import io.minikafka.client.ProducerClient;
import io.minikafka.log.DiskPartitionLog;
import io.minikafka.protocol.BrokerInfo;
import io.minikafka.protocol.Message;
import io.minikafka.protocol.PollReq;
import io.minikafka.protocol.PollResp;
import io.minikafka.protocol.ProtocolConfig;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

/**
 * AC-3: the broker buffers nothing per consumer — {@code handlePoll} reads straight from the log,
 * holding the read lock only for the duration of one segment read. A consumer that sleeps between
 * polls therefore only hurts itself: a second, fast consumer on the same topic should see
 * throughput within tolerance of running alone.
 */
class SlowConsumerIsolationTest {

  private static final String TOPIC = "orders";
  private static final int PARTITION = 0;
  private static final int RECORD_COUNT = 3000;
  private static final long SLOW_CONSUMER_SLEEP_MS = 5000;

  private ConnectionAcceptor acceptor;
  private PartitionManager partitionManager;

  @BeforeEach
  void startBroker(@TempDir Path tempDir) throws IOException {
    BrokerConfig config =
        TestBrokerConfig.singleBroker(
            tempDir.resolve("logs"),
            tempDir.resolve("offsets"),
            TopicConfig.parse(TOPIC + ":1", 1));
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
        new BrokerRequestHandler(metadataService, partitionManager, groupManager, 1024 * 1024);
    acceptor = new ConnectionAcceptor(0, ProtocolConfig.DEFAULT_MAX_FRAME_BYTES, handler);
    acceptor.start();

    try (BrokerConnection producerConn =
        new BrokerConnection(
            "localhost", acceptor.boundPort(), ProtocolConfig.DEFAULT_MAX_FRAME_BYTES)) {
      ProducerClient producer = new ProducerClient(producerConn);
      for (int i = 0; i < RECORD_COUNT; i++) {
        producer.publish(TOPIC, PARTITION, value(i));
      }
    }
  }

  @AfterEach
  void stopBroker() {
    acceptor.close();
    partitionManager.close();
  }

  @Test
  @Timeout(60)
  void fastConsumerThroughputIsUnaffectedByAConcurrentSlowConsumer() throws Exception {
    long baselineMs = timeFastDrain();

    AtomicBoolean stopSlowConsumer = new AtomicBoolean(false);
    CountDownLatch slowConsumerStarted = new CountDownLatch(1);
    Thread slowConsumer =
        Thread.ofVirtual()
            .start(
                () -> {
                  try (BrokerConnection conn =
                      new BrokerConnection(
                          "localhost",
                          acceptor.boundPort(),
                          ProtocolConfig.DEFAULT_MAX_FRAME_BYTES)) {
                    long offset = 0;
                    slowConsumerStarted.countDown();
                    while (!stopSlowConsumer.get()) {
                      Message response =
                          conn.request(
                              new PollReq(conn.nextCorrelationId(), TOPIC, PARTITION, offset));
                      PollResp poll = (PollResp) response;
                      if (!poll.records().isEmpty()) {
                        offset = poll.records().get(poll.records().size() - 1).offset() + 1;
                      }
                      Thread.sleep(SLOW_CONSUMER_SLEEP_MS);
                    }
                  } catch (Exception ignored) {
                    // Closed from the main thread on test teardown.
                  }
                });

    slowConsumerStarted.await();
    long withSlowConsumerMs = timeFastDrain();
    stopSlowConsumer.set(true);
    slowConsumer.interrupt();
    slowConsumer.join(TimeUnit.SECONDS.toMillis(5));

    assertTrue(
        withSlowConsumerMs <= Math.max(500, baselineMs * 5),
        "fast consumer slowed too much by a stalled peer: baseline="
            + baselineMs
            + "ms withSlowConsumer="
            + withSlowConsumerMs
            + "ms");
  }

  /** Drains every record from offset 0 as fast as possible and returns elapsed millis. */
  private long timeFastDrain() throws IOException {
    long start = System.nanoTime();
    try (BrokerConnection conn =
        new BrokerConnection(
            "localhost", acceptor.boundPort(), ProtocolConfig.DEFAULT_MAX_FRAME_BYTES)) {
      long offset = 0;
      int received = 0;
      while (received < RECORD_COUNT) {
        Message response =
            conn.request(new PollReq(conn.nextCorrelationId(), TOPIC, PARTITION, offset));
        PollResp poll = (PollResp) response;
        if (poll.records().isEmpty()) {
          break;
        }
        received += poll.records().size();
        offset = poll.records().get(poll.records().size() - 1).offset() + 1;
      }
      assertEquals(RECORD_COUNT, received);
    }
    return (System.nanoTime() - start) / 1_000_000;
  }

  private static byte[] value(int i) {
    return ("record-" + i).getBytes(StandardCharsets.UTF_8);
  }
}
