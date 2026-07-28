package io.minikafka.broker;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.minikafka.client.BrokerConnection;
import io.minikafka.log.DiskPartitionLog;
import io.minikafka.protocol.BrokerInfo;
import io.minikafka.protocol.ErrorResp;
import io.minikafka.protocol.Message;
import io.minikafka.protocol.PollResp;
import io.minikafka.protocol.ProtocolConfig;
import io.minikafka.protocol.PublishReq;
import io.minikafka.protocol.PublishResp;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

/**
 * AC-2: with a small publish queue capacity and many concurrent producers, the broker rejects at
 * least one publish with {@code CODE_BROKER_BUSY}, yet every message still lands exactly once once
 * a producer retries — backpressure rejects before {@code raftNode.propose}, so a busy publish
 * never touches Raft and INV-3 (no duplicate delivery) holds across the rejection.
 */
class BrokerBusyTest {

  private static final String TOPIC = "orders";
  private static final int PARTITION = 0;
  private static final int PRODUCERS = 20;
  private static final int MESSAGES_PER_PRODUCER = 50;
  private static final int TOTAL_MESSAGES = PRODUCERS * MESSAGES_PER_PRODUCER;

  private ConnectionAcceptor acceptor;
  private PartitionManager partitionManager;

  @BeforeEach
  void startBroker(@TempDir Path tempDir) throws IOException {
    BrokerConfig config =
        TestBrokerConfig.singleBroker(
            tempDir.resolve("logs"),
            tempDir.resolve("offsets"),
            TopicConfig.parse(TOPIC + ":1", 1),
            10,
            1);
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
  @Timeout(60)
  void busyIsObservedUnderLoadButEveryMessageEventuallyLandsExactlyOnce() throws Exception {
    AtomicInteger busyObserved = new AtomicInteger();
    CountDownLatch start = new CountDownLatch(1);
    CountDownLatch done = new CountDownLatch(PRODUCERS);
    List<Exception> failures = new java.util.concurrent.CopyOnWriteArrayList<>();

    for (int p = 0; p < PRODUCERS; p++) {
      final long producerId = p;
      Thread.ofVirtual()
          .start(
              () -> {
                try (BrokerConnection conn =
                    new BrokerConnection(
                        "localhost",
                        acceptor.boundPort(),
                        ProtocolConfig.DEFAULT_MAX_FRAME_BYTES)) {
                  start.await();
                  for (long seq = 0; seq < MESSAGES_PER_PRODUCER; seq++) {
                    publishWithBusyRetry(conn, producerId, seq, busyObserved);
                  }
                } catch (Exception e) {
                  failures.add(e);
                } finally {
                  done.countDown();
                }
              });
    }

    start.countDown();
    assertTrue(
        done.await(50, java.util.concurrent.TimeUnit.SECONDS), "all producers should finish");
    assertTrue(failures.isEmpty(), "no producer thread should fail: " + failures);
    assertTrue(busyObserved.get() > 0, "expected at least one BROKER_BUSY under this load");

    Set<String> delivered = pollAll();
    assertEquals(
        TOTAL_MESSAGES, delivered.size(), "every message should be delivered exactly once");
    for (int p = 0; p < PRODUCERS; p++) {
      for (long seq = 0; seq < MESSAGES_PER_PRODUCER; seq++) {
        assertTrue(delivered.contains(tag(p, seq)), "missing " + tag(p, seq));
      }
    }
  }

  private void publishWithBusyRetry(
      BrokerConnection conn, long producerId, long seqNo, AtomicInteger busyObserved)
      throws IOException, InterruptedException {
    byte[] payload = tag((int) producerId, seqNo).getBytes(StandardCharsets.UTF_8);
    while (true) {
      Message response =
          conn.request(
              new PublishReq(
                  conn.nextCorrelationId(), TOPIC, PARTITION, producerId, seqNo, null, payload));
      if (response instanceof PublishResp) {
        return;
      }
      if (response instanceof ErrorResp err && err.errorCode() == ErrorResp.CODE_BROKER_BUSY) {
        busyObserved.incrementAndGet();
        Thread.sleep(ThreadLocalRandom.current().nextInt(5, 20));
        continue;
      }
      throw new IOException("Unexpected publish response: " + response);
    }
  }

  private Set<String> pollAll() throws IOException {
    Set<String> delivered = new HashSet<>();
    try (BrokerConnection conn =
        new BrokerConnection(
            "localhost", acceptor.boundPort(), ProtocolConfig.DEFAULT_MAX_FRAME_BYTES)) {
      long offset = 0;
      while (delivered.size() < TOTAL_MESSAGES) {
        Message response =
            conn.request(
                new io.minikafka.protocol.PollReq(
                    conn.nextCorrelationId(), TOPIC, PARTITION, offset));
        PollResp poll = (PollResp) response;
        if (poll.records().isEmpty()) {
          break;
        }
        for (PollResp.Record record : poll.records()) {
          delivered.add(new String(record.payload(), StandardCharsets.UTF_8));
          offset = record.offset() + 1;
        }
      }
    }
    return delivered;
  }

  private static String tag(int producerId, long seqNo) {
    return "p" + producerId + "-s" + seqNo;
  }
}
