package io.minikafka.broker;

import static org.junit.jupiter.api.Assertions.assertTrue;

import io.minikafka.client.BrokerConnection;
import io.minikafka.log.DiskPartitionLog;
import io.minikafka.protocol.BrokerInfo;
import io.minikafka.protocol.ProtocolConfig;
import io.minikafka.protocol.PublishReq;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

/**
 * AC-1 proxy (the deterministic stand-in for the 60s heap soak — see {@code HeapSoakTest} for the
 * literal version): under sustained concurrent publish with no consumer draining it, in-flight
 * publishes never exceed the configured capacity and {@code RaftNode.pendingProposals} — the
 * mechanism by which the broker's heap actually stays flat under NFR-10 — stays bounded too.
 */
class BoundedInFlightTest {

  private static final String TOPIC = "orders";
  private static final int PARTITION = 0;
  private static final TopicPartition TP = new TopicPartition(TOPIC, PARTITION);
  private static final int CAPACITY = 15;
  private static final int PRODUCERS = 40;
  private static final int MESSAGES_PER_PRODUCER = 25;

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
            2000);
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
  void inFlightPublishesAndPendingProposalsStayBounded() throws Exception {
    AtomicInteger maxInFlight = new AtomicInteger();
    AtomicInteger maxPendingProposals = new AtomicInteger();
    AtomicBoolean running = new AtomicBoolean(true);

    Thread sampler =
        Thread.ofVirtual()
            .start(
                () -> {
                  while (running.get()) {
                    maxInFlight.getAndUpdate(
                        prev -> Math.max(prev, partitionManager.inFlightPublishes(TP)));
                    PartitionReplica replica = partitionManager.replica(TP);
                    if (replica != null) {
                      maxPendingProposals.getAndUpdate(
                          prev -> Math.max(prev, replica.raftNode().pendingProposalCount()));
                    }
                    try {
                      Thread.sleep(1);
                    } catch (InterruptedException e) {
                      Thread.currentThread().interrupt();
                      return;
                    }
                  }
                });

    CountDownLatch start = new CountDownLatch(1);
    CountDownLatch done = new CountDownLatch(PRODUCERS);
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
                    conn.request(
                        new PublishReq(
                            conn.nextCorrelationId(),
                            TOPIC,
                            PARTITION,
                            producerId,
                            seq,
                            null,
                            value(producerId, seq)));
                  }
                } catch (Exception ignored) {
                  // A rejected or timed-out publish doesn't invalidate the boundedness assertion.
                } finally {
                  done.countDown();
                }
              });
    }

    start.countDown();
    assertTrue(done.await(50, TimeUnit.SECONDS), "producers should finish");
    running.set(false);
    sampler.join(TimeUnit.SECONDS.toMillis(5));

    assertTrue(
        maxInFlight.get() <= CAPACITY,
        "observed in-flight " + maxInFlight.get() + " exceeded capacity " + CAPACITY);
    assertTrue(
        maxPendingProposals.get() <= CAPACITY,
        "observed pending proposals "
            + maxPendingProposals.get()
            + " exceeded capacity "
            + CAPACITY);
  }

  private static byte[] value(long producerId, long seq) {
    return ("p" + producerId + "-s" + seq).getBytes(StandardCharsets.UTF_8);
  }
}
