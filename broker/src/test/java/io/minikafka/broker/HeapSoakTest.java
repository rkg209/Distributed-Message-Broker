package io.minikafka.broker;

import static org.junit.jupiter.api.Assertions.assertTrue;

import io.minikafka.client.BrokerConnection;
import io.minikafka.log.DiskPartitionLog;
import io.minikafka.protocol.BrokerInfo;
import io.minikafka.protocol.ProtocolConfig;
import io.minikafka.protocol.PublishReq;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

/**
 * AC-1 literal: a 60-second max-speed producer burst with no consumer draining it must not grow
 * used heap without bound. Excluded from {@code ./gradlew test} (see {@code excludeTags("soak")} in
 * the root build script); run via {@code ./gradlew :broker:soakTest}. {@link BoundedInFlightTest}
 * is the fast deterministic proxy for this same guarantee.
 */
@Tag("soak")
class HeapSoakTest {

  private static final String TOPIC = "orders";
  private static final int PARTITION = 0;
  private static final int CAPACITY = 1000;
  private static final int PRODUCER_THREADS = 32;
  private static final long DURATION_MS = 60_000;

  private ConnectionAcceptor acceptor;
  private PartitionManager partitionManager;

  @BeforeEach
  void startBroker(@TempDir Path tempDir) throws IOException {
    BrokerConfig config =
        TestBrokerConfig.singleBroker(
            tempDir.resolve("logs"),
            tempDir.resolve("offsets"),
            TopicConfig.parse(TOPIC + ":1", 1),
            CAPACITY);
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
  }

  @AfterEach
  void stopBroker() {
    acceptor.close();
    partitionManager.close();
  }

  @Test
  @Timeout(180)
  void usedHeapStaysFlatUnderA60SecondMaxSpeedBurst() throws Exception {
    AtomicBoolean running = new AtomicBoolean(true);
    CountDownLatch producersDone = new CountDownLatch(PRODUCER_THREADS);
    for (int p = 0; p < PRODUCER_THREADS; p++) {
      final long producerId = p;
      Thread.ofVirtual()
          .start(
              () -> {
                try (BrokerConnection conn =
                    new BrokerConnection(
                        "localhost",
                        acceptor.boundPort(),
                        ProtocolConfig.DEFAULT_MAX_FRAME_BYTES)) {
                  long seq = 0;
                  while (running.get()) {
                    conn.request(
                        new PublishReq(
                            conn.nextCorrelationId(),
                            TOPIC,
                            PARTITION,
                            producerId,
                            seq,
                            null,
                            payload(seq)));
                    seq++;
                  }
                } catch (Exception ignored) {
                  // A busy rejection or timeout just means this iteration didn't append.
                } finally {
                  producersDone.countDown();
                }
              });
    }

    MemoryMXBean memory = ManagementFactory.getMemoryMXBean();
    List<Long> usedHeapSamples = new ArrayList<>();
    long deadline = System.currentTimeMillis() + DURATION_MS;
    while (System.currentTimeMillis() < deadline) {
      Thread.sleep(2000);
      System.gc();
      usedHeapSamples.add(memory.getHeapMemoryUsage().getUsed());
    }

    running.set(false);
    producersDone.await(30, java.util.concurrent.TimeUnit.SECONDS);

    int half = usedHeapSamples.size() / 2;
    long firstHalfMax =
        usedHeapSamples.subList(0, Math.max(half, 1)).stream().mapToLong(l -> l).max().orElse(0);
    long secondHalfMax =
        usedHeapSamples.subList(half, usedHeapSamples.size()).stream()
            .mapToLong(l -> l)
            .max()
            .orElse(0);
    System.out.println(
        "HeapSoakTest: samples="
            + usedHeapSamples.size()
            + " firstHalfMaxMB="
            + (firstHalfMax / (1024 * 1024))
            + " secondHalfMaxMB="
            + (secondHalfMax / (1024 * 1024))
            + " allSamplesMB="
            + usedHeapSamples.stream().map(b -> b / (1024 * 1024)).toList());

    assertTrue(
        secondHalfMax <= firstHalfMax * 2,
        "used heap grew unboundedly: firstHalfMax="
            + firstHalfMax
            + " secondHalfMax="
            + secondHalfMax
            + " samples="
            + usedHeapSamples);
  }

  private static byte[] payload(long seq) {
    return ("record-" + seq).getBytes(StandardCharsets.UTF_8);
  }
}
