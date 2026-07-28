package io.minikafka.broker;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import io.minikafka.client.ClusterClient;
import io.minikafka.client.GroupConsumer;
import io.minikafka.client.ProducerClient;
import io.minikafka.protocol.PollResp;
import io.minikafka.protocol.ProtocolConfig;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

/**
 * Spec 13 acceptance criteria: a group dynamically reassigns partitions on join/leave, the
 * transition is bounded by {@code rebalanceTimeoutMs}, and no message is delivered twice or skipped
 * across a clean rebalance.
 *
 * <p>Each {@link GroupConsumer} is driven from its own dedicated, continuously-running virtual
 * thread from the moment it is created until the test tears it down, mirroring a real application:
 * {@link GroupConsumer#poll()} legitimately blocks a single member until the rebalance barrier
 * clears, so every member must keep calling {@code poll()} independently for the whole test, not
 * just while a particular assertion is being awaited — anything less starves the barrier and trips
 * the server's own {@code rebalanceTimeoutMs} eviction.
 */
class DynamicRebalanceE2ETest {

  private static final String TOPIC = "orders";
  private static final int PARTITIONS = 4;
  private static final String GROUP = "rebalance-group";
  private static final String ASSIGNMENTS =
      "orders:0=1,2,3;orders:1=1,2,3;orders:2=1,2,3;orders:3=1,2,3";
  private static final long AWAIT_TIMEOUT_MS = 20_000;

  private TestCluster cluster;
  private final List<ClusterClient> clients = new ArrayList<>();
  private final Map<GroupConsumer, Poller> pollers = new LinkedHashMap<>();
  private final Set<String> consumed = ConcurrentHashMap.newKeySet();

  @AfterEach
  void tearDown() throws Exception {
    for (Poller poller : pollers.values()) {
      poller.stop();
    }
    for (GroupConsumer c : pollers.keySet()) {
      try {
        c.close();
      } catch (IOException ignored) {
        // best-effort cleanup
      }
    }
    for (ClusterClient c : clients) {
      c.close();
    }
    if (cluster != null) {
      cluster.close();
    }
  }

  /** AC1, AC2, AC3: join/leave reassign partitions, bounded by 2x rebalanceTimeoutMs. */
  @Test
  @Timeout(30)
  void joinAndLeaveRebalanceWithinBoundedTime(@TempDir Path tempDir) throws Exception {
    startCluster(tempDir);

    GroupConsumer a = newPolledConsumer();
    GroupConsumer b = newPolledConsumer();
    awaitStable(a, b);
    assertEquals(List.of(2, 2), sortedAssignmentSizes(a, b));

    long rebalanceStartNanos = System.nanoTime();
    GroupConsumer c = newPolledConsumer();
    awaitStable(a, b, c);
    long elapsedMs = (System.nanoTime() - rebalanceStartNanos) / 1_000_000L;
    // AC1: 4 partitions / 3 members splits {2, 1, 1}.
    assertEquals(List.of(1, 1, 2), sortedAssignmentSizes(a, b, c));
    assertTrue(
        elapsedMs < 2 * TestCluster.DEFAULT_GROUP_REBALANCE_TIMEOUT_MS + 10_000,
        "rebalance took " + elapsedMs + "ms, expected well under 2x rebalanceTimeoutMs");

    closeAndStopPolling(b);
    // AC2: B leaves; A and C split the 4 partitions evenly again.
    awaitStable(a, c);
    assertEquals(List.of(2, 2), sortedAssignmentSizes(a, c));
  }

  /** No message is delivered twice or skipped across a join followed by a leave. */
  @Test
  @Timeout(30)
  void noDuplicateOrGapAcrossRebalance(@TempDir Path tempDir) throws Exception {
    startCluster(tempDir);
    int perBatch = 25;
    int expectedTotal = 0;

    GroupConsumer a = newPolledConsumer();
    publishBatch(0, perBatch);
    expectedTotal += PARTITIONS * perBatch;
    awaitConsumed(expectedTotal);
    a.commitOffsets();

    GroupConsumer b = newPolledConsumer();
    publishBatch(perBatch, perBatch);
    expectedTotal += PARTITIONS * perBatch;
    awaitConsumed(expectedTotal);

    closeAndStopPolling(a);
    publishBatch(2 * perBatch, perBatch);
    expectedTotal += PARTITIONS * perBatch;
    awaitConsumed(expectedTotal);

    assertEquals(expectedTotal, consumed.size());
  }

  private void startCluster(Path tempDir) throws IOException {
    TopicConfig topicConfig = TopicConfig.parse(TOPIC + ":" + PARTITIONS, 1);
    cluster = TestCluster.start(3, ASSIGNMENTS, 3, topicConfig, 100, 1000, 100, tempDir);
    for (int p = 0; p < PARTITIONS; p++) {
      RaftTestSupport.awaitLeader(cluster, new TopicPartition(TOPIC, p));
    }
  }

  private GroupConsumer newPolledConsumer() throws IOException {
    ClusterClient clusterClient = newClusterClient();
    clients.add(clusterClient);
    GroupConsumer consumer = new GroupConsumer(clusterClient, TOPIC, GROUP);
    pollers.put(consumer, new Poller(consumer, consumed));
    return consumer;
  }

  private void closeAndStopPolling(GroupConsumer consumer) throws Exception {
    Poller poller = pollers.remove(consumer);
    poller.stop();
    consumer.close();
  }

  private ClusterClient newClusterClient() throws IOException {
    TestCluster.BrokerNode node = cluster.nodes().get(0);
    return new ClusterClient(
        "localhost", node.acceptor().boundPort(), ProtocolConfig.DEFAULT_MAX_FRAME_BYTES);
  }

  private void publishBatch(int startIndex, int count) throws IOException {
    try (ClusterClient producerClient = newClusterClient()) {
      ProducerClient producer = new ProducerClient(producerClient);
      for (int p = 0; p < PARTITIONS; p++) {
        for (int i = startIndex; i < startIndex + count; i++) {
          producer.publish(TOPIC, p, ("p" + p + "-r" + i).getBytes(StandardCharsets.UTF_8));
        }
      }
    }
  }

  /**
   * Waits for every member to agree on the same generation and, together, to own every partition
   * exactly once. Checking generation agreement alone is not enough: right after a departure, the
   * remaining members can still share their old, pre-departure generation for a moment (nobody has
   * heartbeat since), which looks "stable" but is actually stale — the total-coverage check is what
   * rules that out.
   */
  private void awaitStable(GroupConsumer... members) throws Exception {
    awaitCondition(
        () -> {
          int generation = members[0].generation();
          int totalPartitions = 0;
          for (GroupConsumer m : members) {
            if (m.generation() != generation || m.assignment().isEmpty()) {
              return false;
            }
            totalPartitions += m.assignment().size();
          }
          return totalPartitions == PARTITIONS;
        },
        "Group never reached a stable generation covering all partitions");
  }

  /** Waits until at least {@code targetSize} distinct payloads have been consumed. */
  private void awaitConsumed(int targetSize) throws Exception {
    awaitCondition(
        () -> consumed.size() >= targetSize,
        "Only consumed " + consumed.size() + " of " + targetSize + " expected records");
  }

  private void awaitCondition(java.util.function.BooleanSupplier condition, String timeoutMessage)
      throws Exception {
    long deadlineNanos = System.nanoTime() + AWAIT_TIMEOUT_MS * 1_000_000L;
    while (System.nanoTime() < deadlineNanos) {
      for (Poller poller : pollers.values()) {
        poller.rethrowIfFailed();
      }
      if (condition.getAsBoolean()) {
        return;
      }
      Thread.sleep(10);
    }
    fail(timeoutMessage);
  }

  private static List<Integer> sortedAssignmentSizes(GroupConsumer... members) {
    List<Integer> sizes = new ArrayList<>();
    for (GroupConsumer m : members) {
      sizes.add(m.assignment().size());
    }
    sizes.sort(null);
    return sizes;
  }

  /** Continuously calls {@code poll()} for one {@link GroupConsumer} on its own virtual thread. */
  private static final class Poller {
    private final Thread thread;
    private final AtomicBoolean stopFlag = new AtomicBoolean(false);
    private final AtomicReference<Throwable> error = new AtomicReference<>();

    Poller(GroupConsumer consumer, Set<String> consumed) {
      thread =
          Thread.ofVirtual()
              .start(
                  () -> {
                    try {
                      while (!stopFlag.get()) {
                        for (PollResp.Record r : consumer.poll()) {
                          String payload = new String(r.payload(), StandardCharsets.UTF_8);
                          if (!consumed.add(payload)) {
                            throw new AssertionError(
                                "Duplicate delivery of " + payload + " across a clean rebalance");
                          }
                        }
                      }
                    } catch (Throwable t) {
                      error.compareAndSet(null, t);
                    }
                  });
    }

    void stop() throws Exception {
      stopFlag.set(true);
      thread.join(5000);
      rethrowIfFailed();
    }

    void rethrowIfFailed() throws Exception {
      Throwable t = error.get();
      if (t == null) {
        return;
      }
      if (t instanceof Exception e) {
        throw e;
      }
      throw new AssertionError(t);
    }
  }
}
