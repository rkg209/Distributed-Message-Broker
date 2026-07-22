package io.minikafka.broker;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.minikafka.log.DiskPartitionLog;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Spec 07 test-plan criterion 6: the empty-command no-op entry every new Raft leader appends (Raft
 * §5.4.2, {@code RaftNode#becomeLeader}) must never be applied as a record — {@link
 * PartitionReplica#apply} special-cases {@code command.length == 0} instead of decoding it, so the
 * durable {@link io.minikafka.log.PartitionLog} never gets a corrupt/empty entry.
 */
class NoOpEntryTest {

  private static final String TOPIC = "orders";
  private static final int PARTITION = 0;
  private static final TopicPartition TP = new TopicPartition(TOPIC, PARTITION);

  private PartitionManager partitionManager;

  @BeforeEach
  void startBroker(@TempDir Path tempDir) throws IOException {
    BrokerConfig config =
        TestBrokerConfig.singleBroker(
            tempDir.resolve("logs"),
            tempDir.resolve("offsets"),
            TopicConfig.parse(TOPIC + ":1", 1));
    MetadataService metadataService =
        new MetadataService(
            new io.minikafka.protocol.BrokerInfo(
                config.brokerId(), config.brokerHost(), config.brokerPort()),
            config.topicConfig(),
            config.clusterConfig());
    partitionManager =
        new PartitionManager(
            config,
            metadataService,
            config.clusterConfig(),
            tp -> new DiskPartitionLog(config.logConfigFor(tp)));
    metadataService.attachPartitionManager(partitionManager);
    partitionManager.start();
  }

  @AfterEach
  void stopBroker() {
    partitionManager.close();
  }

  @Test
  void leadersNoOpEntryIsNeverAppliedAsARecord() throws Exception {
    PartitionReplica replica = partitionManager.replica(TP);
    awaitLeader(replica);

    // becomeLeader() already appended (and committed, being a solo group) the per-term no-op
    // entry by the time this replica is elected — RaftNode's commit index reflects it even though
    // no publish has happened yet.
    assertTrue(replica.commitIndex() >= 1, "expected the no-op entry to already be committed");
    assertEquals(0, replica.partitionLog().nextOffset(), "no-op entry must not become a record");

    long offset = partitionManager.publish(TOPIC, PARTITION, null, value(0)).offset();
    assertEquals(0, offset);
    assertEquals(1, replica.partitionLog().nextOffset());
  }

  private static void awaitLeader(PartitionReplica replica) throws IOException {
    long deadline = System.nanoTime() + 5_000_000_000L;
    while (System.nanoTime() < deadline) {
      if (replica.isLeader()) {
        return;
      }
      try {
        Thread.sleep(10);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new IOException(e);
      }
    }
    throw new IOException("Replica never became leader");
  }

  private static byte[] value(int i) {
    return ("record-" + i).getBytes(StandardCharsets.UTF_8);
  }
}
