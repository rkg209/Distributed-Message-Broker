package io.minikafka.broker;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.minikafka.log.DiskPartitionLog;
import io.minikafka.log.LogRecord;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.List;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Spec 07 test-plan criterion 5: a 1-replica Raft group (no peers configured) still elects itself
 * leader and commits — the code path {@link PartitionReplica#append} relies on for every Spec 02–04
 * single-broker E2E test to stay green under Spec 07's real Raft wiring.
 */
class SingleNodeRaftPublishTest {

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
  void aSoleReplicaElectsItselfAndCommits() throws Exception {
    long offset = partitionManager.publish(TOPIC, PARTITION, null, value(0)).offset();
    assertEquals(0, offset);

    List<LogRecord> records = partitionManager.poll(TOPIC, PARTITION, 0, 1024 * 1024);
    assertEquals(1, records.size());
    assertEquals("record-0", new String(records.get(0).value(), StandardCharsets.UTF_8));

    PartitionReplica replica = partitionManager.replica(TP);
    assertTrue(replica.isLeader());
  }

  private static byte[] value(int i) {
    return ("record-" + i).getBytes(StandardCharsets.UTF_8);
  }
}
