package io.minikafka.broker;

import static org.junit.jupiter.api.Assertions.assertEquals;

import io.minikafka.log.AppendResult;
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
 * Spec 09 criterion 1: publishing the same {@code (producerId, seqNo)} twice appends exactly once —
 * the second publish is deduped in {@link PartitionReplica#apply} and returns the same offset.
 */
class IdempotentProduceTest {

  private static final String TOPIC = "orders";
  private static final int PARTITION = 0;
  private static final long PRODUCER_ID = 42L;

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
  void retriedPublishWithSameSeqIsDedupedNotDuplicated() throws Exception {
    AppendResult first =
        partitionManager.publish(TOPIC, PARTITION, PRODUCER_ID, 0L, null, value(0));
    AppendResult retry =
        partitionManager.publish(TOPIC, PARTITION, PRODUCER_ID, 0L, null, value(0));

    assertEquals(first.offset(), retry.offset());

    PartitionReplica replica = partitionManager.replica(new TopicPartition(TOPIC, PARTITION));
    assertEquals(1, replica.partitionLog().nextOffset());

    List<LogRecord> records = partitionManager.poll(TOPIC, PARTITION, 0, 1024 * 1024);
    assertEquals(1, records.size());
  }

  private static byte[] value(int i) {
    return ("record-" + i).getBytes(StandardCharsets.UTF_8);
  }
}
