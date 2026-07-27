package io.minikafka.broker;

import static org.junit.jupiter.api.Assertions.assertEquals;

import io.minikafka.log.AppendResult;
import io.minikafka.log.DiskPartitionLog;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Spec 09 criterion 3: after a broker restart, {@link IdempotencyStore} is rebuilt from the
 * recovered log ({@link IdempotencyStore#rebuildFrom} runs in the {@link PartitionReplica}
 * constructor) — a retried publish of the last sequence sent before the crash is still deduped, not
 * re-appended.
 */
class IdempotencyRecoveryTest {

  private static final String TOPIC = "orders";
  private static final int PARTITION = 0;
  private static final long PRODUCER_ID = 99L;
  private static final int RECORD_COUNT = 43; // seq 0..42

  private record Broker(PartitionManager partitionManager) {
    void stop() {
      partitionManager.close();
    }
  }

  private Broker startBroker(Path logDir, Path offsetDir) throws IOException {
    BrokerConfig config =
        TestBrokerConfig.singleBroker(logDir, offsetDir, TopicConfig.parse(TOPIC + ":1", 1));
    io.minikafka.protocol.BrokerInfo self =
        new io.minikafka.protocol.BrokerInfo(
            config.brokerId(), config.brokerHost(), config.brokerPort());
    MetadataService metadataService =
        new MetadataService(self, config.topicConfig(), config.clusterConfig());
    PartitionManager partitionManager =
        new PartitionManager(
            config,
            metadataService,
            config.clusterConfig(),
            tp -> new DiskPartitionLog(config.logConfigFor(tp)));
    metadataService.attachPartitionManager(partitionManager);
    partitionManager.start();
    return new Broker(partitionManager);
  }

  @Test
  void retryOfLastSeqAfterRestartIsStillDeduped(@TempDir Path tempDir) throws Exception {
    Path logDir = tempDir.resolve("logs");
    Path offsetDir = tempDir.resolve("offsets");

    Broker broker1 = startBroker(logDir, offsetDir);
    long lastOffset = -1;
    for (int seq = 0; seq < RECORD_COUNT; seq++) {
      lastOffset =
          broker1
              .partitionManager()
              .publish(TOPIC, PARTITION, PRODUCER_ID, seq, null, value(seq))
              .offset();
    }
    broker1.stop();

    Broker broker2 = startBroker(logDir, offsetDir);
    try {
      long logSizeAfterRestart =
          broker2
              .partitionManager()
              .replica(new TopicPartition(TOPIC, PARTITION))
              .partitionLog()
              .nextOffset();
      assertEquals(RECORD_COUNT, logSizeAfterRestart);

      AppendResult retry =
          broker2
              .partitionManager()
              .publish(
                  TOPIC, PARTITION, PRODUCER_ID, RECORD_COUNT - 1, null, value(RECORD_COUNT - 1));
      assertEquals(lastOffset, retry.offset());
      assertEquals(
          RECORD_COUNT,
          broker2
              .partitionManager()
              .replica(new TopicPartition(TOPIC, PARTITION))
              .partitionLog()
              .nextOffset(),
          "retried seq must not have appended a new record");
    } finally {
      broker2.stop();
    }
  }

  @Test
  void nonIdempotentRecordAfterACommittedDuplicateIsNotReappendedOnRestart(@TempDir Path tempDir)
      throws Exception {
    // Regression for a replay-accounting bug: a committed DUPLICATE occupies a Raft index but
    // appends nothing, so the replay-skip boundary must be tracked by Raft index
    // (AppliedIndexStore),
    // not by counting physical PartitionLog records. A boundary derived from record count would
    // under-skip by one entry here and re-apply the non-idempotent record below on restart.
    Path logDir = tempDir.resolve("logs");
    Path offsetDir = tempDir.resolve("offsets");

    Broker broker1 = startBroker(logDir, offsetDir);
    broker1.partitionManager().publish(TOPIC, PARTITION, PRODUCER_ID, 0L, null, value(0)); // APPEND
    broker1
        .partitionManager()
        .publish(TOPIC, PARTITION, PRODUCER_ID, 0L, null, value(0)); // DUPLICATE
    broker1
        .partitionManager()
        .publish(
            TOPIC, PARTITION, -1L, -1L, null, "non-idempotent".getBytes(StandardCharsets.UTF_8));
    broker1.stop();

    Broker broker2 = startBroker(logDir, offsetDir);
    try {
      long logSize =
          broker2
              .partitionManager()
              .replica(new TopicPartition(TOPIC, PARTITION))
              .partitionLog()
              .nextOffset();
      assertEquals(2, logSize, "the non-idempotent record must not be duplicated by replay");
    } finally {
      broker2.stop();
    }
  }

  private static byte[] value(int i) {
    return ("record-" + i).getBytes(StandardCharsets.UTF_8);
  }
}
