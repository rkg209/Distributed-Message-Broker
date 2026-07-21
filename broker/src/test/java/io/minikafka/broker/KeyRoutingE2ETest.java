package io.minikafka.broker;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.minikafka.client.BrokerConnection;
import io.minikafka.client.MetadataClient;
import io.minikafka.client.PartitionRouter;
import io.minikafka.client.ProducerClient;
import io.minikafka.protocol.BrokerInfo;
import io.minikafka.protocol.ProtocolConfig;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * AC-1: 10,000 messages across 10 distinct keys to a 4-partition topic all land in the same
 * partition per key, and per-key order is preserved.
 */
class KeyRoutingE2ETest {

  private static final String TOPIC = "orders";
  private static final int PARTITIONS = 4;
  private static final int KEY_COUNT = 10;
  private static final int RECORD_COUNT = 10_000;

  private ConnectionAcceptor acceptor;
  private TopicRegistry topicRegistry;
  private ConsumerGroupManager consumerGroupManager;

  @BeforeEach
  void startBroker(@TempDir Path tempDir) throws IOException {
    BrokerInfo self = new BrokerInfo(1, "localhost", 0);
    MetadataService metadataService =
        new MetadataService(self, TopicConfig.parse(TOPIC + ":" + PARTITIONS, 1));
    topicRegistry = new TopicRegistry();
    PartitionManager partitionManager = new PartitionManager(topicRegistry, metadataService);
    consumerGroupManager = new ConsumerGroupManager(tempDir.resolve("offsets"));
    BrokerRequestHandler handler =
        new BrokerRequestHandler(
            metadataService, partitionManager, consumerGroupManager, 4 * 1024 * 1024);
    acceptor = new ConnectionAcceptor(0, ProtocolConfig.DEFAULT_MAX_FRAME_BYTES, handler);
    acceptor.start();
  }

  @AfterEach
  void stopBroker() {
    acceptor.close();
    topicRegistry.close();
    consumerGroupManager.close();
  }

  @Test
  void sameKeyLandsInSamePartitionAndPreservesPerKeyOrder() throws IOException {
    Map<String, Integer> keyToPartition = new HashMap<>();
    try (BrokerConnection conn = connect()) {
      ProducerClient producer =
          new ProducerClient(conn, new MetadataClient(conn), new PartitionRouter());
      for (int i = 0; i < RECORD_COUNT; i++) {
        String key = "key-" + (i % KEY_COUNT);
        ProducerClient.PublishAck ack =
            producer.publish(TOPIC, key.getBytes(StandardCharsets.UTF_8), value(i));
        Integer expectedPartition = keyToPartition.get(key);
        if (expectedPartition == null) {
          keyToPartition.put(key, ack.partition());
        } else {
          assertEquals(expectedPartition, ack.partition(), "key " + key + " changed partition");
        }
      }
    }
    assertTrue(keyToPartition.size() == KEY_COUNT);

    // Per-partition order: drain each partition and check the payload sequence numbers embedded
    // per key are increasing (each key's records must appear in production order).
    for (int p = 0; p < PARTITIONS; p++) {
      List<io.minikafka.protocol.PollResp.Record> records = drainPartition(p);
      Map<String, Long> lastSeenSeqPerKey = new HashMap<>();
      for (io.minikafka.protocol.PollResp.Record record : records) {
        String text = new String(record.payload(), StandardCharsets.UTF_8);
        String[] parts = text.split("-");
        String key = parts[0] + "-" + parts[1];
        long seq = Long.parseLong(parts[3]);
        Long last = lastSeenSeqPerKey.get(key);
        if (last != null) {
          assertTrue(seq > last, "out-of-order record for " + key + ": " + last + " then " + seq);
        }
        lastSeenSeqPerKey.put(key, seq);
      }
    }
  }

  private List<io.minikafka.protocol.PollResp.Record> drainPartition(int partition)
      throws IOException {
    try (BrokerConnection conn = connect()) {
      io.minikafka.client.ConsumerClient consumer =
          new io.minikafka.client.ConsumerClient(conn, TOPIC, partition, 0L);
      List<io.minikafka.protocol.PollResp.Record> all = new java.util.ArrayList<>();
      List<io.minikafka.protocol.PollResp.Record> batch;
      while (!(batch = consumer.poll()).isEmpty()) {
        all.addAll(batch);
      }
      return all;
    }
  }

  private BrokerConnection connect() throws IOException {
    return new BrokerConnection(
        "localhost", acceptor.boundPort(), ProtocolConfig.DEFAULT_MAX_FRAME_BYTES);
  }

  private static byte[] value(int seq) {
    return ("key-" + (seq % KEY_COUNT) + "-seq-" + seq).getBytes(StandardCharsets.UTF_8);
  }
}
