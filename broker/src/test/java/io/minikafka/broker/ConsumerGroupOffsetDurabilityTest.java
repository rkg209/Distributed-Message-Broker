package io.minikafka.broker;

import static org.junit.jupiter.api.Assertions.assertEquals;

import io.minikafka.client.BrokerConnection;
import io.minikafka.client.ConsumerClient;
import io.minikafka.client.ProducerClient;
import io.minikafka.log.DiskPartitionLog;
import io.minikafka.log.LogConfig;
import io.minikafka.protocol.BrokerInfo;
import io.minikafka.protocol.CommitOffsetReq;
import io.minikafka.protocol.CommitOffsetResp;
import io.minikafka.protocol.Message;
import io.minikafka.protocol.PollReq;
import io.minikafka.protocol.PollResp;
import io.minikafka.protocol.ProtocolConfig;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * AC-3: a consumer in group G reads 500 of 1,000 messages and commits offset 500. The broker
 * restarts (fresh acceptor/registry/group-manager over the same directories); a new consumer in the
 * same group resumes at exactly 500 — no re-read, no gap.
 */
class ConsumerGroupOffsetDurabilityTest {

  private static final String TOPIC = "orders";
  private static final int PARTITION = 0;
  private static final int RECORD_COUNT = 1000;
  private static final String GROUP = "group-g";

  private record Broker(
      ConnectionAcceptor acceptor, TopicRegistry topicRegistry, ConsumerGroupManager groupManager) {
    void stop() {
      acceptor.close();
      topicRegistry.close();
      groupManager.close();
    }
  }

  private Broker startBroker(Path logDir, Path offsetDir) throws IOException {
    BrokerInfo self = new BrokerInfo(1, "localhost", 0);
    MetadataService metadataService = new MetadataService(self, TopicConfig.parse(null, 1));
    TopicRegistry topicRegistry =
        new TopicRegistry(
            tp ->
                new DiskPartitionLog(
                    LogConfig.defaultsFor(logDir.resolve(tp.topic() + "-" + tp.partition()))));
    PartitionManager partitionManager = new PartitionManager(topicRegistry, metadataService);
    ConsumerGroupManager groupManager = new ConsumerGroupManager(offsetDir);
    BrokerRequestHandler handler =
        new BrokerRequestHandler(metadataService, partitionManager, groupManager, 1024 * 1024);
    ConnectionAcceptor acceptor =
        new ConnectionAcceptor(0, ProtocolConfig.DEFAULT_MAX_FRAME_BYTES, handler);
    acceptor.start();
    return new Broker(acceptor, topicRegistry, groupManager);
  }

  @Test
  void consumerResumesFromCommittedOffsetAfterRestart(@TempDir Path tempDir) throws Exception {
    Path logDir = tempDir.resolve("logs");
    Path offsetDir = tempDir.resolve("offsets");

    Broker broker1 = startBroker(logDir, offsetDir);
    try (BrokerConnection producerConn = connect(broker1)) {
      ProducerClient producer = new ProducerClient(producerConn);
      for (int i = 0; i < RECORD_COUNT; i++) {
        producer.publish(TOPIC, PARTITION, value(i));
      }
    }

    // Reads exactly 500 records one request at a time (each request asks for a single explicit
    // offset, so the count read is exact regardless of how many records a batch could hold) and
    // durably commits offset 500.
    try (BrokerConnection consumerConn = connect(broker1)) {
      for (int i = 0; i < 500; i++) {
        PollReq req = new PollReq(consumerConn.nextCorrelationId(), TOPIC, PARTITION, i);
        Message response = consumerConn.request(req);
        List<PollResp.Record> records = ((PollResp) response).records();
        assertEquals(i, records.get(0).offset());
      }
      Message commitResponse =
          consumerConn.request(
              new CommitOffsetReq(consumerConn.nextCorrelationId(), GROUP, TOPIC, PARTITION, 500L));
      assertEquals(true, ((CommitOffsetResp) commitResponse).ok());
    }
    broker1.stop();

    Broker broker2 = startBroker(logDir, offsetDir);
    try {
      try (BrokerConnection consumerConn = connect(broker2)) {
        ConsumerClient consumer = new ConsumerClient(consumerConn, TOPIC, PARTITION, GROUP);
        assertEquals(500L, consumer.currentOffset());

        List<PollResp.Record> remainder = new ArrayList<>();
        List<PollResp.Record> batch;
        while (!(batch = consumer.poll()).isEmpty()) {
          remainder.addAll(batch);
        }
        assertEquals(500, remainder.size());
        assertEquals(500L, remainder.get(0).offset());
        assertEquals(999L, remainder.get(remainder.size() - 1).offset());
      }
    } finally {
      broker2.stop();
    }
  }

  private static BrokerConnection connect(Broker broker) throws IOException {
    return new BrokerConnection(
        "localhost", broker.acceptor().boundPort(), ProtocolConfig.DEFAULT_MAX_FRAME_BYTES);
  }

  private static byte[] value(int i) {
    return ("record-" + i).getBytes(StandardCharsets.UTF_8);
  }
}
