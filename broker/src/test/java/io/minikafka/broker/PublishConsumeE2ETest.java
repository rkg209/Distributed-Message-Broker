package io.minikafka.broker;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.minikafka.client.BrokerConnection;
import io.minikafka.client.ConsumerClient;
import io.minikafka.client.ProducerClient;
import io.minikafka.protocol.BrokerInfo;
import io.minikafka.protocol.PollResp;
import io.minikafka.protocol.ProtocolConfig;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** Spec 02 acceptance: a real producer publishes over TCP and a real consumer polls it back. */
class PublishConsumeE2ETest {

  private static final String TOPIC = "orders";
  private static final int PARTITION = 0;
  private static final int RECORD_COUNT = 1000;

  private ConnectionAcceptor acceptor;
  private TopicRegistry topicRegistry;

  @BeforeEach
  void startBroker() throws IOException {
    BrokerInfo self = new BrokerInfo(1, "localhost", 0);
    topicRegistry = new TopicRegistry();
    PartitionManager partitionManager = new PartitionManager(topicRegistry);
    BrokerRequestHandler handler = new BrokerRequestHandler(self, partitionManager, 1024 * 1024);
    acceptor = new ConnectionAcceptor(0, ProtocolConfig.DEFAULT_MAX_FRAME_BYTES, handler);
    acceptor.start();
  }

  @AfterEach
  void stopBroker() {
    acceptor.close();
    topicRegistry.close();
  }

  @Test
  void producerPublishesAndConsumerReceivesAllRecordsInOrder() throws IOException {
    try (BrokerConnection producerConn = connect()) {
      ProducerClient producer = new ProducerClient(producerConn);
      for (int i = 0; i < RECORD_COUNT; i++) {
        long offset = producer.publish(TOPIC, PARTITION, value(i));
        assertEquals(i, offset);
      }
    }

    try (BrokerConnection consumerConn = connect()) {
      ConsumerClient consumer = new ConsumerClient(consumerConn, TOPIC, PARTITION, 0);
      List<PollResp.Record> drained = drain(consumer);

      assertEquals(RECORD_COUNT, drained.size());
      for (int i = 0; i < RECORD_COUNT; i++) {
        assertEquals(i, drained.get(i).offset());
        assertEquals(
            new String(value(i), StandardCharsets.UTF_8),
            new String(drained.get(i).payload(), StandardCharsets.UTF_8));
      }
    }
  }

  @Test
  void consumerFromMidOffsetReceivesOnlyTheTail() throws IOException {
    try (BrokerConnection producerConn = connect()) {
      ProducerClient producer = new ProducerClient(producerConn);
      for (int i = 0; i < RECORD_COUNT; i++) {
        producer.publish(TOPIC, PARTITION, value(i));
      }
    }

    try (BrokerConnection consumerConn = connect()) {
      ConsumerClient consumer = new ConsumerClient(consumerConn, TOPIC, PARTITION, 500);
      List<PollResp.Record> drained = drain(consumer);

      assertEquals(500, drained.size());
      assertEquals(500, drained.get(0).offset());
      assertEquals(999, drained.get(drained.size() - 1).offset());
    }
  }

  @Test
  void pollBeyondLastOffsetReturnsEmptyBatchNotAnError() throws IOException {
    try (BrokerConnection producerConn = connect()) {
      new ProducerClient(producerConn).publish(TOPIC, PARTITION, value(0));
    }

    try (BrokerConnection consumerConn = connect()) {
      ConsumerClient consumer = new ConsumerClient(consumerConn, TOPIC, PARTITION, 50);
      List<PollResp.Record> records = consumer.poll();
      assertTrue(records.isEmpty());
      assertEquals(50, consumer.currentOffset());
    }
  }

  private BrokerConnection connect() throws IOException {
    return new BrokerConnection(
        "localhost", acceptor.boundPort(), ProtocolConfig.DEFAULT_MAX_FRAME_BYTES);
  }

  private static byte[] value(int i) {
    return ("record-" + i).getBytes(StandardCharsets.UTF_8);
  }

  private static List<PollResp.Record> drain(ConsumerClient consumer) throws IOException {
    List<PollResp.Record> all = new ArrayList<>();
    List<PollResp.Record> batch;
    while (!(batch = consumer.poll()).isEmpty()) {
      all.addAll(batch);
    }
    return all;
  }
}
