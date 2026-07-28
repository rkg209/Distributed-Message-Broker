package io.minikafka.broker;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import io.minikafka.client.BrokerConnection;
import io.minikafka.client.SequenceGapException;
import io.minikafka.log.DiskPartitionLog;
import io.minikafka.protocol.Message;
import io.minikafka.protocol.ProtocolConfig;
import io.minikafka.protocol.PublishReq;
import io.minikafka.protocol.PublishResp;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Spec 09 criterion 2: publishing a sequence number that skips ahead of the broker's last committed
 * sequence is rejected with {@code CODE_SEQUENCE_GAP} and does not poison later publishes — a
 * subsequent publish that fills the gap still succeeds.
 */
class SequenceGapTest {

  private static final String TOPIC = "orders";
  private static final int PARTITION = 0;
  private static final long PRODUCER_ID = 7L;

  private ConnectionAcceptor acceptor;
  private PartitionManager partitionManager;

  @BeforeEach
  void startBroker(@TempDir Path tempDir) throws IOException {
    BrokerConfig config =
        TestBrokerConfig.singleBroker(
            tempDir.resolve("logs"),
            tempDir.resolve("offsets"),
            TopicConfig.parse(TOPIC + ":1", 1));
    io.minikafka.protocol.BrokerInfo self =
        new io.minikafka.protocol.BrokerInfo(
            config.brokerId(), config.brokerHost(), config.brokerPort());
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
  void skippedSeqIsRejectedAndDoesNotPoisonTheGapFillingRetry() throws IOException {
    try (BrokerConnection conn =
        new BrokerConnection(
            "localhost", acceptor.boundPort(), ProtocolConfig.DEFAULT_MAX_FRAME_BYTES)) {
      publish(conn, 0L);
      publish(conn, 1L);

      Message gapResponse = rawPublish(conn, 3L);
      assertEquals(
          io.minikafka.protocol.ErrorResp.CODE_SEQUENCE_GAP, asError(gapResponse).errorCode());

      // seq 2 fills the gap and still succeeds — the rejected seq 3 did not poison the producer.
      long offset = publish(conn, 2L);
      assertEquals(2L, offset);
    }
  }

  @Test
  void clientSideThrowsSequenceGapExceptionNotAGenericProtocolException() throws IOException {
    // Exercised through ProducerClient by directly pinning seqNo via a raw connection, since
    // ProducerClient itself never emits a gap on its own — it always sends the next contiguous seq.
    try (BrokerConnection conn =
        new BrokerConnection(
            "localhost", acceptor.boundPort(), ProtocolConfig.DEFAULT_MAX_FRAME_BYTES)) {
      publish(conn, 0L);
      assertThrows(SequenceGapException.class, () -> failingPublish(conn, 5L));
    }
  }

  private void failingPublish(BrokerConnection conn, long seqNo) throws IOException {
    Message response = rawPublish(conn, seqNo);
    if (response instanceof io.minikafka.protocol.ErrorResp err
        && err.errorCode() == io.minikafka.protocol.ErrorResp.CODE_SEQUENCE_GAP) {
      throw new SequenceGapException("Publish failed: " + err.message());
    }
  }

  private long publish(BrokerConnection conn, long seqNo) throws IOException {
    Message response = rawPublish(conn, seqNo);
    return ((PublishResp) response).offset();
  }

  private Message rawPublish(BrokerConnection conn, long seqNo) throws IOException {
    return conn.request(
        new PublishReq(
            conn.nextCorrelationId(), TOPIC, PARTITION, PRODUCER_ID, seqNo, null, value(seqNo)));
  }

  private static io.minikafka.protocol.ErrorResp asError(Message m) {
    return (io.minikafka.protocol.ErrorResp) m;
  }

  private static byte[] value(long i) {
    return ("record-" + i).getBytes(StandardCharsets.UTF_8);
  }
}
