package io.minikafka.broker;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

import io.minikafka.client.BrokerConnection;
import io.minikafka.log.DiskPartitionLog;
import io.minikafka.protocol.BrokerInfo;
import io.minikafka.protocol.Message;
import io.minikafka.protocol.PollReq;
import io.minikafka.protocol.PollResp;
import io.minikafka.protocol.ProtocolConfig;
import io.minikafka.protocol.PublishReq;
import io.minikafka.protocol.PublishResp;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.List;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

/**
 * Spec 09 acceptance criterion 5: 100 messages produced through a {@link FlakyProxy} that drops
 * ~50% of acks (the broker still commits before the drop). Every dropped ack forces a retry with
 * the same {@code (producerId, seqNo)}; the consumer must still see exactly 100 records, seq 0..99,
 * no duplicates. Seeded like {@code FrameDecoderPropertyTest} so a failure is reproducible — the
 * seed is reported in the assertion message.
 */
class ChaosDedupTest {

  private static final String TOPIC = "orders";
  private static final int PARTITION = 0;
  private static final long PRODUCER_ID = 1234L;
  private static final int RECORD_COUNT = 100;
  private static final long SEED = 20260727L;
  private static final int MAX_ATTEMPTS_PER_MESSAGE = 25;

  private ConnectionAcceptor acceptor;
  private PartitionManager partitionManager;
  private FlakyProxy proxy;

  @BeforeEach
  void startBrokerAndProxy(@TempDir Path tempDir) throws IOException {
    BrokerConfig config =
        TestBrokerConfig.singleBroker(
            tempDir.resolve("logs"),
            tempDir.resolve("offsets"),
            TopicConfig.parse(TOPIC + ":1", 1));
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
    proxy = new FlakyProxy("localhost", acceptor.boundPort(), 0.5, SEED);
  }

  @AfterEach
  void stopBrokerAndProxy() {
    proxy.close();
    acceptor.close();
    partitionManager.close();
  }

  @Test
  @Timeout(60)
  void ackLossForcesRetryButConsumerSeesNoDuplicates() throws IOException {
    for (int seq = 0; seq < RECORD_COUNT; seq++) {
      long offset = publishWithRetry(seq);
      assertEquals(seq, offset, "seed=" + SEED + " seq=" + seq + " offset must equal seq");
    }

    try (BrokerConnection consumerConn =
        new BrokerConnection(
            "localhost", acceptor.boundPort(), ProtocolConfig.DEFAULT_MAX_FRAME_BYTES)) {
      List<PollResp.Record> all = new java.util.ArrayList<>();
      long next = 0;
      while (all.size() < RECORD_COUNT) {
        Message response =
            consumerConn.request(
                new PollReq(consumerConn.nextCorrelationId(), TOPIC, PARTITION, next));
        List<PollResp.Record> batch = ((PollResp) response).records();
        if (batch.isEmpty()) {
          break;
        }
        all.addAll(batch);
        next = batch.get(batch.size() - 1).offset() + 1;
      }
      assertEquals(
          RECORD_COUNT,
          all.size(),
          "seed=" + SEED + " consumer must see exactly RECORD_COUNT records");
      for (int i = 0; i < RECORD_COUNT; i++) {
        assertEquals(
            (long) i, all.get(i).offset(), "seed=" + SEED + " no gap/duplicate at index " + i);
      }
    }
  }

  private long publishWithRetry(int seq) throws IOException {
    IOException lastError = null;
    for (int attempt = 0; attempt < MAX_ATTEMPTS_PER_MESSAGE; attempt++) {
      try (BrokerConnection conn =
          new BrokerConnection(
              "localhost", proxy.boundPort(), ProtocolConfig.DEFAULT_MAX_FRAME_BYTES)) {
        Message response =
            conn.request(
                new PublishReq(
                    conn.nextCorrelationId(),
                    TOPIC,
                    PARTITION,
                    PRODUCER_ID,
                    seq,
                    null,
                    value(seq)));
        return ((PublishResp) response).offset();
      } catch (IOException e) {
        lastError = e; // ack dropped by the proxy; broker already committed — retry same seq
      }
    }
    fail("seed=" + SEED + " seq=" + seq + " exhausted retries: " + lastError);
    throw new AssertionError("unreachable");
  }

  private static byte[] value(int i) {
    return ("record-" + i).getBytes(StandardCharsets.UTF_8);
  }
}
