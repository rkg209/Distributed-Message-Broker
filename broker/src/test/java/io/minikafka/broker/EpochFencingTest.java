package io.minikafka.broker;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.minikafka.protocol.AppendEntriesReq;
import io.minikafka.protocol.ErrorResp;
import io.minikafka.protocol.Message;
import java.nio.file.Path;
import java.util.List;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

/**
 * Spec 08 acceptance criterion 2 (INV-4): after leader L is deposed (killed), a survivor at the
 * new, higher term must reject an {@code AppendEntries} carrying L's stale term with {@code
 * CODE_STALE_LEADER_EPOCH} rather than treat it as an ordinary log-mismatch rejection — and must
 * not apply any entries it carries. This drives the request directly at a survivor's {@link
 * BrokerRequestHandler} (bypassing the network) since the fencing check being tested lives entirely
 * in {@link BrokerRequestHandler#handle}, not in the wire transport.
 */
class EpochFencingTest {

  private static final String TOPIC = "orders";
  private static final int PARTITION = 0;
  private static final TopicPartition TP = new TopicPartition(TOPIC, PARTITION);

  private TestCluster cluster;

  @AfterEach
  void stopCluster() {
    if (cluster != null) {
      cluster.close();
    }
  }

  @Test
  @Timeout(30)
  void staleLeaderAppendEntriesIsFencedAfterFailover(@TempDir Path tempDir) throws Exception {
    TopicConfig topicConfig = TopicConfig.parse(TOPIC + ":1", 1);
    cluster = TestCluster.start(3, TOPIC + ":0=1,2,3", 3, topicConfig, 200, 2000, 200, tempDir);

    int oldLeaderId = RaftTestSupport.awaitLeader(cluster, TP);
    long staleTerm = cluster.leaderEpochOf(oldLeaderId, TP);

    cluster.killBroker(oldLeaderId);
    int newLeaderId = RaftTestSupport.awaitNewLeader(cluster, TP, oldLeaderId);
    long newTerm = cluster.leaderEpochOf(newLeaderId, TP);
    assertTrue(newTerm > staleTerm, "new leader's term must exceed the deposed leader's");

    int survivorId =
        List.of(1, 2, 3).stream()
            .filter(id -> id != oldLeaderId && id != newLeaderId)
            .findFirst()
            .orElseThrow();
    TestCluster.BrokerNode survivor = cluster.node(survivorId);
    long survivorLogSizeBefore =
        survivor.partitionManager().replica(TP).partitionLog().nextOffset();

    BrokerRequestHandler survivorHandler =
        new BrokerRequestHandler(
            survivor.metadataService(),
            survivor.partitionManager(),
            survivor.consumerGroupManager(),
            1024 * 1024);

    AppendEntriesReq staleReq =
        new AppendEntriesReq(
            1L,
            TOPIC,
            PARTITION,
            staleTerm,
            oldLeaderId,
            0L,
            0L,
            List.of(new AppendEntriesReq.Entry(staleTerm, 999L, "forged".getBytes())),
            0L);

    Message response = survivorHandler.handle(staleReq);

    ErrorResp err = assertInstanceOf(ErrorResp.class, response);
    assertEquals(ErrorResp.CODE_STALE_LEADER_EPOCH, err.errorCode());

    long survivorLogSizeAfter = survivor.partitionManager().replica(TP).partitionLog().nextOffset();
    assertEquals(
        survivorLogSizeBefore, survivorLogSizeAfter, "fenced request must not grow the log");
  }
}
