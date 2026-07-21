package io.minikafka.broker;

import static org.junit.jupiter.api.Assertions.fail;

import java.io.IOException;
import java.nio.file.Path;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** AC-1: every broker in a static cluster reaches ALIVE for both its peers within the timeout. */
class HeartbeatMonitorTest {

  private static final long HEARTBEAT_INTERVAL_MS = 50;
  private static final long HEARTBEAT_TIMEOUT_MS = 500;

  private TestCluster cluster;

  @AfterEach
  void stopCluster() {
    if (cluster != null) {
      cluster.close();
    }
  }

  @Test
  void everyBrokerSeesAllPeersAsAliveWithinTimeout(@TempDir Path tempDir) throws IOException {
    cluster =
        TestCluster.start(
            3,
            null,
            1,
            TopicConfig.parse(null, 1),
            HEARTBEAT_INTERVAL_MS,
            HEARTBEAT_TIMEOUT_MS,
            HEARTBEAT_INTERVAL_MS,
            tempDir);

    for (TestCluster.BrokerNode node : cluster.nodes()) {
      for (TestCluster.BrokerNode peer : cluster.nodes()) {
        if (peer.info().brokerId() == node.info().brokerId()) {
          continue;
        }
        awaitState(node.heartbeatMonitor(), peer.info().brokerId(), PeerState.ALIVE);
      }
    }
  }

  static void awaitState(HeartbeatMonitor monitor, int brokerId, PeerState expected)
      throws IOException {
    long deadline = System.nanoTime() + 5_000_000_000L;
    while (System.nanoTime() < deadline) {
      if (monitor.stateOf(brokerId) == expected) {
        return;
      }
      try {
        Thread.sleep(20);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new IOException(e);
      }
    }
    fail("Peer " + brokerId + " never reached " + expected + "; was " + monitor.stateOf(brokerId));
  }
}
