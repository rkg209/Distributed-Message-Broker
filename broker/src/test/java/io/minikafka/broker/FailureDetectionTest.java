package io.minikafka.broker;

import java.io.IOException;
import java.nio.file.Path;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * AC-2: closing a broker's acceptor makes its peers report SUSPECTED within {@code 2 x
 * heartbeatTimeout}, and restarting it flips them back to ALIVE via the reconnect path.
 */
class FailureDetectionTest {

  private static final long HEARTBEAT_INTERVAL_MS = 50;
  private static final long HEARTBEAT_TIMEOUT_MS = 300;

  private TestCluster cluster;

  @AfterEach
  void stopCluster() {
    if (cluster != null) {
      cluster.close();
    }
  }

  @Test
  void peersDetectCrashAndRecovery(@TempDir Path tempDir) throws IOException {
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

    HeartbeatMonitorTest.awaitState(cluster.node(1).heartbeatMonitor(), 2, PeerState.ALIVE);
    HeartbeatMonitorTest.awaitState(cluster.node(3).heartbeatMonitor(), 2, PeerState.ALIVE);

    cluster.stopAcceptor(2);

    HeartbeatMonitorTest.awaitState(cluster.node(1).heartbeatMonitor(), 2, PeerState.SUSPECTED);
    HeartbeatMonitorTest.awaitState(cluster.node(3).heartbeatMonitor(), 2, PeerState.SUSPECTED);

    cluster.restartAcceptor(2);

    HeartbeatMonitorTest.awaitState(cluster.node(1).heartbeatMonitor(), 2, PeerState.ALIVE);
    HeartbeatMonitorTest.awaitState(cluster.node(3).heartbeatMonitor(), 2, PeerState.ALIVE);
  }
}
