package io.minikafka.chaos;

import static org.junit.jupiter.api.Assertions.assertEquals;

import io.minikafka.chaos.check.CheckResult;
import io.minikafka.chaos.check.LossChecker;
import io.minikafka.client.ClusterClient;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * AC-3: the current leader is brought up with a real, non-zero {@code BROKER_FSYNC_DELAY_MS} — a
 * genuine slow-disk simulation at the single {@code LogSegment.force()} chokepoint, since {@code
 * tc} shapes network queues rather than disk I/O. Asserts no loss despite the slow leader.
 */
class DiskSlownessChaosTest {

  private static DockerCluster cluster;
  private static ClusterClient bootstrapClient;

  @BeforeAll
  static void startCluster() throws Exception {
    cluster = DockerCluster.start(ChaosTestSupport.COMPOSE_DIR);
    bootstrapClient = ChaosTestSupport.bootstrapClient(cluster);
  }

  @AfterAll
  static void stopCluster() {
    if (cluster != null) {
      cluster.close();
    }
  }

  @Test
  void noLossWithASlowLeaderDisk() throws Exception {
    ChaosConfig config = ChaosConfig.fromSystemProperties();
    FaultInjector faultInjector = new FaultInjector(cluster, ChaosTestSupport.COMPOSE_DIR);

    bootstrapClient.refresh();
    int leader = bootstrapClient.leaderFor(config.topic(), 0);
    faultInjector.slowDisk(leader, 200);
    try {
      ChaosConfig noCrashes =
          new ChaosConfig(
              0,
              config.messages(),
              config.producers(),
              config.consumers(),
              config.topic(),
              config.partitions(),
              config.seed(),
              Long.MAX_VALUE,
              config.recoverySettleMs(),
              config.partitionHealMs(),
              config.runTimeoutMs());
      ChaosOrchestrator orchestrator =
          new ChaosOrchestrator(noCrashes, bootstrapClient, faultInjector);
      ChaosReport report = orchestrator.run();

      CheckResult loss = new LossChecker().check(report.history());
      assertEquals(
          CheckResult.Status.PASS, loss.status(), loss.summary() + " " + loss.violations());
    } finally {
      faultInjector.healAll();
    }
  }
}
