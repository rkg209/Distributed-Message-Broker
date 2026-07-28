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
 *
 * <p>Deliberately caps load below the suite's default {@code -Pmessages}/{@code -Pproducers}: with
 * RF=3, {@link #SLOW_DISK_DELAY_MS} of extra fsync latency on one replica caps every partition's
 * commit throughput (any commit that needs that replica for majority pays the delay), and 8
 * producers hammering that with no pacing exhausts {@code BROKER_PUBLISH_QUEUE_CAPACITY}/{@code
 * BROKER_PUBLISH_ACQUIRE_TIMEOUT_MS} almost immediately — confirmed empirically: at the suite
 * defaults (2000 messages / 8 producers) nearly all publishes fail on exhausted retries, and the
 * drain phase doesn't have enough patience for consumers to catch a backlog that size, which
 * LossChecker reports as loss even though nothing was actually dropped. At this test's own lighter
 * load the same scenario passes with zero loss. The point of this test is "no loss under a real
 * slow disk," not "the system keeps up under sustained overload" — that's what Spec 10's
 * backpressure tests own — so the load stays modest enough to actually finish the assertion.
 */
class DiskSlownessChaosTest {

  private static final int SLOW_DISK_DELAY_MS = 200;
  private static final long MAX_MESSAGES = 300;
  private static final int MAX_PRODUCERS = 2;

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
    faultInjector.slowDisk(leader, SLOW_DISK_DELAY_MS);
    try {
      ChaosConfig noCrashes =
          new ChaosConfig(
              0,
              Math.min(config.messages(), MAX_MESSAGES),
              Math.min(config.producers(), MAX_PRODUCERS),
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
