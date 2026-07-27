package io.minikafka.chaos;

import static org.junit.jupiter.api.Assertions.assertEquals;

import io.minikafka.chaos.check.CheckResult;
import io.minikafka.chaos.check.LinearizabilityChecker;
import io.minikafka.client.ClusterClient;
import java.util.Random;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * AC-4: 10 seeds, each varying producer count, payload size, and poll/fault cadence via the seed
 * alone (every other {@link ChaosConfig} tunable stays fixed per-run so the variation is
 * reproducible). Linearizability must PASS on all 10 — this is the property this whole harness
 * exists to prove, run across enough distinct schedules that a narrow one-off fluke can't hide.
 */
class RandomizedScheduleChaosTest {

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

  @ParameterizedTest
  @ValueSource(longs = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10})
  void linearizabilityHoldsAcrossRandomizedSchedules(long seed) throws Exception {
    Random random = new Random(seed);
    int producers = 2 + random.nextInt(7);
    int crashes = 1 + random.nextInt(5);
    long messages = 2_000 + random.nextInt(3_000);

    ChaosConfig config =
        new ChaosConfig(
            crashes,
            messages,
            producers,
            3,
            "chaos-seed-" + seed,
            3,
            seed,
            Math.max(1, messages / crashes),
            2_000,
            5_000,
            10 * 60 * 1000);

    FaultInjector faultInjector = new FaultInjector(cluster, ChaosTestSupport.COMPOSE_DIR);
    ChaosOrchestrator orchestrator = new ChaosOrchestrator(config, bootstrapClient, faultInjector);
    ChaosReport report = orchestrator.run();

    CheckResult linearizability = new LinearizabilityChecker(500_000).check(report.history());

    assertEquals(
        CheckResult.Status.PASS,
        linearizability.status(),
        "seed " + seed + ": " + linearizability.summary() + " " + linearizability.violations());
  }
}
