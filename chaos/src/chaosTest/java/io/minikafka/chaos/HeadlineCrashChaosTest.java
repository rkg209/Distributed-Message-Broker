package io.minikafka.chaos;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.minikafka.chaos.check.CheckResult;
import io.minikafka.chaos.check.DivergenceChecker;
import io.minikafka.chaos.check.DuplicationChecker;
import io.minikafka.chaos.check.LinearizabilityChecker;
import io.minikafka.chaos.check.LossChecker;
import io.minikafka.chaos.check.ReplicaReader;
import io.minikafka.chaos.check.ResultsWriter;
import io.minikafka.client.ClusterClient;
import java.nio.file.Path;
import java.util.List;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * AC-1, the headline chaos claim: leader kills injected every {@code messages/crashes} acks;
 * asserts zero loss, zero duplication, no divergence across replicas, and a valid linearization —
 * then records the run in {@code docs/results.md}. Run as {@code ./gradlew chaosTest} (tiered
 * default) or {@code ./gradlew chaosTest -Pcrashes=1000 -Pmessages=10000000} for the real headline
 * number.
 */
class HeadlineCrashChaosTest {

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
  void zeroLossZeroDuplicationAcrossInjectedCrashes() throws Exception {
    ChaosConfig config = ChaosConfig.fromSystemProperties();
    FaultInjector faultInjector = new FaultInjector(cluster, ChaosTestSupport.COMPOSE_DIR);
    ChaosOrchestrator orchestrator = new ChaosOrchestrator(config, bootstrapClient, faultInjector);

    ChaosReport report = orchestrator.run();

    Thread.sleep(config.recoverySettleMs()); // let lagging replicas catch up before comparing
    List<ReplicaReader> replicas = ChaosTestSupport.replicaReaders(cluster);

    CheckResult loss = new LossChecker().check(report.history());
    CheckResult duplication = new DuplicationChecker().check(report.history());
    CheckResult linearizability = new LinearizabilityChecker(200_000).check(report.history());
    CheckResult divergence =
        new DivergenceChecker(replicas, config.topic(), config.partitions(), 1024 * 1024)
            .check(report.history());

    if (loss.status() != CheckResult.Status.PASS
        || duplication.status() != CheckResult.Status.PASS
        || divergence.status() != CheckResult.Status.PASS
        || linearizability.status() != CheckResult.Status.PASS) {
      HistoryRecorder.dump(report.history(), Path.of("build", "chaos-history-headline.csv"));
    }

    new ResultsWriter()
        .writeChaosRun(
            Path.of("../docs/results.md"), report, loss, duplication, linearizability, divergence);

    assertEquals(CheckResult.Status.PASS, loss.status(), loss.summary() + " " + loss.violations());
    assertEquals(
        CheckResult.Status.PASS,
        duplication.status(),
        duplication.summary() + " " + duplication.violations());
    assertEquals(
        CheckResult.Status.PASS,
        divergence.status(),
        divergence.summary() + " " + divergence.violations());
    assertEquals(
        CheckResult.Status.PASS,
        linearizability.status(),
        linearizability.summary() + " " + linearizability.violations());
    assertTrue(report.crashesInjected() > 0, "expected at least one crash to be injected");
  }
}
