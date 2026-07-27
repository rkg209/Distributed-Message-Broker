package io.minikafka.chaos;

import static org.junit.jupiter.api.Assertions.assertEquals;

import io.minikafka.chaos.check.CheckResult;
import io.minikafka.chaos.check.DivergenceChecker;
import io.minikafka.chaos.check.LossChecker;
import io.minikafka.chaos.check.ReplicaReader;
import io.minikafka.chaos.check.ResultsWriter;
import io.minikafka.client.ClusterClient;
import java.nio.file.Path;
import java.util.List;
import java.util.Random;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * AC-2: injects random broker-pair network partitions, heals each after a random 5-10s window, and
 * asserts no split-brain (divergence PASS) and no loss. Skipped rather than reported as a pass if
 * the Docker host can't grant {@code NET_ADMIN}/{@code iptables} inside the container (common on
 * some Docker Desktop VM kernels).
 */
class NetworkPartitionChaosTest {

  private static DockerCluster cluster;
  private static ClusterClient bootstrapClient;
  private static FaultInjector faultInjector;

  @BeforeAll
  static void startCluster() throws Exception {
    cluster = DockerCluster.start(ChaosTestSupport.COMPOSE_DIR);
    bootstrapClient = ChaosTestSupport.bootstrapClient(cluster);
    faultInjector = new FaultInjector(cluster, ChaosTestSupport.COMPOSE_DIR);

    try {
      faultInjector.partitionNetwork(cluster.brokerIds().get(0), cluster.brokerIds().get(1));
      faultInjector.healNetwork(cluster.brokerIds().get(0), cluster.brokerIds().get(1));
    } catch (Exception e) {
      Assumptions.abort(
          "iptables/NET_ADMIN unavailable in this Docker environment: " + e.getMessage());
    }
  }

  @AfterAll
  static void stopCluster() {
    if (faultInjector != null) {
      faultInjector.healAll();
    }
    if (cluster != null) {
      cluster.close();
    }
  }

  @Test
  void noSplitBrainAndNoLossAcrossRandomPartitions() throws Exception {
    ChaosConfig config = ChaosConfig.fromSystemProperties();
    Random random = new Random(config.seed());
    List<Integer> brokers = cluster.brokerIds();

    int partitionsInjected = Math.max(1, config.crashes() / 4);
    for (int i = 0; i < partitionsInjected; i++) {
      int a = brokers.get(random.nextInt(brokers.size()));
      int b = brokers.get(random.nextInt(brokers.size()));
      if (a == b) {
        continue;
      }
      faultInjector.partitionNetwork(a, b);
      Thread.sleep(config.randomPartitionHealMs(random));
      faultInjector.healNetwork(a, b);
      bootstrapClient.refresh();
    }

    ChaosOrchestrator orchestrator =
        new ChaosOrchestrator(
            config, bootstrapClient, new FaultInjector(cluster, ChaosTestSupport.COMPOSE_DIR));
    ChaosReport report = orchestrator.run();

    Thread.sleep(config.recoverySettleMs());
    List<ReplicaReader> replicas = ChaosTestSupport.replicaReaders(cluster);

    CheckResult loss = new LossChecker().check(report.history());
    CheckResult divergence =
        new DivergenceChecker(replicas, config.topic(), config.partitions(), 1024 * 1024)
            .check(report.history());

    boolean splitBrain = divergence.status() != CheckResult.Status.PASS;
    new ResultsWriter()
        .writeNetworkPartitionRun(
            Path.of("../docs/results.md"),
            new ChaosReport(
                config,
                0,
                partitionsInjected,
                report.messagesSent(),
                report.messagesAcked(),
                report.messagesReceived(),
                report.durationMs(),
                report.history()),
            splitBrain,
            divergence.status() == CheckResult.Status.PASS
                    && loss.status() == CheckResult.Status.PASS
                ? CheckResult.Status.PASS
                : CheckResult.Status.FAIL);

    assertEquals(
        CheckResult.Status.PASS,
        divergence.status(),
        divergence.summary() + " " + divergence.violations());
    assertEquals(CheckResult.Status.PASS, loss.status(), loss.summary() + " " + loss.violations());
  }
}
