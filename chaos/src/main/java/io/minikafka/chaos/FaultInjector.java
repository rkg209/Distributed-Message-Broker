package io.minikafka.chaos;

import com.github.dockerjava.api.DockerClient;
import io.minikafka.client.ClusterClient;
import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.Deque;

/**
 * The four fault operations the chaos harness injects, all implemented against real Docker
 * containers via {@link DockerCluster} (docker-java under the hood, same client Testcontainers
 * uses).
 *
 * <p>{@code slowDisk} is the honest substitute for the spec wording "use {@code tc} to add latency
 * to the disk I/O path": {@code tc} shapes network queues, not disk I/O, so instead this recreates
 * the target broker's container with {@code BROKER_FSYNC_DELAY_MS} set, which sleeps for that long
 * immediately before every {@code fsync} at the single chokepoint, {@code LogSegment.force()}.
 */
public final class FaultInjector {

  private final DockerCluster cluster;
  private final File composeDir;
  private final Deque<Runnable> teardown = new ArrayDeque<>();

  public FaultInjector(DockerCluster cluster, File composeDir) {
    this.cluster = cluster;
    this.composeDir = composeDir;
  }

  /** Kills the current leader of {@code topic}/{@code partition} with SIGKILL. Returns its id. */
  public int killLeader(ClusterClient clusterClient, String topic, int partition)
      throws IOException {
    clusterClient.refresh();
    int leaderId = clusterClient.leaderFor(topic, partition);
    DockerClient dockerClient = cluster.containerState(leaderId).getDockerClient();
    String containerId = cluster.containerState(leaderId).getContainerId();
    dockerClient.killContainerCmd(containerId).withSignal("KILL").exec();
    return leaderId;
  }

  /** Restarts a previously killed broker and waits for it to come back up. */
  public void restart(int brokerId, Duration timeout) throws InterruptedException {
    DockerClient dockerClient = cluster.containerState(brokerId).getDockerClient();
    String containerId = cluster.containerState(brokerId).getContainerId();
    dockerClient.startContainerCmd(containerId).exec();
    cluster.awaitHealthy(brokerId, timeout);
  }

  /** Drops traffic between two brokers in both directions. Idempotent; registers auto-heal. */
  public void partitionNetwork(int brokerA, int brokerB) throws IOException, InterruptedException {
    String ipA = cluster.ipOf(brokerA);
    String ipB = cluster.ipOf(brokerB);
    exec(brokerA, "iptables", "-A", "INPUT", "-s", ipB, "-j", "DROP");
    exec(brokerB, "iptables", "-A", "INPUT", "-s", ipA, "-j", "DROP");
    teardown.push(
        () -> {
          try {
            healNetwork(brokerA, brokerB);
          } catch (IOException | InterruptedException e) {
            throw new IllegalStateException("failed to auto-heal partition on teardown", e);
          }
        });
  }

  /** Removes the DROP rules installed by {@link #partitionNetwork}. Idempotent. */
  public void healNetwork(int brokerA, int brokerB) throws IOException, InterruptedException {
    String ipA = cluster.ipOf(brokerA);
    String ipB = cluster.ipOf(brokerB);
    execIgnoringFailure(brokerA, "iptables", "-D", "INPUT", "-s", ipB, "-j", "DROP");
    execIgnoringFailure(brokerB, "iptables", "-D", "INPUT", "-s", ipA, "-j", "DROP");
  }

  /** Recreates {@code brokerId}'s container with a real fsync delay of {@code latencyMs}. */
  public void slowDisk(int brokerId, long latencyMs) throws IOException, InterruptedException {
    Path override = Files.createTempFile("chaos-disk-override-", ".yml");
    teardown.push(() -> deleteQuietly(override));
    String service = cluster.containerNameFor(brokerId);
    String yaml =
        "services:\n  "
            + service
            + ":\n    environment:\n      BROKER_FSYNC_DELAY_MS: \""
            + latencyMs
            + "\"\n";
    Files.writeString(override, yaml, StandardCharsets.UTF_8);
    runCompose(
        "-f",
        "docker-compose.yml",
        "-f",
        "docker-compose.chaos.yml",
        "-f",
        override.toAbsolutePath().toString(),
        "up",
        "-d",
        "--no-deps",
        "--force-recreate",
        service);
    cluster.awaitHealthy(brokerId, Duration.ofSeconds(30));
  }

  /** Runs every registered heal action (partition heals, temp file cleanup). Never leaks rules. */
  public void healAll() {
    while (!teardown.isEmpty()) {
      teardown.pop().run();
    }
  }

  private void exec(int brokerId, String... command) throws IOException, InterruptedException {
    var result = cluster.containerState(brokerId).execInContainer(command);
    if (result.getExitCode() != 0) {
      throw new IOException(
          "command "
              + String.join(" ", command)
              + " on broker-"
              + brokerId
              + " failed: "
              + result.getStderr());
    }
  }

  private void execIgnoringFailure(int brokerId, String... command)
      throws IOException, InterruptedException {
    cluster.containerState(brokerId).execInContainer(command);
  }

  private void runCompose(String... args) throws IOException, InterruptedException {
    String[] full = new String[args.length + 2];
    full[0] = "docker";
    full[1] = "compose";
    System.arraycopy(args, 0, full, 2, args.length);
    Process process = new ProcessBuilder(full).directory(composeDir).inheritIO().start();
    int exitCode = process.waitFor();
    if (exitCode != 0) {
      throw new IOException("docker compose " + String.join(" ", args) + " exited " + exitCode);
    }
  }

  private static void deleteQuietly(Path path) {
    try {
      Files.deleteIfExists(path);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }
}
