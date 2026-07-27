package io.minikafka.chaos;

import java.io.File;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.testcontainers.containers.ComposeContainer;
import org.testcontainers.containers.ContainerState;
import org.testcontainers.containers.wait.strategy.Wait;

/**
 * Wraps the real 3-broker Docker Compose cluster ({@code docker/docker-compose.yml} + {@code
 * docker-compose.chaos.yml}) the same way {@code ClusterFormationIT} does, exposing the
 * broker-id-keyed operations {@link FaultInjector} needs.
 */
public final class DockerCluster implements AutoCloseable {

  private static final List<Integer> BROKER_IDS = List.of(1, 2, 3);

  private final ComposeContainer environment;
  private final Map<Integer, String> serviceNames = new ConcurrentHashMap<>();

  private DockerCluster(ComposeContainer environment) {
    this.environment = environment;
    for (int id : BROKER_IDS) {
      serviceNames.put(id, "broker-" + id);
    }
  }

  public static DockerCluster start(File composeDir) {
    ComposeContainer environment =
        new ComposeContainer(
                new File(composeDir, "docker-compose.yml"),
                new File(composeDir, "docker-compose.chaos.yml"))
            .withExposedService("broker-1", 9092, Wait.forLogMessage(".*joined cluster.*\\n", 1))
            .withExposedService("broker-2", 9092, Wait.forLogMessage(".*joined cluster.*\\n", 1))
            .withExposedService("broker-3", 9092, Wait.forLogMessage(".*joined cluster.*\\n", 1))
            .withLocalCompose(true);
    environment.start();
    return new DockerCluster(environment);
  }

  public List<Integer> brokerIds() {
    return BROKER_IDS;
  }

  public String bootstrapHost() {
    return environment.getServiceHost(serviceNameFor(BROKER_IDS.get(0)), 9092);
  }

  public int bootstrapPort() {
    return environment.getServicePort(serviceNameFor(BROKER_IDS.get(0)), 9092);
  }

  public String hostFor(int brokerId) {
    return environment.getServiceHost(serviceNameFor(brokerId), 9092);
  }

  public int hostPortFor(int brokerId) {
    return environment.getServicePort(serviceNameFor(brokerId), 9092);
  }

  public String containerNameFor(int brokerId) {
    return serviceNameFor(brokerId);
  }

  public String ipOf(int brokerId) {
    return containerStateOf(brokerId)
        .map(
            c ->
                c.getContainerInfo().getNetworkSettings().getNetworks().values().stream()
                    .findFirst()
                    .map(n -> n.getIpAddress())
                    .orElseThrow(
                        () -> new IllegalStateException("broker-" + brokerId + " has no network")))
        .orElseThrow(
            () -> new IllegalStateException("broker-" + brokerId + " container not found"));
  }

  public ContainerState containerState(int brokerId) {
    return containerStateOf(brokerId)
        .orElseThrow(
            () -> new IllegalStateException("broker-" + brokerId + " container not found"));
  }

  private java.util.Optional<ContainerState> containerStateOf(int brokerId) {
    return environment.getContainerByServiceName(serviceNameFor(brokerId));
  }

  public void awaitHealthy(int brokerId, Duration timeout) throws InterruptedException {
    long deadline = System.nanoTime() + timeout.toNanos();
    while (System.nanoTime() < deadline) {
      var container = containerStateOf(brokerId);
      if (container.isPresent() && container.get().isRunning()) {
        return;
      }
      Thread.sleep(200);
    }
    throw new IllegalStateException(
        "broker-" + brokerId + " did not become healthy within " + timeout);
  }

  private String serviceNameFor(int brokerId) {
    return serviceNames.get(brokerId);
  }

  @Override
  public void close() {
    environment.stop();
  }
}
