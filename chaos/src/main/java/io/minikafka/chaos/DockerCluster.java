package io.minikafka.chaos;

import com.github.dockerjava.api.DockerClient;
import com.github.dockerjava.api.command.InspectContainerResponse;
import com.github.dockerjava.api.model.Container;
import java.io.File;
import java.time.Duration;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.containers.ComposeContainer;
import org.testcontainers.containers.ContainerState;

/**
 * Wraps the real 3-broker Docker Compose cluster ({@code docker/docker-compose.yml} + {@code
 * docker-compose.chaos.yml}) the same way {@code ClusterFormationIT} does, exposing the
 * broker-id-keyed operations {@link FaultInjector} needs.
 *
 * <p>Deliberately does not use {@code ComposeContainer.withExposedService} /
 * {@code getContainerByServiceName}: in Testcontainers 1.21.x against Docker Compose V2's
 * container-naming scheme, {@code withExposedService} routes through a shared ambassador
 * container that links to the target by name immediately after {@code up} returns and
 * intermittently fails with "Aborting attempt to link ... as it is not running" even though the
 * container is up, and without it {@code getContainerByServiceName}'s backing map is simply never
 * populated (no wait/exposed-service call means no service-instance tracking at all). Since our
 * compose file publishes fixed host ports, this class instead resolves containers directly
 * through the shared Testcontainers {@link DockerClient} by the {@code
 * com.docker.compose.service} label Compose itself attaches, which sidesteps needing to predict
 * the (Testcontainers-mangled) generated project name entirely.
 */
public final class DockerCluster implements AutoCloseable {

  private static final List<Integer> BROKER_IDS = List.of(1, 2, 3);
  private static final String HOST = "localhost";
  private static final String COMPOSE_SERVICE_LABEL = "com.docker.compose.service";
  private static final String COMPOSE_PROJECT_LABEL = "com.docker.compose.project";

  private final ComposeContainer environment;
  private final DockerClient dockerClient;
  private final Map<Integer, String> serviceNames = new ConcurrentHashMap<>();
  private final Map<Integer, Integer> hostPorts = new ConcurrentHashMap<>();

  private DockerCluster(ComposeContainer environment) {
    this.environment = environment;
    this.dockerClient = DockerClientFactory.lazyClient();
    for (int id : BROKER_IDS) {
      serviceNames.put(id, "broker-" + id);
      hostPorts.put(id, 9091 + id);
    }
  }

  public static DockerCluster start(File composeDir) {
    ComposeContainer environment =
        new ComposeContainer(
            new File(composeDir, "docker-compose.yml"), new File(composeDir, "docker-compose.chaos.yml"));
    environment.start();
    DockerCluster cluster = new DockerCluster(environment);
    for (int id : BROKER_IDS) {
      cluster.awaitJoinedCluster(id, Duration.ofSeconds(60));
    }
    return cluster;
  }

  public List<Integer> brokerIds() {
    return BROKER_IDS;
  }

  public String bootstrapHost() {
    return HOST;
  }

  public int bootstrapPort() {
    return hostPorts.get(BROKER_IDS.get(0));
  }

  public String hostFor(int brokerId) {
    return HOST;
  }

  public int hostPortFor(int brokerId) {
    return hostPorts.get(brokerId);
  }

  public String containerNameFor(int brokerId) {
    return serviceNameFor(brokerId);
  }

  /**
   * The Compose project name Testcontainers generated for the running cluster, read back off the
   * container's own {@code com.docker.compose.project} label. Needed by {@link FaultInjector} to
   * target the same project with a raw {@code docker compose} CLI call (e.g. {@code slowDisk}'s
   * {@code --force-recreate}) instead of falling back to the directory-derived default project,
   * which would operate on an unrelated (and likely nonexistent) compose stack.
   */
  public String composeProjectOf(int brokerId) {
    List<Container> matches =
        dockerClient
            .listContainersCmd()
            .withShowAll(true)
            .withLabelFilter(Map.of(COMPOSE_SERVICE_LABEL, serviceNameFor(brokerId)))
            .exec();
    return matches.stream()
        .max(Comparator.comparingLong(Container::getCreated))
        .map(c -> c.getLabels().get(COMPOSE_PROJECT_LABEL))
        .orElseThrow(
            () -> new IllegalStateException("broker-" + brokerId + " container not found"));
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

  private Optional<ContainerState> containerStateOf(int brokerId) {
    List<Container> matches =
        dockerClient
            .listContainersCmd()
            .withShowAll(true)
            .withLabelFilter(Map.of(COMPOSE_SERVICE_LABEL, serviceNameFor(brokerId)))
            .exec();
    return matches.stream()
        .max(Comparator.comparingLong(Container::getCreated))
        .map(c -> dockerClient.inspectContainerCmd(c.getId()).exec())
        .map(info -> new ResolvedContainerState(dockerClient, info));
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

  private void awaitJoinedCluster(int brokerId, Duration timeout) {
    long deadline = System.nanoTime() + timeout.toNanos();
    while (System.nanoTime() < deadline) {
      if (recentLogsContain(brokerId, "joined cluster")) {
        return;
      }
      try {
        Thread.sleep(200);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new IllegalStateException("interrupted while waiting for broker-" + brokerId, e);
      }
    }
    throw new IllegalStateException(
        "broker-" + brokerId + " did not log 'joined cluster' within " + timeout);
  }

  private boolean recentLogsContain(int brokerId, String needle) {
    return containerStateOf(brokerId).map(c -> c.getLogs().contains(needle)).orElse(false);
  }

  @Override
  public void close() {
    environment.stop();
  }

  /** Minimal {@link ContainerState} backed directly by a fresh {@code inspectContainerCmd} call. */
  private record ResolvedContainerState(DockerClient dockerClient, InspectContainerResponse info)
      implements ContainerState {

    @Override
    public List<Integer> getExposedPorts() {
      return List.of();
    }

    @Override
    public InspectContainerResponse getContainerInfo() {
      return info;
    }

    @Override
    public DockerClient getDockerClient() {
      return dockerClient;
    }
  }
}
