package io.minikafka.raft;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.stream.Collectors;

/**
 * Wires N {@link RaftNode}s together through one {@link MockTransport} with {@link
 * InMemoryRaftLogStore}s and {@link CountingStateMachine}s, for in-process cluster tests. Uses
 * small, fast timeouts so tests converge quickly without being flaky under load.
 */
final class TestRaftCluster implements AutoCloseable {

  static final RaftConfig FAST_CONFIG_TEMPLATE =
      new RaftConfig(300, 600, 50, 100, 300, Path.of("."), "test");

  private final MockTransport transport;
  private final Map<Integer, RaftNode> nodes = new ConcurrentHashMap<>();
  private final Map<Integer, RaftLogStore> logStores = new ConcurrentHashMap<>();
  private final Map<Integer, Path> stateDirs = new ConcurrentHashMap<>();
  private final Map<Integer, CountingStateMachine> stateMachines = new ConcurrentHashMap<>();
  private final Map<Long, Set<Integer>> leadersByTerm = new ConcurrentHashMap<>();
  private final List<Integer> allIds;

  TestRaftCluster(int nodeCount) {
    this(nodeCount, 42L);
  }

  TestRaftCluster(int nodeCount, long seed) {
    this.transport = new MockTransport(seed);
    List<Integer> ids = new ArrayList<>();
    for (int i = 1; i <= nodeCount; i++) {
      ids.add(i);
    }
    this.allIds = List.copyOf(ids);
    for (int id : ids) {
      startFresh(id);
    }
  }

  private void startFresh(int id) {
    try {
      Path dir = Files.createTempDirectory("raft-test-" + id + "-");
      stateDirs.put(id, dir);
      RaftLogStore logStore = new InMemoryRaftLogStore();
      logStores.put(id, logStore);
      CountingStateMachine stateMachine = new CountingStateMachine();
      stateMachines.put(id, stateMachine);
      launch(id, dir, logStore, stateMachine);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  private void launch(int id, Path dir, RaftLogStore logStore, CountingStateMachine stateMachine) {
    List<Integer> peers = peersOf(id);
    RaftConfig config =
        new RaftConfig(
            FAST_CONFIG_TEMPLATE.minElectionTimeoutMs(),
            FAST_CONFIG_TEMPLATE.maxElectionTimeoutMs(),
            FAST_CONFIG_TEMPLATE.heartbeatIntervalMs(),
            FAST_CONFIG_TEMPLATE.maxEntriesPerAppend(),
            FAST_CONFIG_TEMPLATE.rpcTimeoutMs(),
            dir,
            "group-" + id);
    PersistentState persistentState = PersistentState.load(dir);
    RaftNode node =
        new RaftNode(
            id,
            peers,
            config,
            logStore,
            persistentState,
            transport,
            stateMachine,
            System::nanoTime);
    node.onLeadershipChange(
        (term, nodeId) ->
            leadersByTerm.computeIfAbsent(term, t -> new CopyOnWriteArraySet<>()).add(nodeId));
    transport.register(id, node);
    nodes.put(id, node);
    node.start();
  }

  private List<Integer> peersOf(int id) {
    return allIds.stream().filter(other -> other != id).collect(Collectors.toList());
  }

  RaftNode node(int id) {
    return nodes.get(id);
  }

  List<RaftNode> nodes() {
    return List.copyOf(nodes.values());
  }

  CountingStateMachine stateMachineOf(int id) {
    return stateMachines.get(id);
  }

  Map<Long, Set<Integer>> leadersByTerm() {
    return Map.copyOf(leadersByTerm);
  }

  MockTransport transport() {
    return transport;
  }

  void partition(int a, int b) {
    transport.partition(a, b);
  }

  void heal(int a, int b) {
    transport.heal(a, b);
  }

  /** Polls until some node reports itself LEADER, or throws if none does within {@code timeout}. */
  RaftNode awaitLeader(Duration timeout) throws InterruptedException {
    long deadline = System.nanoTime() + timeout.toNanos();
    while (System.nanoTime() < deadline) {
      for (RaftNode node : nodes.values()) {
        if (node.role() == RaftRole.LEADER) {
          return node;
        }
      }
      Thread.sleep(10);
    }
    throw new AssertionError("no leader elected within " + timeout);
  }

  /**
   * Like {@link #awaitLeader}, but also confirms the same node is still leader in the same term
   * after a short settle window — guards against a caller observing a leader that steps down (due
   * to a concurrent split-vote election) moments later, before it can act on the reference.
   */
  RaftNode awaitStableLeader(Duration timeout) throws InterruptedException {
    long deadline = System.nanoTime() + timeout.toNanos();
    while (System.nanoTime() < deadline) {
      RaftNode candidate = awaitLeader(Duration.ofNanos(deadline - System.nanoTime()));
      long term = candidate.currentTerm();
      Thread.sleep(150);
      if (candidate.role() == RaftRole.LEADER && candidate.currentTerm() == term) {
        return candidate;
      }
    }
    throw new AssertionError("no stable leader within " + timeout);
  }

  /** Polls until one of {@code candidateIds} reports itself LEADER, or throws on timeout. */
  RaftNode awaitLeaderAmong(int[] candidateIds, Duration timeout) throws InterruptedException {
    long deadline = System.nanoTime() + timeout.toNanos();
    while (System.nanoTime() < deadline) {
      for (int id : candidateIds) {
        RaftNode node = nodes.get(id);
        if (node != null && node.role() == RaftRole.LEADER) {
          return node;
        }
      }
      Thread.sleep(10);
    }
    throw new AssertionError("no leader elected among " + java.util.Arrays.toString(candidateIds));
  }

  /**
   * Proposes {@code command} on the current leader, retrying against a freshly discovered leader if
   * the one found by {@link #awaitLeader} steps down before the proposal is accepted (e.g. due to a
   * concurrent split-vote term bump right after it won its own election).
   *
   * <p><b>Not idempotent.</b> Raw Raft has no dedup layer (that's Spec 09's idempotent producer); a
   * retry after a client-side timeout on an already-appended-but-not-yet-locally-confirmed proposal
   * can legitimately result in the command being durably committed twice. Callers that care about
   * exact counts should assert {@code >=}, not {@code ==}.
   */
  ApplyResult proposeOnLeader(byte[] command, Duration timeout)
      throws InterruptedException, java.util.concurrent.TimeoutException {
    long deadline = System.nanoTime() + timeout.toNanos();
    Exception last = null;
    while (System.nanoTime() < deadline) {
      RaftNode leader = awaitLeader(Duration.ofNanos(deadline - System.nanoTime()));
      try {
        return leader
            .propose(command)
            .get(
                Math.max(1, (deadline - System.nanoTime()) / 1_000_000),
                java.util.concurrent.TimeUnit.MILLISECONDS);
      } catch (java.util.concurrent.ExecutionException e) {
        if (e.getCause() instanceof NotLeaderException) {
          last = e;
          continue;
        }
        throw new RuntimeException(e);
      } catch (java.util.concurrent.TimeoutException e) {
        last = e;
      }
    }
    throw new AssertionError("propose never succeeded within " + timeout, last);
  }

  /** Polls until {@code node}'s commitIndex reaches at least {@code index}. */
  void awaitCommitIndex(RaftNode node, long index, Duration timeout) throws InterruptedException {
    long deadline = System.nanoTime() + timeout.toNanos();
    while (System.nanoTime() < deadline) {
      if (node.commitIndex() >= index) {
        return;
      }
      Thread.sleep(10);
    }
    throw new AssertionError(
        "node " + node.selfId() + " commitIndex did not reach " + index + " within " + timeout);
  }

  void crash(int id) {
    RaftNode node = nodes.remove(id);
    transport.unregister(id);
    if (node != null) {
      node.close();
    }
  }

  void restart(int id) {
    Path dir = stateDirs.get(id);
    RaftLogStore logStore = logStores.get(id);
    CountingStateMachine stateMachine = stateMachines.get(id);
    launch(id, dir, logStore, stateMachine);
  }

  @Override
  public void close() {
    for (RaftNode node : nodes.values()) {
      node.close();
    }
    for (Path dir : stateDirs.values()) {
      deleteRecursively(dir);
    }
  }

  private static void deleteRecursively(Path dir) {
    try (var stream = Files.walk(dir)) {
      stream.sorted(Comparator.reverseOrder()).forEach(TestRaftCluster::deleteQuietly);
    } catch (IOException ignored) {
      // best-effort cleanup
    }
  }

  private static void deleteQuietly(Path p) {
    try {
      Files.deleteIfExists(p);
    } catch (IOException ignored) {
      // best-effort cleanup
    }
  }
}
