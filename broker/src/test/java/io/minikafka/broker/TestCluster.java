package io.minikafka.broker;

import io.minikafka.log.DiskPartitionLog;
import io.minikafka.log.LogConfig;
import io.minikafka.log.PartitionLog;
import io.minikafka.protocol.BrokerInfo;
import io.minikafka.protocol.ProtocolConfig;
import java.io.IOException;
import java.net.ServerSocket;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * In-process cluster of N brokers, each a real {@link ConnectionAcceptor} on an ephemeral port,
 * wired with a {@link ClusterConfig} built from their actual bound ports and (from Spec 07) a real
 * {@link PartitionManager} backed by Raft. Used by cluster-formation, failure-detection, and
 * replication tests that would otherwise require Docker.
 */
final class TestCluster implements AutoCloseable {

  static final long DEFAULT_RAFT_ELECTION_MIN_MS = 400;
  static final long DEFAULT_RAFT_ELECTION_MAX_MS = 800;
  static final long DEFAULT_RAFT_HEARTBEAT_MS = 50;
  static final long DEFAULT_RAFT_RPC_TIMEOUT_MS = 300;
  static final long DEFAULT_RAFT_PROPOSE_TIMEOUT_MS = 5000;
  static final long DEFAULT_RAFT_LEADER_WAIT_MS = 3000;

  record BrokerNode(
      BrokerInfo info,
      ConnectionAcceptor acceptor,
      MetadataService metadataService,
      HeartbeatMonitor heartbeatMonitor,
      PartitionManager partitionManager,
      ConsumerGroupManager consumerGroupManager) {}

  private final List<BrokerNode> nodes;
  private final java.util.Set<Integer> killed = java.util.concurrent.ConcurrentHashMap.newKeySet();

  private TestCluster(List<BrokerNode> nodes) {
    this.nodes = nodes;
  }

  static TestCluster start(
      int brokerCount,
      String partitionAssignments,
      int replicationFactor,
      TopicConfig topicConfig,
      long heartbeatIntervalMs,
      long heartbeatTimeoutMs,
      long peerReconnectBackoffMs,
      Path tempDir)
      throws IOException {
    List<BrokerInfo> brokers = reservePorts(brokerCount);
    String brokerListSpec =
        brokers.stream()
            .map(b -> b.brokerId() + "@" + b.host() + ":" + b.port())
            .collect(Collectors.joining(","));

    List<BrokerNode> nodes = new ArrayList<>(brokerCount);
    for (BrokerInfo self : brokers) {
      ClusterConfig clusterConfig =
          ClusterConfig.parse(
              brokerListSpec,
              partitionAssignments,
              replicationFactor,
              null,
              self.brokerId(),
              topicConfig);
      MetadataService metadataService = new MetadataService(self, topicConfig, clusterConfig);
      Path brokerDir = tempDir.resolve("broker-" + self.brokerId());
      BrokerConfig brokerConfig =
          testBrokerConfig(
              self,
              brokerDir,
              clusterConfig,
              topicConfig,
              heartbeatIntervalMs,
              heartbeatTimeoutMs,
              peerReconnectBackoffMs);
      PartitionManager partitionManager =
          new PartitionManager(
              brokerConfig,
              metadataService,
              clusterConfig,
              tp -> new DiskPartitionLog(brokerConfig.logConfigFor(tp)));
      metadataService.attachPartitionManager(partitionManager);
      ConsumerGroupManager consumerGroupManager =
          new ConsumerGroupManager(brokerDir.resolve("offsets"));
      BrokerRequestHandler handler =
          new BrokerRequestHandler(
              metadataService, partitionManager, consumerGroupManager, 1024 * 1024);
      ConnectionAcceptor acceptor =
          new ConnectionAcceptor(self.port(), ProtocolConfig.DEFAULT_MAX_FRAME_BYTES, handler);
      acceptor.start();
      HeartbeatMonitor heartbeatMonitor =
          new HeartbeatMonitor(
              self,
              clusterConfig.peersOf(self.brokerId()),
              heartbeatIntervalMs,
              heartbeatTimeoutMs,
              peerReconnectBackoffMs);
      heartbeatMonitor.start();
      partitionManager.start();
      nodes.add(
          new BrokerNode(
              self,
              acceptor,
              metadataService,
              heartbeatMonitor,
              partitionManager,
              consumerGroupManager));
    }
    return new TestCluster(nodes);
  }

  private static BrokerConfig testBrokerConfig(
      BrokerInfo self,
      Path brokerDir,
      ClusterConfig clusterConfig,
      TopicConfig topicConfig,
      long heartbeatIntervalMs,
      long heartbeatTimeoutMs,
      long peerReconnectBackoffMs) {
    return new BrokerConfig(
        self.brokerId(),
        self.host(),
        self.port(),
        ProtocolConfig.DEFAULT_MAX_FRAME_BYTES,
        1024 * 1024,
        brokerDir.toString(),
        LogConfig.DEFAULT_FSYNC_POLICY,
        LogConfig.DEFAULT_FSYNC_INTERVAL_MS,
        LogConfig.DEFAULT_MAX_SEGMENT_BYTES,
        LogConfig.DEFAULT_INDEX_INTERVAL_BYTES,
        LogConfig.UNLIMITED,
        LogConfig.UNLIMITED,
        topicConfig,
        brokerDir.resolve("__offsets").toString(),
        clusterConfig,
        heartbeatIntervalMs,
        heartbeatTimeoutMs,
        peerReconnectBackoffMs,
        DEFAULT_RAFT_ELECTION_MIN_MS,
        DEFAULT_RAFT_ELECTION_MAX_MS,
        DEFAULT_RAFT_HEARTBEAT_MS,
        DEFAULT_RAFT_RPC_TIMEOUT_MS,
        io.minikafka.raft.RaftConfig.DEFAULT_MAX_ENTRIES_PER_APPEND,
        DEFAULT_RAFT_PROPOSE_TIMEOUT_MS,
        DEFAULT_RAFT_LEADER_WAIT_MS);
  }

  private static List<BrokerInfo> reservePorts(int brokerCount) throws IOException {
    List<BrokerInfo> brokers = new ArrayList<>(brokerCount);
    for (int i = 1; i <= brokerCount; i++) {
      int port;
      try (ServerSocket reserved = new ServerSocket(0)) {
        port = reserved.getLocalPort();
      }
      brokers.add(new BrokerInfo(i, "localhost", port));
    }
    return brokers;
  }

  BrokerNode node(int brokerId) {
    return nodes.stream()
        .filter(n -> n.info().brokerId() == brokerId)
        .findFirst()
        .orElseThrow(() -> new IllegalArgumentException("No such broker: " + brokerId));
  }

  List<BrokerNode> nodes() {
    return nodes;
  }

  /**
   * The durable state-machine log for {@code tp} on {@code brokerId}, or {@code null} if unhosted.
   */
  PartitionLog partitionLogOf(int brokerId, TopicPartition tp) {
    PartitionReplica replica = node(brokerId).partitionManager().replica(tp);
    return replica == null ? null : replica.partitionLog();
  }

  /** The broker id this cluster believes is the Raft leader for {@code tp}, or -1 if unknown. */
  int leaderOf(TopicPartition tp) {
    for (BrokerNode node : nodes) {
      PartitionReplica replica = node.partitionManager().replica(tp);
      if (replica != null && replica.isLeader()) {
        return node.info().brokerId();
      }
    }
    return -1;
  }

  /**
   * Stops just this broker's acceptor (simulating a crash) without closing its heartbeat monitor.
   */
  void stopAcceptor(int brokerId) {
    node(brokerId).acceptor().close();
  }

  /**
   * Fully kills a broker: acceptor, heartbeat sender, and every {@link PartitionReplica}'s {@code
   * RaftNode} (election timer included). Unlike {@link #stopAcceptor}, this also silences the
   * broker's *outbound* Raft traffic — a broker whose acceptor alone is closed can still open
   * outbound connections and keep sending ever-higher-term RequestVotes (no PreVote phase is
   * implemented), which would destabilize an otherwise-healthy leader even though the "killed"
   * broker can never itself win an election.
   */
  void killBroker(int brokerId) {
    BrokerNode node = node(brokerId);
    node.heartbeatMonitor().close();
    node.acceptor().close();
    node.partitionManager().close();
    killed.add(brokerId);
  }

  /** Restarts a previously stopped broker's acceptor on the same port. */
  void restartAcceptor(int brokerId) throws IOException {
    BrokerNode node = node(brokerId);
    ConnectionAcceptor acceptor =
        new ConnectionAcceptor(
            node.info().port(),
            ProtocolConfig.DEFAULT_MAX_FRAME_BYTES,
            new BrokerRequestHandler(
                node.metadataService(),
                node.partitionManager(),
                node.consumerGroupManager(),
                1024 * 1024));
    acceptor.start();
    int index = nodes.indexOf(node);
    nodes.set(
        index,
        new BrokerNode(
            node.info(),
            acceptor,
            node.metadataService(),
            node.heartbeatMonitor(),
            node.partitionManager(),
            node.consumerGroupManager()));
  }

  @Override
  public void close() {
    for (BrokerNode node : nodes) {
      if (killed.contains(node.info().brokerId())) {
        node.consumerGroupManager().close();
        continue;
      }
      node.heartbeatMonitor().close();
      node.acceptor().close();
      node.partitionManager().close();
      node.consumerGroupManager().close();
    }
  }
}
