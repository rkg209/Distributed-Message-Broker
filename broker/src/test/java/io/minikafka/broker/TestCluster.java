package io.minikafka.broker;

import io.minikafka.log.DiskPartitionLog;
import io.minikafka.log.LogConfig;
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
 * wired with a {@link ClusterConfig} built from their actual bound ports. Used by cluster-formation
 * and failure-detection tests that would otherwise require Docker.
 */
final class TestCluster implements AutoCloseable {

  record BrokerNode(
      BrokerInfo info,
      ConnectionAcceptor acceptor,
      MetadataService metadataService,
      HeartbeatMonitor heartbeatMonitor,
      TopicRegistry topicRegistry,
      ConsumerGroupManager consumerGroupManager) {}

  private final List<BrokerNode> nodes;

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
      TopicRegistry topicRegistry =
          new TopicRegistry(
              tp ->
                  new DiskPartitionLog(
                      LogConfig.defaultsFor(brokerDir.resolve(tp.topic() + "-" + tp.partition()))));
      PartitionManager partitionManager = new PartitionManager(topicRegistry, metadataService);
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
      nodes.add(
          new BrokerNode(
              self,
              acceptor,
              metadataService,
              heartbeatMonitor,
              topicRegistry,
              consumerGroupManager));
    }
    return new TestCluster(nodes);
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
   * Stops just this broker's acceptor (simulating a crash) without closing its heartbeat monitor.
   */
  void stopAcceptor(int brokerId) {
    node(brokerId).acceptor().close();
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
                new PartitionManager(node.topicRegistry(), node.metadataService()),
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
            node.topicRegistry(),
            node.consumerGroupManager()));
  }

  @Override
  public void close() {
    for (BrokerNode node : nodes) {
      node.heartbeatMonitor().close();
      node.acceptor().close();
      node.topicRegistry().close();
      node.consumerGroupManager().close();
    }
  }
}
