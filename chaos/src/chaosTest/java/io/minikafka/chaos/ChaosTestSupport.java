package io.minikafka.chaos;

import io.minikafka.chaos.check.ReplicaReader;
import io.minikafka.client.BrokerConnection;
import io.minikafka.client.ClusterClient;
import io.minikafka.protocol.ProtocolConfig;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/** Shared Docker cluster / client wiring for the chaosTest suite. */
final class ChaosTestSupport {

  static final File COMPOSE_DIR = new File("../docker");

  private ChaosTestSupport() {}

  static ClusterClient bootstrapClient(DockerCluster cluster) throws IOException {
    return new ClusterClient(
        cluster.bootstrapHost(), cluster.bootstrapPort(), ProtocolConfig.DEFAULT_MAX_FRAME_BYTES);
  }

  static List<ReplicaReader> replicaReaders(DockerCluster cluster) throws IOException {
    List<ReplicaReader> readers = new ArrayList<>();
    for (int brokerId : cluster.brokerIds()) {
      BrokerConnection connection =
          new BrokerConnection(
              cluster.hostFor(brokerId),
              cluster.hostPortFor(brokerId),
              ProtocolConfig.DEFAULT_MAX_FRAME_BYTES);
      readers.add(new LiveReplicaReader(brokerId, connection));
    }
    return readers;
  }
}
