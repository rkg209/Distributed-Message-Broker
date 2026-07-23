package io.minikafka.client;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import io.minikafka.protocol.BrokerInfo;
import io.minikafka.protocol.Frame;
import io.minikafka.protocol.FrameDecoder;
import io.minikafka.protocol.FrameEncoder;
import io.minikafka.protocol.Message;
import io.minikafka.protocol.MessageCodec;
import io.minikafka.protocol.MetadataResp;
import io.minikafka.protocol.PartitionMetadata;
import io.minikafka.protocol.ProtocolConfig;
import io.minikafka.protocol.ProtocolException;
import io.minikafka.protocol.TopicMetadata;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * Exercises {@link ClusterClient} leader resolution and {@link ClusterClient#refresh()} against
 * tiny in-test stub servers bound to pre-reserved ports (so a {@link BrokerInfo} list can reference
 * each server's port before it starts serving), mirroring {@link BrokerConnectionTest}'s style: the
 * client module depends only on {@code protocol}, not on {@code broker}.
 */
class ClusterClientTest {

  private static final int MAX = ProtocolConfig.DEFAULT_MAX_FRAME_BYTES;
  private final MessageCodec codec = MessageCodec.instance();

  private final List<ServerSocket> servers = new ArrayList<>();
  private final List<ExecutorService> pools = new ArrayList<>();

  private static int reservePort() throws IOException {
    try (ServerSocket s = new ServerSocket(0)) {
      return s.getLocalPort();
    }
  }

  /** Binds and serves on {@code port}, answering every request via {@code responder}. */
  private void startServerOn(int port, Function<Message, Message> responder) throws IOException {
    ServerSocket server = new ServerSocket(port);
    ExecutorService pool = Executors.newSingleThreadExecutor();
    servers.add(server);
    pools.add(pool);
    pool.submit(
        () -> {
          try (Socket socket = server.accept()) {
            FrameEncoder encoder = new FrameEncoder(socket.getOutputStream());
            FrameDecoder decoder = new FrameDecoder(socket.getInputStream(), MAX);
            Frame frame;
            while ((frame = decoder.read()) != null) {
              Message request = codec.decode(frame);
              encoder.write(codec.encode(responder.apply(request)));
            }
          } catch (IOException ignored) {
            // connection closed by the test, or server socket closed to simulate a crash
          }
          return null;
        });
  }

  @AfterEach
  void tearDown() {
    for (ExecutorService pool : pools) {
      pool.shutdownNow();
    }
    for (ServerSocket server : servers) {
      try {
        server.close();
      } catch (IOException ignored) {
        // best-effort
      }
    }
  }

  private static MetadataResp metadataWithLeader(
      long correlationId, List<BrokerInfo> brokers, int leaderId) {
    TopicMetadata topic =
        new TopicMetadata("orders", List.of(new PartitionMetadata(0, leaderId, List.of(1, 2))));
    return new MetadataResp(correlationId, brokers, List.of(topic));
  }

  @Test
  @Timeout(10)
  void leaderForResolvesFromBootstrapMetadata() throws IOException {
    int port1 = reservePort();
    BrokerInfo broker1 = new BrokerInfo(1, "localhost", port1);
    startServerOn(port1, req -> metadataWithLeader(req.correlationId(), List.of(broker1), 1));

    try (ClusterClient clusterClient = new ClusterClient("localhost", port1, MAX)) {
      assertEquals(1, clusterClient.leaderFor("orders", 0));
    }
  }

  @Test
  @Timeout(10)
  void refreshFindsNewLeaderViaSurvivingBroker() throws IOException {
    int port1 = reservePort();
    int port2 = reservePort();
    BrokerInfo broker1 = new BrokerInfo(1, "localhost", port1);
    BrokerInfo broker2 = new BrokerInfo(2, "localhost", port2);
    List<BrokerInfo> brokers = List.of(broker1, broker2);

    AtomicInteger currentLeader = new AtomicInteger(1);
    startServerOn(port1, req -> metadataWithLeader(req.correlationId(), brokers, 1));
    startServerOn(
        port2, req -> metadataWithLeader(req.correlationId(), brokers, currentLeader.get()));

    try (ClusterClient clusterClient = new ClusterClient("localhost", port1, MAX)) {
      assertEquals(1, clusterClient.leaderFor("orders", 0));

      // Simulate a failover: broker 1 dies, broker 2 (still reachable) now reports leader=2.
      servers.get(0).close();
      currentLeader.set(2);
      clusterClient.refresh();
      assertEquals(2, clusterClient.leaderFor("orders", 0));
    }
  }

  @Test
  @Timeout(10)
  void refreshThrowsWhenNoBrokerReachable() throws IOException {
    int port1 = reservePort();
    BrokerInfo broker1 = new BrokerInfo(1, "localhost", port1);
    startServerOn(port1, req -> metadataWithLeader(req.correlationId(), List.of(broker1), 1));

    ClusterClient clusterClient = new ClusterClient("localhost", port1, MAX);
    tearDown();
    assertThrows(ProtocolException.class, clusterClient::refresh);
  }
}
