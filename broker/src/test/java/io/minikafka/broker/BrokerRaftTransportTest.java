package io.minikafka.broker;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;

import io.minikafka.protocol.BrokerInfo;
import io.minikafka.protocol.ErrorResp;
import io.minikafka.protocol.Frame;
import io.minikafka.protocol.FrameDecoder;
import io.minikafka.protocol.FrameEncoder;
import io.minikafka.protocol.Message;
import io.minikafka.protocol.MessageCodec;
import io.minikafka.protocol.ProtocolConfig;
import io.minikafka.raft.AppendEntriesRequest;
import io.minikafka.raft.AppendEntriesResponse;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Function;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * Covers the term-recovery path this spec added to {@link BrokerRaftTransport#appendEntries}: a
 * peer's {@code ErrorResp(CODE_STALE_LEADER_EPOCH, "STALE_TERM:<term>: ...")} must be translated
 * into an {@link AppendEntriesResponse} carrying that term (not thrown as a bare {@code
 * RaftRpcException}), so a fenced leader's own {@code RaftNode.replicateTo} sees {@code resp.term()
 * > capturedTerm} and steps down through the normal Raft path — the exact regression the
 * /raft-review caught: discarding the term left a fenced leader believing it was still leader until
 * the new leader's own RPC happened to reach it.
 */
class BrokerRaftTransportTest {

  private static final int MAX = ProtocolConfig.DEFAULT_MAX_FRAME_BYTES;
  private final MessageCodec codec = MessageCodec.instance();

  private ServerSocket server;
  private ExecutorService serverPool;

  private int startServer(Function<Message, Message> responder) throws IOException {
    server = new ServerSocket(0);
    serverPool = Executors.newSingleThreadExecutor();
    serverPool.submit(
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
            // connection closed by the test
          }
          return null;
        });
    return server.getLocalPort();
  }

  @AfterEach
  void tearDown() {
    if (serverPool != null) {
      serverPool.shutdownNow();
    }
    if (server != null) {
      try {
        server.close();
      } catch (IOException ignored) {
        // best-effort
      }
    }
  }

  @Test
  @Timeout(10)
  void staleLeaderEpochErrorIsTranslatedIntoAHigherTermResponse() throws Exception {
    int port =
        startServer(
            req ->
                new ErrorResp(
                    req.correlationId(),
                    ErrorResp.CODE_STALE_LEADER_EPOCH,
                    "STALE_TERM:5: stale leader epoch 3 < current epoch 5 for orders-0"));

    try (BrokerRaftTransport transport = transportTo(port)) {
      AppendEntriesRequest req = new AppendEntriesRequest(3, 1, 0, 0, List.of(), 0);
      AppendEntriesResponse resp = transport.appendEntries(2, req).get();

      assertEquals(5, resp.term());
      assertFalse(resp.success());
    }
  }

  @Test
  @Timeout(10)
  void unparsableStaleEpochErrorStillThrows() throws Exception {
    int port =
        startServer(
            req ->
                new ErrorResp(
                    req.correlationId(), ErrorResp.CODE_STALE_LEADER_EPOCH, "malformed message"));

    try (BrokerRaftTransport transport = transportTo(port)) {
      AppendEntriesRequest req = new AppendEntriesRequest(3, 1, 0, 0, List.of(), 0);
      ExecutionException ex =
          assertThrows(ExecutionException.class, () -> transport.appendEntries(2, req).get());
      assertEquals(BrokerRaftTransport.RaftRpcException.class, ex.getCause().getClass());
    }
  }

  private BrokerRaftTransport transportTo(int port) {
    BrokerInfo peer = new BrokerInfo(2, "localhost", port);
    return new BrokerRaftTransport(
        new TopicPartition("orders", 0),
        Map.of(2, peer),
        2000,
        200,
        Executors.newVirtualThreadPerTaskExecutor());
  }
}
