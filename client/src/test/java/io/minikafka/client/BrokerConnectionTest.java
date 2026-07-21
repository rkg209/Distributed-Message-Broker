package io.minikafka.client;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import io.minikafka.protocol.BrokerInfo;
import io.minikafka.protocol.Frame;
import io.minikafka.protocol.FrameDecoder;
import io.minikafka.protocol.FrameEncoder;
import io.minikafka.protocol.Message;
import io.minikafka.protocol.MessageCodec;
import io.minikafka.protocol.MetadataReq;
import io.minikafka.protocol.MetadataResp;
import io.minikafka.protocol.ProtocolConfig;
import io.minikafka.protocol.ProtocolException;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Function;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * Exercises {@link BrokerConnection} against a tiny in-test server built directly on the protocol
 * codec (the client module depends only on {@code protocol}, not on {@code broker}).
 */
class BrokerConnectionTest {

  private static final int MAX = ProtocolConfig.DEFAULT_MAX_FRAME_BYTES;
  private final MessageCodec codec = MessageCodec.instance();

  private ServerSocket server;
  private ExecutorService serverPool;

  /**
   * Starts a one-connection stub server that maps each request to a response via {@code responder}.
   */
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
  void tearDown() throws IOException {
    if (serverPool != null) {
      serverPool.shutdownNow();
    }
    if (server != null) {
      server.close();
    }
  }

  @Test
  @Timeout(10)
  void requestReturnsMatchingResponse() throws IOException {
    int port =
        startServer(
            req ->
                new MetadataResp(
                    req.correlationId(), List.of(new BrokerInfo(1, "localhost", 9092)), List.of()));

    try (BrokerConnection conn = new BrokerConnection("localhost", port, MAX)) {
      long id = conn.nextCorrelationId();
      Message response = conn.request(new MetadataReq(id));
      MetadataResp meta = assertInstanceOf(MetadataResp.class, response);
      assertEquals(id, meta.correlationId());
    }
  }

  @Test
  void correlationIdsAreUnique() throws IOException {
    int port = startServer(req -> new MetadataResp(req.correlationId(), List.of(), List.of()));
    try (BrokerConnection conn = new BrokerConnection("localhost", port, MAX)) {
      assertNotEquals(conn.nextCorrelationId(), conn.nextCorrelationId());
    }
  }

  @Test
  @Timeout(10)
  void mismatchedCorrelationIdThrows() throws IOException {
    // Server deliberately replies with the wrong correlation id.
    int port = startServer(req -> new MetadataResp(req.correlationId() + 1, List.of(), List.of()));

    try (BrokerConnection conn = new BrokerConnection("localhost", port, MAX)) {
      assertThrows(ProtocolException.class, () -> conn.request(new MetadataReq(1L)));
    }
  }
}
