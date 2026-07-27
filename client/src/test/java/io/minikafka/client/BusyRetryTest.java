package io.minikafka.client;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.minikafka.protocol.ErrorResp;
import io.minikafka.protocol.Frame;
import io.minikafka.protocol.FrameDecoder;
import io.minikafka.protocol.FrameEncoder;
import io.minikafka.protocol.Message;
import io.minikafka.protocol.MessageCodec;
import io.minikafka.protocol.ProtocolConfig;
import io.minikafka.protocol.PublishReq;
import io.minikafka.protocol.PublishResp;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * Covers {@link BusyRetry}'s backoff/exhaustion behavior directly, and {@link ProducerClient}'s
 * wiring of it end-to-end against a stub broker (the client module has no dependency on {@code
 * broker}, so a tiny protocol-level stub server stands in — see {@code BrokerConnectionTest} for
 * the same pattern).
 */
class BusyRetryTest {

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
  void succeedsImmediatelyWithoutRetryingWhenNotBusy() throws IOException {
    AtomicInteger calls = new AtomicInteger();
    BusyRetry retry = new BusyRetry(5, 10, 100);

    Message response =
        retry.send(
            () -> {
              calls.incrementAndGet();
              return new PublishResp(1L, 42L);
            });

    assertEquals(1, calls.get());
    assertEquals(new PublishResp(1L, 42L), response);
  }

  @Test
  @Timeout(10)
  void retriesOnBusyThenReturnsTheEventualNonBusyResponse() throws IOException {
    AtomicInteger calls = new AtomicInteger();
    BusyRetry retry = new BusyRetry(5, 1, 5);

    Message response =
        retry.send(
            () -> {
              int attempt = calls.getAndIncrement();
              if (attempt < 3) {
                return new ErrorResp(1L, ErrorResp.CODE_BROKER_BUSY, "busy");
              }
              return new PublishResp(1L, 99L);
            });

    assertEquals(4, calls.get());
    assertEquals(new PublishResp(1L, 99L), response);
  }

  @Test
  @Timeout(10)
  void returnsTheFinalBusyResponseOnceRetriesAreExhausted() throws IOException {
    AtomicInteger calls = new AtomicInteger();
    BusyRetry retry = new BusyRetry(3, 1, 5);

    Message response =
        retry.send(
            () -> {
              calls.incrementAndGet();
              return new ErrorResp(1L, ErrorResp.CODE_BROKER_BUSY, "still busy");
            });

    assertEquals(4, calls.get(), "one initial attempt plus 3 retries");
    assertEquals(ErrorResp.CODE_BROKER_BUSY, ((ErrorResp) response).errorCode());
  }

  @Test
  @Timeout(10)
  void backoffIsCappedRatherThanGrowingUnboundedly() throws IOException {
    // A large base with many retries would take hours uncapped; with a small cap the whole
    // exhaustion sequence must finish quickly.
    BusyRetry retry = new BusyRetry(8, 10_000, 20);

    long start = System.nanoTime();
    retry.send(() -> new ErrorResp(1L, ErrorResp.CODE_BROKER_BUSY, "busy"));
    long elapsedMs = (System.nanoTime() - start) / 1_000_000;

    assertTrue(
        elapsedMs < 2000,
        "backoff should have been capped at ~20ms per attempt, took " + elapsedMs + "ms");
  }

  @Test
  @Timeout(10)
  void producerClientBusyRetryReusesTheSameProducerIdAndSeqAcrossAttempts() throws IOException {
    List<PublishReq> received = new CopyOnWriteArrayList<>();
    AtomicInteger attempt = new AtomicInteger();
    int port =
        startServer(
            req -> {
              PublishReq publishReq = (PublishReq) req;
              received.add(publishReq);
              if (attempt.getAndIncrement() < 2) {
                return new ErrorResp(req.correlationId(), ErrorResp.CODE_BROKER_BUSY, "busy");
              }
              return new PublishResp(req.correlationId(), 7L);
            });

    try (BrokerConnection conn = new BrokerConnection("localhost", port, MAX)) {
      ProducerClient producer =
          new ProducerClient(conn, new MetadataClient(conn), new PartitionRouter());
      long offset = producer.publish("orders", 0, "hello".getBytes());
      assertEquals(7L, offset);
    }

    assertEquals(3, received.size());
    long producerId = received.get(0).producerId();
    long seqNo = received.get(0).seqNo();
    for (PublishReq req : received) {
      assertEquals(producerId, req.producerId());
      assertEquals(seqNo, req.seqNo());
    }
  }

  @Test
  @Timeout(10)
  void exhaustedBusyRetryThrowsBrokerBusyException() throws IOException {
    int port =
        startServer(req -> new ErrorResp(req.correlationId(), ErrorResp.CODE_BROKER_BUSY, "busy"));

    try (BrokerConnection conn = new BrokerConnection("localhost", port, MAX)) {
      ProducerClient producer =
          new ProducerClient(conn, new MetadataClient(conn), new PartitionRouter(), 2, 1, 5);
      assertThrows(BrokerBusyException.class, () -> producer.publish("orders", 0, "hi".getBytes()));
    }
  }
}
