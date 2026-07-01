package io.minikafka.broker;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.minikafka.protocol.BrokerInfo;
import io.minikafka.protocol.FrameDecoder;
import io.minikafka.protocol.FrameEncoder;
import io.minikafka.protocol.Message;
import io.minikafka.protocol.MessageCodec;
import io.minikafka.protocol.MetadataReq;
import io.minikafka.protocol.MetadataResp;
import io.minikafka.protocol.ProtocolConfig;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/** AC-6: the server handles 100 concurrent virtual-thread connections without error. */
class ConcurrencyTest {

  private static final int MAX = ProtocolConfig.DEFAULT_MAX_FRAME_BYTES;
  private static final int CLIENTS = 100;

  @Test
  @Timeout(30)
  void handlesHundredConcurrentConnections() throws Exception {
    ConnectionAcceptor acceptor =
        new ConnectionAcceptor(
            0, MAX, new StubRequestHandler(new BrokerInfo(1, "localhost", 9092)));
    acceptor.start();
    int port = acceptor.boundPort();

    MessageCodec codec = MessageCodec.instance();
    CountDownLatch start = new CountDownLatch(1);
    CountDownLatch done = new CountDownLatch(CLIENTS);
    AtomicInteger successes = new AtomicInteger();
    AtomicInteger failures = new AtomicInteger();

    try {
      for (int i = 0; i < CLIENTS; i++) {
        final long correlationId = i;
        Thread.ofVirtual()
            .start(
                () -> {
                  try (Socket socket = new Socket()) {
                    start.await();
                    socket.connect(new InetSocketAddress("localhost", port));
                    new FrameEncoder(socket.getOutputStream())
                        .write(codec.encode(new MetadataReq(correlationId)));
                    Message response =
                        codec.decode(new FrameDecoder(socket.getInputStream(), MAX).read());
                    MetadataResp meta = assertInstanceOf(MetadataResp.class, response);
                    assertEquals(correlationId, meta.correlationId());
                    successes.incrementAndGet();
                  } catch (Exception | AssertionError e) {
                    failures.incrementAndGet();
                  } finally {
                    done.countDown();
                  }
                });
      }

      start.countDown();
      assertTrue(done.await(20, TimeUnit.SECONDS), "all clients should finish");
      assertEquals(0, failures.get(), "no client should fail");
      assertEquals(CLIENTS, successes.get());
    } finally {
      acceptor.close();
    }
  }
}
