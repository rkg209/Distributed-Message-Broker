package io.minikafka.broker;

import io.minikafka.protocol.Frame;
import io.minikafka.protocol.FrameDecoder;
import io.minikafka.protocol.FrameEncoder;
import io.minikafka.protocol.ProtocolConfig;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * A seeded TCP proxy that forwards each request frame to a real broker, forwards the response back
 * to the client, and — with a configurable probability — drops the response by closing the client
 * socket instead of forwarding it. The broker has already committed the request by the time the
 * drop decision is made, so this reproduces exactly the fault {@link
 * io.minikafka.client.RedirectingCall}'s {@code IOException} retry exists for: the message
 * committed but the ack never made it back. Used by {@link ChaosDedupTest}.
 */
final class FlakyProxy implements AutoCloseable {

  private final ServerSocket serverSocket;
  private final String targetHost;
  private final int targetPort;
  private final double dropProbability;
  private final Random random;
  private final ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor();
  private volatile boolean closed;

  FlakyProxy(String targetHost, int targetPort, double dropProbability, long seed)
      throws IOException {
    this.serverSocket = new ServerSocket(0);
    this.targetHost = targetHost;
    this.targetPort = targetPort;
    this.dropProbability = dropProbability;
    this.random = new Random(seed);
    executor.submit(this::acceptLoop);
  }

  int boundPort() {
    return serverSocket.getLocalPort();
  }

  private void acceptLoop() {
    while (!closed) {
      try {
        Socket client = serverSocket.accept();
        executor.submit(() -> relay(client));
      } catch (IOException e) {
        return; // serverSocket closed by close()
      }
    }
  }

  private void relay(Socket client) {
    try (client;
        Socket broker = new Socket(targetHost, targetPort)) {
      InputStream clientIn = client.getInputStream();
      OutputStream clientOut = client.getOutputStream();
      InputStream brokerIn = broker.getInputStream();
      OutputStream brokerOut = broker.getOutputStream();
      FrameDecoder clientDecoder =
          new FrameDecoder(clientIn, ProtocolConfig.DEFAULT_MAX_FRAME_BYTES);
      FrameEncoder brokerEncoder = new FrameEncoder(brokerOut);
      FrameDecoder brokerDecoder =
          new FrameDecoder(brokerIn, ProtocolConfig.DEFAULT_MAX_FRAME_BYTES);
      FrameEncoder clientEncoder = new FrameEncoder(clientOut);

      while (true) {
        Frame request = clientDecoder.read();
        if (request == null) {
          return;
        }
        brokerEncoder.write(request);
        Frame response = brokerDecoder.read();
        if (response == null) {
          return;
        }
        boolean drop;
        synchronized (random) {
          drop = random.nextDouble() < dropProbability;
        }
        if (drop) {
          // The broker already committed; close without forwarding so the client sees an
          // IOException on this request specifically, forcing a retry.
          return;
        }
        clientEncoder.write(response);
      }
    } catch (IOException e) {
      // Connection torn down mid-relay — expected under fault injection.
    }
  }

  @Override
  public void close() {
    closed = true;
    try {
      serverSocket.close();
    } catch (IOException ignored) {
      // best-effort close on shutdown
    }
    executor.shutdownNow();
  }
}
