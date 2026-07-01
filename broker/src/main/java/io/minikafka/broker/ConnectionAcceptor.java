package io.minikafka.broker;

import io.minikafka.protocol.ErrorResp;
import io.minikafka.protocol.Frame;
import io.minikafka.protocol.FrameDecoder;
import io.minikafka.protocol.FrameEncoder;
import io.minikafka.protocol.Message;
import io.minikafka.protocol.MessageCodec;
import io.minikafka.protocol.ProtocolException;
import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Accepts TCP connections and serves each on its own virtual thread. Per connection it decodes
 * frames, dispatches them to a {@link RequestHandler}, and writes the response.
 *
 * <p>A malformed frame is answered with an {@code ERROR_RESP} and never takes down the acceptor:
 * corruption that breaks stream framing closes just that one connection, while a payload-level
 * decode error (framing intact) is reported and the connection continues.
 */
public final class ConnectionAcceptor implements AutoCloseable {

  private static final Logger log = LoggerFactory.getLogger(ConnectionAcceptor.class);

  private final int port;
  private final int maxFrameBytes;
  private final RequestHandler handler;
  private final MessageCodec codec = MessageCodec.instance();

  private ServerSocket serverSocket;
  private ExecutorService connectionExecutor;
  private Thread acceptThread;
  private volatile boolean running;

  public ConnectionAcceptor(int port, int maxFrameBytes, RequestHandler handler) {
    this.port = port;
    this.maxFrameBytes = maxFrameBytes;
    this.handler = handler;
  }

  /** Binds the server socket and starts the accept loop. Idempotent guard: call once. */
  public synchronized void start() throws IOException {
    if (running) {
      throw new IllegalStateException("Already started");
    }
    serverSocket = new ServerSocket(port);
    connectionExecutor = Executors.newVirtualThreadPerTaskExecutor();
    running = true;
    acceptThread = Thread.ofPlatform().name("acceptor-" + boundPort()).start(this::acceptLoop);
    log.info("ConnectionAcceptor listening on port {}", boundPort());
  }

  /** The actual bound port; useful when constructed with port 0 (ephemeral) in tests. */
  public int boundPort() {
    if (serverSocket == null) {
      throw new IllegalStateException("Not started");
    }
    return serverSocket.getLocalPort();
  }

  private void acceptLoop() {
    while (running) {
      final Socket socket;
      try {
        socket = serverSocket.accept();
      } catch (IOException e) {
        if (running) {
          log.warn("accept() failed", e);
        }
        return; // socket closed by close() or fatal accept error
      }
      connectionExecutor.submit(() -> serveConnection(socket));
    }
  }

  private void serveConnection(Socket socket) {
    try (socket;
        InputStream rawIn = socket.getInputStream();
        OutputStream rawOut = socket.getOutputStream()) {
      InputStream in = new BufferedInputStream(rawIn);
      FrameEncoder encoder = new FrameEncoder(new BufferedOutputStream(rawOut));
      FrameDecoder decoder = new FrameDecoder(in, maxFrameBytes);

      while (running) {
        Frame frame;
        try {
          frame = decoder.read();
        } catch (ProtocolException e) {
          // Framing itself is corrupt — we can't trust byte boundaries anymore, so report and
          // close.
          log.debug("Malformed frame from {}: {}", socket.getRemoteSocketAddress(), e.getMessage());
          writeQuietly(
              encoder,
              new ErrorResp(
                  ErrorResp.NO_CORRELATION, ErrorResp.CODE_PROTOCOL_ERROR, e.getMessage()));
          return;
        }
        if (frame == null) {
          return; // client closed cleanly
        }

        Message request;
        try {
          request = codec.decode(frame);
        } catch (ProtocolException e) {
          // Framing is intact; only this payload was bad — report and keep the connection open.
          log.debug(
              "Undecodable payload from {}: {}", socket.getRemoteSocketAddress(), e.getMessage());
          encoder.write(
              codec.encode(
                  new ErrorResp(
                      ErrorResp.NO_CORRELATION, ErrorResp.CODE_PROTOCOL_ERROR, e.getMessage())));
          continue;
        }

        Message response = handler.handle(request);
        encoder.write(codec.encode(response));
      }
    } catch (SocketException e) {
      log.debug("Connection reset: {}", e.getMessage());
    } catch (IOException e) {
      log.warn("Connection I/O error", e);
    }
  }

  private static void writeQuietly(FrameEncoder encoder, Message message) {
    try {
      encoder.write(MessageCodec.instance().encode(message));
    } catch (IOException e) {
      // Peer likely already gone; nothing more we can do for this connection.
    }
  }

  /** Stops accepting, closes the listening socket, and shuts down in-flight connection threads. */
  @Override
  public synchronized void close() {
    running = false;
    if (serverSocket != null) {
      try {
        serverSocket.close();
      } catch (IOException e) {
        log.warn("Error closing server socket", e);
      }
    }
    if (connectionExecutor != null) {
      connectionExecutor.shutdownNow();
    }
    if (acceptThread != null) {
      try {
        acceptThread.join(2000);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    }
  }
}
