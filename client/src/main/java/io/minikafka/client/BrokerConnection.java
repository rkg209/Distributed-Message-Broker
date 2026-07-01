package io.minikafka.client;

import io.minikafka.protocol.Frame;
import io.minikafka.protocol.FrameDecoder;
import io.minikafka.protocol.FrameEncoder;
import io.minikafka.protocol.Message;
import io.minikafka.protocol.MessageCodec;
import io.minikafka.protocol.ProtocolException;
import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.concurrent.atomic.AtomicLong;

/**
 * A single TCP connection to one broker. Spec 01 supports synchronous, one-request-at-a-time
 * exchanges with correlation-id matching; connection pooling, retries, and pipelining are out of
 * scope until later specs.
 *
 * <p>Not thread-safe: {@link #request} must not be called concurrently on the same connection.
 */
public final class BrokerConnection implements AutoCloseable {

  private final Socket socket;
  private final FrameEncoder encoder;
  private final FrameDecoder decoder;
  private final MessageCodec codec = MessageCodec.instance();
  private final AtomicLong correlationCounter = new AtomicLong();

  public BrokerConnection(String host, int port, int maxFrameBytes) throws IOException {
    this.socket = new Socket();
    this.socket.connect(new InetSocketAddress(host, port));
    this.encoder = new FrameEncoder(new BufferedOutputStream(socket.getOutputStream()));
    this.decoder =
        new FrameDecoder(new BufferedInputStream(socket.getInputStream()), maxFrameBytes);
  }

  /** Allocates a fresh correlation id, unique within this connection. */
  public long nextCorrelationId() {
    return correlationCounter.incrementAndGet();
  }

  /**
   * Sends a request and blocks for its response.
   *
   * @throws ProtocolException if the server closes mid-exchange or replies with a mismatched
   *     correlation id
   */
  public Message request(Message request) throws IOException {
    encoder.write(codec.encode(request));
    Frame frame = decoder.read();
    if (frame == null) {
      throw new EOFException("Broker closed the connection before responding");
    }
    Message response = codec.decode(frame);
    if (response.correlationId() != request.correlationId()) {
      throw new ProtocolException(
          "Correlation id mismatch: sent "
              + request.correlationId()
              + ", received "
              + response.correlationId());
    }
    return response;
  }

  @Override
  public void close() throws IOException {
    socket.close();
  }
}
