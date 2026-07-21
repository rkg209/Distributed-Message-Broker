package io.minikafka.broker;

import io.minikafka.protocol.BrokerInfo;
import io.minikafka.protocol.Frame;
import io.minikafka.protocol.FrameDecoder;
import io.minikafka.protocol.FrameEncoder;
import io.minikafka.protocol.Message;
import io.minikafka.protocol.MessageCodec;
import io.minikafka.protocol.ProtocolConfig;
import io.minikafka.protocol.ProtocolException;
import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.concurrent.locks.ReentrantLock;

/**
 * An outbound, reconnecting TCP connection to one peer broker, shared by the heartbeat loop and
 * (from Spec 06) Raft RPCs. Unlike {@code client.BrokerConnection}, this is thread-safe (guarded by
 * a lock) and reconnects lazily on the next {@link #send} after any I/O failure, rate-limited by
 * {@code reconnectBackoffMs} so a dead peer doesn't spin the caller.
 */
public final class PeerConnection implements AutoCloseable {

  private final BrokerInfo peer;
  private final int maxFrameBytes;
  private final long readTimeoutMs;
  private final long reconnectBackoffMs;
  private final ReentrantLock lock = new ReentrantLock();
  private final MessageCodec codec = MessageCodec.instance();

  private Socket socket;
  private FrameEncoder encoder;
  private FrameDecoder decoder;
  private long lastConnectAttemptNanos = Long.MIN_VALUE;

  public PeerConnection(BrokerInfo peer, long readTimeoutMs, long reconnectBackoffMs) {
    this(peer, ProtocolConfig.DEFAULT_MAX_FRAME_BYTES, readTimeoutMs, reconnectBackoffMs);
  }

  public PeerConnection(
      BrokerInfo peer, int maxFrameBytes, long readTimeoutMs, long reconnectBackoffMs) {
    this.peer = peer;
    this.maxFrameBytes = maxFrameBytes;
    this.readTimeoutMs = readTimeoutMs;
    this.reconnectBackoffMs = reconnectBackoffMs;
  }

  /**
   * Sends a request and blocks for its response, connecting or reconnecting first if needed.
   *
   * @throws IOException if the peer is unreachable, within backoff, or the exchange fails; the
   *     underlying socket is closed on any failure so the next call retries a fresh connection
   */
  public Message send(Message request) throws IOException {
    lock.lock();
    try {
      ensureConnected();
      try {
        encoder.write(codec.encode(request));
        Frame frame = decoder.read();
        if (frame == null) {
          throw new EOFException("Peer " + peer.brokerId() + " closed the connection");
        }
        Message response = codec.decode(frame);
        if (response.correlationId() != request.correlationId()) {
          throw new ProtocolException(
              "Correlation id mismatch from peer "
                  + peer.brokerId()
                  + ": sent "
                  + request.correlationId()
                  + ", received "
                  + response.correlationId());
        }
        return response;
      } catch (IOException e) {
        closeSocket();
        throw e;
      }
    } finally {
      lock.unlock();
    }
  }

  /** Whether the socket is currently connected (best-effort; a stale peer may not know yet). */
  public boolean isConnected() {
    lock.lock();
    try {
      return socket != null && socket.isConnected() && !socket.isClosed();
    } finally {
      lock.unlock();
    }
  }

  private void ensureConnected() throws IOException {
    if (socket != null && socket.isConnected() && !socket.isClosed()) {
      return;
    }
    long now = System.nanoTime();
    if (lastConnectAttemptNanos != Long.MIN_VALUE
        && (now - lastConnectAttemptNanos) < reconnectBackoffMs * 1_000_000L) {
      throw new IOException(
          "Peer " + peer.brokerId() + " reconnect suppressed by backoff (" + peer + ")");
    }
    lastConnectAttemptNanos = now;
    Socket newSocket = new Socket();
    newSocket.connect(new InetSocketAddress(peer.host(), peer.port()), (int) readTimeoutMs);
    newSocket.setSoTimeout((int) readTimeoutMs);
    socket = newSocket;
    encoder = new FrameEncoder(new BufferedOutputStream(socket.getOutputStream()));
    decoder = new FrameDecoder(new BufferedInputStream(socket.getInputStream()), maxFrameBytes);
  }

  private void closeSocket() {
    if (socket != null) {
      try {
        socket.close();
      } catch (IOException ignored) {
        // Already broken; nothing more to do.
      }
      socket = null;
      encoder = null;
      decoder = null;
    }
  }

  @Override
  public void close() {
    lock.lock();
    try {
      closeSocket();
    } finally {
      lock.unlock();
    }
  }
}
