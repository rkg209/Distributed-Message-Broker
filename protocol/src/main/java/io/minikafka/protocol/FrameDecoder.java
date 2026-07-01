package io.minikafka.protocol;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;

/**
 * Reads length-prefixed {@link Frame}s from an {@link InputStream}. Blocks until a complete frame
 * is available. Not thread-safe; one decoder per connection.
 *
 * <p>Malformed input never hangs or crashes the caller: a bad length, an over-long frame, an
 * unknown type byte, or a truncated payload all raise {@link ProtocolException}. A clean
 * end-of-stream at a frame boundary returns {@code null}.
 */
public final class FrameDecoder {

  private final InputStream in;
  private final int maxFrameBytes;

  public FrameDecoder(InputStream in, int maxFrameBytes) {
    if (maxFrameBytes < 1) {
      throw new IllegalArgumentException("maxFrameBytes must be >= 1, got: " + maxFrameBytes);
    }
    this.in = in;
    this.maxFrameBytes = maxFrameBytes;
  }

  /**
   * Reads the next frame.
   *
   * @return the frame, or {@code null} if the stream closed cleanly at a frame boundary
   * @throws ProtocolException if the frame is malformed (bad length, too large, unknown type, or
   *     truncated)
   * @throws IOException if the underlying stream fails
   */
  public Frame read() throws IOException {
    int b0 = in.read();
    if (b0 == -1) {
      return null; // clean close at a frame boundary
    }
    int b1 = in.read();
    int b2 = in.read();
    int b3 = in.read();
    if ((b1 | b2 | b3) < 0) {
      throw new ProtocolException("Truncated length prefix");
    }
    int length = (b0 << 24) | (b1 << 16) | (b2 << 8) | b3;

    if (length < 1) {
      throw new ProtocolException("Frame length must be >= 1, got: " + length);
    }
    // Reject before allocating any payload buffer — this is the primary DoS guard.
    if (length > maxFrameBytes) {
      throw new ProtocolException("Frame length " + length + " exceeds max " + maxFrameBytes);
    }

    int typeByte = in.read();
    if (typeByte == -1) {
      throw new ProtocolException("Truncated frame: missing type byte");
    }
    MessageType type = MessageType.fromCode((byte) typeByte);

    byte[] payload = new byte[length - 1];
    readFully(payload);
    return new Frame(type, payload);
  }

  private void readFully(byte[] dest) throws IOException {
    int off = 0;
    while (off < dest.length) {
      int n = in.read(dest, off, dest.length - off);
      if (n == -1) {
        throw new EOFException("Truncated frame payload: expected " + dest.length + ", got " + off);
      }
      off += n;
    }
  }
}
