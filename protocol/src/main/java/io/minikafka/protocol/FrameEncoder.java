package io.minikafka.protocol;

import java.io.IOException;
import java.io.OutputStream;

/**
 * Writes {@link Frame}s to an {@link OutputStream} as length-prefixed records:
 *
 * <pre>[length: int32 big-endian][type: 1 byte][payload: length-1 bytes]</pre>
 *
 * where {@code length == 1 + payload.length}. Not thread-safe; one encoder per connection.
 */
public final class FrameEncoder {

  private final OutputStream out;

  public FrameEncoder(OutputStream out) {
    this.out = out;
  }

  /** Writes one frame and flushes it. */
  public void write(Frame frame) throws IOException {
    int length = frame.length();
    out.write((length >>> 24) & 0xFF);
    out.write((length >>> 16) & 0xFF);
    out.write((length >>> 8) & 0xFF);
    out.write(length & 0xFF);
    out.write(frame.type().code());
    out.write(frame.payload());
    out.flush();
  }
}
