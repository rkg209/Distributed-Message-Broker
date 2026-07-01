package io.minikafka.protocol;

/**
 * Protocol-level configuration. Externalized so the maximum frame size (a DoS guard in {@link
 * FrameDecoder}) is never hardcoded at a call site.
 *
 * @param maxFrameBytes maximum value of the on-wire {@code length} field (type byte + payload). A
 *     frame whose declared length exceeds this is rejected before any payload buffer is allocated.
 */
public record ProtocolConfig(int maxFrameBytes) {

  /** Default maximum frame size: 16 MB. */
  public static final int DEFAULT_MAX_FRAME_BYTES = 16 * 1024 * 1024;

  public ProtocolConfig {
    if (maxFrameBytes < 1) {
      throw new IllegalArgumentException("maxFrameBytes must be >= 1, got: " + maxFrameBytes);
    }
  }

  public static ProtocolConfig defaults() {
    return new ProtocolConfig(DEFAULT_MAX_FRAME_BYTES);
  }
}
