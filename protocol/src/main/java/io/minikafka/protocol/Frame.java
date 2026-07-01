package io.minikafka.protocol;

import java.util.Arrays;

/**
 * A raw, type-tagged frame: the still-serialized unit that {@link FrameEncoder} writes and {@link
 * FrameDecoder} reads. The on-wire {@code length} field equals {@code 1 + payload.length} (the type
 * byte plus the payload). Decoding a {@code Frame} into a typed {@link Message} is {@link
 * MessageCodec}'s job.
 */
public record Frame(MessageType type, byte[] payload) {

  public Frame {
    if (type == null) {
      throw new IllegalArgumentException("type must not be null");
    }
    if (payload == null) {
      throw new IllegalArgumentException("payload must not be null");
    }
  }

  /** The on-wire {@code length} field: type byte (1) plus payload bytes. */
  public int length() {
    return 1 + payload.length;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    return o instanceof Frame other && type == other.type && Arrays.equals(payload, other.payload);
  }

  @Override
  public int hashCode() {
    return 31 * type.hashCode() + Arrays.hashCode(payload);
  }

  @Override
  public String toString() {
    return "Frame[type=" + type + ", payloadLen=" + payload.length + "]";
  }
}
