package io.minikafka.protocol;

import java.util.Arrays;

/** Broker → Client: a single record at the given offset (batching arrives in a later spec). */
public record PollResp(long correlationId, long offset, byte[] payload) implements Message {

  public PollResp {
    if (payload == null) {
      throw new IllegalArgumentException("payload must not be null");
    }
  }

  @Override
  public MessageType type() {
    return MessageType.POLL_RESP;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    return o instanceof PollResp p
        && correlationId == p.correlationId
        && offset == p.offset
        && Arrays.equals(payload, p.payload);
  }

  @Override
  public int hashCode() {
    int result = Long.hashCode(correlationId);
    result = 31 * result + Long.hashCode(offset);
    result = 31 * result + Arrays.hashCode(payload);
    return result;
  }

  @Override
  public String toString() {
    return "PollResp[correlationId="
        + correlationId
        + ", offset="
        + offset
        + ", payloadLen="
        + payload.length
        + "]";
  }
}
