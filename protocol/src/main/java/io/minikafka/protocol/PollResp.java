package io.minikafka.protocol;

import java.util.Arrays;
import java.util.List;

/**
 * Broker → Client: a batch of records starting at the poll offset. An empty batch is not an error.
 */
public record PollResp(long correlationId, List<Record> records) implements Message {

  public PollResp {
    if (records == null) {
      throw new IllegalArgumentException("records must not be null");
    }
    records = List.copyOf(records);
  }

  @Override
  public MessageType type() {
    return MessageType.POLL_RESP;
  }

  /** A single record within a poll batch. */
  public record Record(long offset, byte[] payload) {

    public Record {
      if (payload == null) {
        throw new IllegalArgumentException("payload must not be null");
      }
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      return o instanceof Record r && offset == r.offset && Arrays.equals(payload, r.payload);
    }

    @Override
    public int hashCode() {
      return 31 * Long.hashCode(offset) + Arrays.hashCode(payload);
    }

    @Override
    public String toString() {
      return "Record[offset=" + offset + ", payloadLen=" + payload.length + "]";
    }
  }
}
