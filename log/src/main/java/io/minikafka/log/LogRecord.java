package io.minikafka.log;

import java.util.Arrays;

/**
 * An immutable record stored in a {@link PartitionLog}. Spec 02 leaves {@code key} unused (null) —
 * keys arrive in Spec 04.
 */
public record LogRecord(long offset, long timestamp, byte[] key, byte[] value) {

  public LogRecord {
    if (value == null) {
      throw new IllegalArgumentException("value must not be null");
    }
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    return o instanceof LogRecord r
        && offset == r.offset
        && timestamp == r.timestamp
        && Arrays.equals(key, r.key)
        && Arrays.equals(value, r.value);
  }

  @Override
  public int hashCode() {
    int result = Long.hashCode(offset);
    result = 31 * result + Long.hashCode(timestamp);
    result = 31 * result + Arrays.hashCode(key);
    result = 31 * result + Arrays.hashCode(value);
    return result;
  }

  @Override
  public String toString() {
    return "LogRecord[offset="
        + offset
        + ", timestamp="
        + timestamp
        + ", valueLen="
        + value.length
        + "]";
  }
}
