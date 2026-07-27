package io.minikafka.log;

import java.util.Arrays;

/**
 * An immutable record stored in a {@link PartitionLog}. {@code producerId == NO_PRODUCER_ID} marks
 * a non-idempotent record (bypasses dedup entirely).
 */
public record LogRecord(
    long offset, long timestamp, long producerId, long seqNo, byte[] key, byte[] value) {

  /** Sentinel producer id for non-idempotent records. */
  public static final long NO_PRODUCER_ID = -1L;

  /** Sentinel sequence number paired with {@link #NO_PRODUCER_ID}. */
  public static final long NO_SEQ = -1L;

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
        && producerId == r.producerId
        && seqNo == r.seqNo
        && Arrays.equals(key, r.key)
        && Arrays.equals(value, r.value);
  }

  @Override
  public int hashCode() {
    int result = Long.hashCode(offset);
    result = 31 * result + Long.hashCode(timestamp);
    result = 31 * result + Long.hashCode(producerId);
    result = 31 * result + Long.hashCode(seqNo);
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
        + ", producerId="
        + producerId
        + ", seqNo="
        + seqNo
        + ", valueLen="
        + value.length
        + "]";
  }
}
