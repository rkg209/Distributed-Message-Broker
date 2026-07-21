package io.minikafka.log;

/**
 * Thrown when a read requests an offset below {@link PartitionLog#firstOffset()} — i.e. an offset
 * that retention has already deleted. Reading at or beyond {@link PartitionLog#nextOffset()} is NOT
 * an error; it returns an empty batch (a consumer simply caught up).
 */
public final class OffsetOutOfRangeException extends RuntimeException {

  private final long requestedOffset;
  private final long firstOffset;
  private final long nextOffset;

  public OffsetOutOfRangeException(long requestedOffset, long firstOffset, long nextOffset) {
    super(
        "Requested offset "
            + requestedOffset
            + " is below the retained range ["
            + firstOffset
            + ", "
            + nextOffset
            + ")");
    this.requestedOffset = requestedOffset;
    this.firstOffset = firstOffset;
    this.nextOffset = nextOffset;
  }

  public long requestedOffset() {
    return requestedOffset;
  }

  public long firstOffset() {
    return firstOffset;
  }

  public long nextOffset() {
    return nextOffset;
  }
}
