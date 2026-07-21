package io.minikafka.log;

import java.util.List;

/**
 * A single partition's append-only record log. Spec 02 ships {@link InMemoryPartitionLog}; Spec 03
 * adds a durable, segment-backed implementation behind this same interface.
 */
public interface PartitionLog {

  /** Appends a record, assigning it the next offset. */
  AppendResult append(LogRecord record);

  /**
   * Reads records starting at {@code fromOffset}, accumulating until the next record would exceed
   * {@code maxBytes} (at least one record is always returned if one exists). Returns an empty list
   * if {@code fromOffset} is at or beyond {@link #nextOffset()}.
   */
  List<LogRecord> read(long fromOffset, int maxBytes);

  /** The offset that will be assigned to the next appended record. */
  long nextOffset();

  /** The oldest retained offset (0 until retention is implemented). */
  long firstOffset();

  /** Forces any buffered data to durable storage. No-op until Spec 03. */
  void flush();

  /** Restores state from durable storage on startup. No-op until Spec 03. */
  void recover();

  /** Releases any resources held by this log. */
  void close();
}
