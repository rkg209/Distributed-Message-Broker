package io.minikafka.raft;

import java.util.List;

/** Durable (or in-memory, for tests) storage for one node's Raft log. */
public interface RaftLogStore extends AutoCloseable {

  /** Appends the entry. Entries must be appended in strictly increasing index order. */
  void append(RaftEntry entry);

  /** Returns the entry at {@code index}, or {@code null} if it is not present. */
  RaftEntry get(long index);

  /** Returns up to {@code maxEntries} consecutive entries starting at {@code index}. */
  List<RaftEntry> getFrom(long index, int maxEntries);

  /** Index of the last entry in the log, or 0 if the log is empty. */
  long lastIndex();

  /** Term of the last entry in the log, or 0 if the log is empty. */
  long lastTerm();

  /** Index of the first entry retained in the log, or 1 if none have been truncated away. */
  long firstIndex();

  /** Deletes every entry with index {@code >= fromIndex} (inclusive). */
  void truncateFrom(long fromIndex);

  void close();
}
