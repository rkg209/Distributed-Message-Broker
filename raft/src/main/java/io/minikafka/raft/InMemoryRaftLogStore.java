package io.minikafka.raft;

import java.util.ArrayList;
import java.util.List;

/** Non-durable {@link RaftLogStore} for fast unit tests. Indices are 1-based. */
public final class InMemoryRaftLogStore implements RaftLogStore {

  private final List<RaftEntry> entries = new ArrayList<>();
  private long firstIndex = 1;

  @Override
  public synchronized void append(RaftEntry entry) {
    long expected = lastIndex() + 1;
    if (entry.index() != expected) {
      throw new IllegalArgumentException(
          "expected next index " + expected + " but got " + entry.index());
    }
    entries.add(entry);
  }

  @Override
  public synchronized RaftEntry get(long index) {
    int pos = (int) (index - firstIndex);
    if (pos < 0 || pos >= entries.size()) {
      return null;
    }
    return entries.get(pos);
  }

  @Override
  public synchronized List<RaftEntry> getFrom(long index, int maxEntries) {
    List<RaftEntry> result = new ArrayList<>();
    for (long i = index; i < index + maxEntries; i++) {
      RaftEntry e = get(i);
      if (e == null) {
        break;
      }
      result.add(e);
    }
    return result;
  }

  @Override
  public synchronized long lastIndex() {
    return entries.isEmpty() ? firstIndex - 1 : entries.get(entries.size() - 1).index();
  }

  @Override
  public synchronized long lastTerm() {
    return entries.isEmpty() ? 0 : entries.get(entries.size() - 1).term();
  }

  @Override
  public synchronized long firstIndex() {
    return firstIndex;
  }

  @Override
  public synchronized void truncateFrom(long fromIndex) {
    int pos = (int) (fromIndex - firstIndex);
    if (pos < 0) {
      entries.clear();
      return;
    }
    if (pos < entries.size()) {
      entries.subList(pos, entries.size()).clear();
    }
  }

  @Override
  public synchronized void close() {
    // nothing to release
  }
}
