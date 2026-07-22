package io.minikafka.raft;

import java.util.Arrays;

/**
 * One entry in a Raft log. Indices are 1-based; index 0 is the implicit sentinel "empty log" entry
 * and is never actually stored.
 */
public record RaftEntry(long term, long index, byte[] command) {

  public RaftEntry {
    if (term < 0) {
      throw new IllegalArgumentException("term must be >= 0, got: " + term);
    }
    if (index < 1) {
      throw new IllegalArgumentException("index must be >= 1, got: " + index);
    }
    if (command == null) {
      throw new IllegalArgumentException("command must not be null");
    }
    command = command.clone();
  }

  @Override
  public byte[] command() {
    return command.clone();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof RaftEntry other)) {
      return false;
    }
    return term == other.term && index == other.index && Arrays.equals(command, other.command);
  }

  @Override
  public int hashCode() {
    int result = Long.hashCode(term);
    result = 31 * result + Long.hashCode(index);
    result = 31 * result + Arrays.hashCode(command);
    return result;
  }

  @Override
  public String toString() {
    return "RaftEntry[term=" + term + ", index=" + index + ", commandLen=" + command.length + "]";
  }
}
