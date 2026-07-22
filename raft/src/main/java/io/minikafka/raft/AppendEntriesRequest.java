package io.minikafka.raft;

import java.util.List;

/** Leader → follower log replication / heartbeat RPC (Raft §5.3). */
public record AppendEntriesRequest(
    long term,
    int leaderId,
    long prevLogIndex,
    long prevLogTerm,
    List<RaftEntry> entries,
    long leaderCommit) {

  public AppendEntriesRequest {
    if (term < 0) {
      throw new IllegalArgumentException("term must be >= 0, got: " + term);
    }
    if (prevLogIndex < 0) {
      throw new IllegalArgumentException("prevLogIndex must be >= 0, got: " + prevLogIndex);
    }
    if (prevLogTerm < 0) {
      throw new IllegalArgumentException("prevLogTerm must be >= 0, got: " + prevLogTerm);
    }
    if (entries == null) {
      throw new IllegalArgumentException("entries must not be null");
    }
    if (leaderCommit < 0) {
      throw new IllegalArgumentException("leaderCommit must be >= 0, got: " + leaderCommit);
    }
    entries = List.copyOf(entries);
  }
}
