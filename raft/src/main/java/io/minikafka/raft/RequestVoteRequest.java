package io.minikafka.raft;

/** Candidate → peer vote solicitation RPC (Raft §5.2). */
public record RequestVoteRequest(long term, int candidateId, long lastLogIndex, long lastLogTerm) {

  public RequestVoteRequest {
    if (term < 0) {
      throw new IllegalArgumentException("term must be >= 0, got: " + term);
    }
    if (lastLogIndex < 0) {
      throw new IllegalArgumentException("lastLogIndex must be >= 0, got: " + lastLogIndex);
    }
    if (lastLogTerm < 0) {
      throw new IllegalArgumentException("lastLogTerm must be >= 0, got: " + lastLogTerm);
    }
  }
}
