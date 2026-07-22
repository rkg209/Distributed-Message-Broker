package io.minikafka.raft;

/** Peer's reply to {@link RequestVoteRequest}. */
public record RequestVoteResponse(long term, boolean voteGranted) {

  public RequestVoteResponse {
    if (term < 0) {
      throw new IllegalArgumentException("term must be >= 0, got: " + term);
    }
  }
}
