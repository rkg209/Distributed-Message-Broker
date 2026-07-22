package io.minikafka.raft;

/**
 * Follower's reply to {@link AppendEntriesRequest}. {@code conflictIndex}/{@code conflictTerm} let
 * the leader back off {@code nextIndex} by a whole term per round-trip instead of one entry at a
 * time; {@code followerLastIndex} lets the leader set {@code matchIndex} directly instead of
 * inferring it from the request it sent.
 */
public record AppendEntriesResponse(
    long term, boolean success, long conflictIndex, long conflictTerm, long followerLastIndex) {

  public AppendEntriesResponse {
    if (term < 0) {
      throw new IllegalArgumentException("term must be >= 0, got: " + term);
    }
    if (conflictIndex < 0) {
      throw new IllegalArgumentException("conflictIndex must be >= 0, got: " + conflictIndex);
    }
    if (conflictTerm < 0) {
      throw new IllegalArgumentException("conflictTerm must be >= 0, got: " + conflictTerm);
    }
    if (followerLastIndex < 0) {
      throw new IllegalArgumentException(
          "followerLastIndex must be >= 0, got: " + followerLastIndex);
    }
  }
}
