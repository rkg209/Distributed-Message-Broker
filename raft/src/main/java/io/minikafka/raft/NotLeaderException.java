package io.minikafka.raft;

/**
 * Thrown to fail a {@link RaftNode#propose} future when the node is not (or is no longer) leader.
 */
public class NotLeaderException extends RuntimeException {

  private final int leaderId;

  public NotLeaderException(int leaderId) {
    super("not leader; current known leader is " + (leaderId < 0 ? "unknown" : leaderId));
    this.leaderId = leaderId;
  }

  /** The node's best guess at the current leader, or -1 if unknown. */
  public int leaderId() {
    return leaderId;
  }
}
