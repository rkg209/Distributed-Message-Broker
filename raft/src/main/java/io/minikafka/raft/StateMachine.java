package io.minikafka.raft;

/**
 * The application plugged into a {@link RaftNode}. {@link #apply} is called exactly once per
 * committed entry, strictly in index order, on every node (leader and followers alike) — this is
 * what makes replicated state deterministic.
 */
public interface StateMachine {

  ApplyResult apply(long index, byte[] command);
}
