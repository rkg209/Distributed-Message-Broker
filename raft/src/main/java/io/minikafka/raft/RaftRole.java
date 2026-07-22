package io.minikafka.raft;

/** The three roles a {@link RaftNode} can occupy, per Raft §5.1. */
public enum RaftRole {
  FOLLOWER,
  CANDIDATE,
  LEADER
}
