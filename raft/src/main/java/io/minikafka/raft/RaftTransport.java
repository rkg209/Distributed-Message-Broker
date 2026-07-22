package io.minikafka.raft;

import java.util.concurrent.CompletableFuture;

/**
 * Sends Raft RPCs to a peer. Implementations may fail the returned future for network reasons
 * (timeout, unreachable peer) — that must never be conflated with an RPC that reached the peer and
 * was rejected.
 */
public interface RaftTransport {

  CompletableFuture<AppendEntriesResponse> appendEntries(
      int targetBrokerId, AppendEntriesRequest request);

  CompletableFuture<RequestVoteResponse> requestVote(
      int targetBrokerId, RequestVoteRequest request);
}
