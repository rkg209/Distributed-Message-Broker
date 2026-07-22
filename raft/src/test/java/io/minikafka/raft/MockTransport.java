package io.minikafka.raft;

import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

/**
 * In-process {@link RaftTransport} over a {@code Map<Integer, RaftNode>}. Supports simulating
 * network partitions ({@link #partition}/{@link #heal}), fixed delivery delay, and a seeded
 * message-drop probability.
 */
final class MockTransport implements RaftTransport {

  private final Map<Integer, RaftNode> nodes = new ConcurrentHashMap<>();
  private final Set<Long> partitionedLinks = ConcurrentHashMap.newKeySet();
  private final Random random;
  private volatile long delayMs = 0;
  private volatile double dropProbability = 0.0;

  MockTransport(long seed) {
    this.random = new Random(seed);
  }

  void register(int nodeId, RaftNode node) {
    nodes.put(nodeId, node);
  }

  void unregister(int nodeId) {
    nodes.remove(nodeId);
  }

  void setDelayMs(long delayMs) {
    this.delayMs = delayMs;
  }

  void setDropProbability(double dropProbability) {
    this.dropProbability = dropProbability;
  }

  private static long linkKey(int a, int b) {
    int lo = Math.min(a, b);
    int hi = Math.max(a, b);
    return (((long) lo) << 32) | (hi & 0xffffffffL);
  }

  void partition(int a, int b) {
    partitionedLinks.add(linkKey(a, b));
  }

  void heal(int a, int b) {
    partitionedLinks.remove(linkKey(a, b));
  }

  @Override
  public CompletableFuture<AppendEntriesResponse> appendEntries(
      int targetBrokerId, AppendEntriesRequest request) {
    return dispatch(request.leaderId(), targetBrokerId, node -> node.handleAppendEntries(request));
  }

  @Override
  public CompletableFuture<RequestVoteResponse> requestVote(
      int targetBrokerId, RequestVoteRequest request) {
    return dispatch(request.candidateId(), targetBrokerId, node -> node.handleRequestVote(request));
  }

  private <T> CompletableFuture<T> dispatch(
      int sourceId, int targetBrokerId, java.util.function.Function<RaftNode, T> fn) {
    CompletableFuture<T> future = new CompletableFuture<>();
    if (partitionedLinks.contains(linkKey(sourceId, targetBrokerId))) {
      future.completeExceptionally(new java.io.IOException("link partitioned"));
      return future;
    }
    RaftNode target = nodes.get(targetBrokerId);
    if (target == null) {
      future.completeExceptionally(new IllegalStateException("unknown node " + targetBrokerId));
      return future;
    }
    Thread.ofVirtual()
        .start(
            () -> {
              try {
                if (dropProbability > 0 && random.nextDouble() < dropProbability) {
                  future.completeExceptionally(new java.io.IOException("simulated drop"));
                  return;
                }
                if (delayMs > 0) {
                  Thread.sleep(delayMs);
                }
                future.complete(fn.apply(target));
              } catch (Exception e) {
                future.completeExceptionally(e);
              }
            });
    return future;
  }
}
