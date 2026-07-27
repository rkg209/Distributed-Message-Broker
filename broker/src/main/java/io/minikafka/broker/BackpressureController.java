package io.minikafka.broker;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

/**
 * Bounds the number of publishes in flight per partition. Every publish must hold a permit for the
 * duration of {@link PartitionReplica#append} — since one Raft-pending proposal corresponds 1:1 to
 * one thread holding a permit, this bounds {@code RaftNode.pendingProposals} per partition and,
 * transitively, keeps the broker's heap flat under a producer that outruns Raft commit throughput
 * (NFR-10). A hot partition's semaphore is independent of every other partition's, so one saturated
 * partition cannot starve a cold one.
 *
 * <p>Non-fair semaphores: publishes to a given partition are already serialized by Raft, so strict
 * FIFO admission buys nothing and non-fair acquisition has higher throughput.
 */
final class BackpressureController {

  private final int capacity;
  private final long acquireTimeoutMs;
  private final Map<TopicPartition, Semaphore> permits = new ConcurrentHashMap<>();

  BackpressureController(int capacity, long acquireTimeoutMs) {
    this.capacity = capacity;
    this.acquireTimeoutMs = acquireTimeoutMs;
  }

  /**
   * Attempts to acquire one of {@code capacity} permits for {@code tp}, waiting up to {@code
   * acquireTimeoutMs}. Returns {@code false} on timeout or interruption — per CLAUDE.md, an
   * interrupt is never silently swallowed: the flag is restored before returning.
   */
  boolean tryAcquire(TopicPartition tp) {
    Semaphore semaphore = permits.computeIfAbsent(tp, t -> new Semaphore(capacity, false));
    try {
      return semaphore.tryAcquire(acquireTimeoutMs, TimeUnit.MILLISECONDS);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      return false;
    }
  }

  /** Releases a permit previously acquired via {@link #tryAcquire}. */
  void release(TopicPartition tp) {
    Semaphore semaphore = permits.get(tp);
    if (semaphore != null) {
      semaphore.release();
    }
  }

  /** Publishes currently in flight for {@code tp} — for tests and metrics. */
  int inFlight(TopicPartition tp) {
    Semaphore semaphore = permits.get(tp);
    return semaphore == null ? 0 : capacity - semaphore.availablePermits();
  }

  int capacity() {
    return capacity;
  }
}
