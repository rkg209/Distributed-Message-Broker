package io.minikafka.raft;

import java.util.concurrent.ThreadLocalRandom;
import java.util.function.LongSupplier;

/**
 * Fires {@code onTimeout} when no {@link #reset()} has occurred for a randomized interval in {@code
 * [minElectionTimeoutMs, maxElectionTimeoutMs)} — re-randomized on every reset to avoid repeated
 * split votes (Raft §5.2). Runs on its own virtual thread and polls in small increments so it can
 * be suppressed/resumed promptly when the node becomes/steps down as leader.
 */
final class ElectionTimer {

  private static final long POLL_INTERVAL_MS = 5;

  private final RaftConfig config;
  private final LongSupplier clockNanos;
  private final Runnable onTimeout;

  private volatile Thread thread;
  private volatile boolean running;
  private volatile boolean suppressed;
  private volatile long deadlineNanos;

  ElectionTimer(RaftConfig config, LongSupplier clockNanos, Runnable onTimeout) {
    this.config = config;
    this.clockNanos = clockNanos;
    this.onTimeout = onTimeout;
  }

  void start() {
    running = true;
    reset();
    thread = Thread.ofVirtual().name("raft-election-timer").start(this::loop);
  }

  private void loop() {
    while (running) {
      try {
        Thread.sleep(POLL_INTERVAL_MS);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        return;
      }
      if (!running || suppressed) {
        continue;
      }
      if (clockNanos.getAsLong() >= deadlineNanos) {
        onTimeout.run();
      }
    }
  }

  /** Randomizes a new deadline and re-arms the timer. */
  void reset() {
    long spanMs = config.maxElectionTimeoutMs() - config.minElectionTimeoutMs();
    long timeoutMs =
        config.minElectionTimeoutMs()
            + (spanMs <= 0 ? 0 : ThreadLocalRandom.current().nextLong(spanMs));
    deadlineNanos = clockNanos.getAsLong() + timeoutMs * 1_000_000L;
  }

  /** Disables timeout firing (called on becoming leader). */
  void suppress() {
    suppressed = true;
  }

  /** Re-enables timeout firing with a fresh randomized deadline (called on stepping down). */
  void resume() {
    reset();
    suppressed = false;
  }

  void close() {
    running = false;
    Thread t = thread;
    if (t != null) {
      t.interrupt();
    }
  }
}
