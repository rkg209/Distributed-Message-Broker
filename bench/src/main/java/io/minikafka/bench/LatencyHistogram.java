package io.minikafka.bench;

import java.util.Arrays;
import java.util.concurrent.locks.ReentrantLock;

/**
 * A minimal on-heap latency histogram: records values into a fixed-size growable array under a lock
 * (called once per publish, not a hot inner loop, so lock contention is not a concern) and computes
 * percentiles by sorting on read. Not built for JMH's sampling mode — that's {@code
 * PublishLatencyBenchmark}'s job — this is only for {@link LoadGenerator}'s summary line.
 */
final class LatencyHistogram {

  private final ReentrantLock lock = new ReentrantLock();
  private long[] values = new long[1024];
  private int size = 0;

  void record(long millis) {
    lock.lock();
    try {
      if (size == values.length) {
        values = Arrays.copyOf(values, values.length * 2);
      }
      values[size++] = millis;
    } finally {
      lock.unlock();
    }
  }

  long percentile(double p) {
    lock.lock();
    try {
      if (size == 0) {
        return 0;
      }
      long[] sorted = Arrays.copyOf(values, size);
      Arrays.sort(sorted);
      int index = (int) Math.ceil((p / 100.0) * sorted.length) - 1;
      index = Math.max(0, Math.min(sorted.length - 1, index));
      return sorted[index];
    } finally {
      lock.unlock();
    }
  }
}
