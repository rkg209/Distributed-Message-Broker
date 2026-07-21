package io.minikafka.log;

import java.util.concurrent.atomic.AtomicLong;

/**
 * Counters used to prove the O(log n) lookup claim (criterion 4) by counting seeks rather than
 * timing wall-clock, which is noisy and environment-dependent.
 */
final class LogStats {

  final AtomicLong segmentLookups = new AtomicLong();
  final AtomicLong indexLookups = new AtomicLong();
  final AtomicLong recordScans = new AtomicLong();

  long segmentLookups() {
    return segmentLookups.get();
  }

  long indexLookups() {
    return indexLookups.get();
  }

  long recordScans() {
    return recordScans.get();
  }
}
