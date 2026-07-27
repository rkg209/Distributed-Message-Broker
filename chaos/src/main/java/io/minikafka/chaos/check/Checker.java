package io.minikafka.chaos.check;

import io.minikafka.chaos.HistoryRecorder.History;

/** Verifies one correctness invariant against a recorded chaos-run {@link History}. */
public interface Checker {

  CheckResult check(History history);
}
