package io.minikafka.chaos.check;

import java.util.List;

/** The outcome of one {@link Checker} run. */
public record CheckResult(String name, Status status, String summary, List<Violation> violations) {

  public CheckResult {
    violations = List.copyOf(violations);
  }

  public enum Status {
    PASS,
    FAIL,
    /**
     * The checker could not reach a verdict (e.g. search budget exhausted) — never a silent PASS.
     */
    UNKNOWN
  }

  public record Violation(String description) {}
}
