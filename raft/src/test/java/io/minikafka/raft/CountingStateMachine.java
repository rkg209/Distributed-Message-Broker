package io.minikafka.raft;

import java.util.ArrayList;
import java.util.List;

/** Records every committed {@code (index, command)} pair, in the order {@code apply} was called. */
final class CountingStateMachine implements StateMachine {

  record Applied(long index, byte[] command) {}

  private final List<Applied> applied = new ArrayList<>();

  @Override
  public synchronized ApplyResult apply(long index, byte[] command) {
    applied.add(new Applied(index, command));
    return ApplyResult.ok(command);
  }

  synchronized List<Applied> applied() {
    return List.copyOf(applied);
  }
}
