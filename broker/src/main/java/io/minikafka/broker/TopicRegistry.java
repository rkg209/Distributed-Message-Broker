package io.minikafka.broker;

import io.minikafka.log.InMemoryPartitionLog;
import io.minikafka.log.PartitionLog;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;

/**
 * Maps {@link TopicPartition} to its {@link PartitionLog}, creating logs lazily on first access.
 * The log {@link Supplier} is injected so Spec 03 can swap in the durable segment log without
 * touching this class.
 */
public final class TopicRegistry implements AutoCloseable {

  private final ConcurrentHashMap<TopicPartition, PartitionLog> logs = new ConcurrentHashMap<>();
  private final Supplier<PartitionLog> logFactory;

  public TopicRegistry() {
    this(InMemoryPartitionLog::new);
  }

  public TopicRegistry(Supplier<PartitionLog> logFactory) {
    this.logFactory = logFactory;
  }

  /** Returns the log for {@code tp}, creating it (via the injected factory) if absent. */
  public PartitionLog getOrCreate(TopicPartition tp) {
    return logs.computeIfAbsent(tp, ignored -> logFactory.get());
  }

  /** Returns the log for {@code tp}, or {@code null} if it has never been created. */
  public PartitionLog get(TopicPartition tp) {
    return logs.get(tp);
  }

  @Override
  public void close() {
    logs.values().forEach(PartitionLog::close);
  }
}
