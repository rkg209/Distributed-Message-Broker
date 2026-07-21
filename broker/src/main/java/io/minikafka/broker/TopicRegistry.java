package io.minikafka.broker;

import io.minikafka.log.InMemoryPartitionLog;
import io.minikafka.log.PartitionLog;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

/**
 * Maps {@link TopicPartition} to its {@link PartitionLog}, creating logs lazily on first access.
 * The log factory is injected as a {@link Function} (rather than a {@code Supplier}) because the
 * durable {@code DiskPartitionLog} needs the partition identity to pick its own storage directory.
 */
public final class TopicRegistry implements AutoCloseable {

  private final ConcurrentHashMap<TopicPartition, PartitionLog> logs = new ConcurrentHashMap<>();
  private final Function<TopicPartition, PartitionLog> logFactory;

  public TopicRegistry() {
    this(tp -> new InMemoryPartitionLog());
  }

  public TopicRegistry(Function<TopicPartition, PartitionLog> logFactory) {
    this.logFactory = logFactory;
  }

  /** Returns the log for {@code tp}, creating it (via the injected factory) if absent. */
  public PartitionLog getOrCreate(TopicPartition tp) {
    return logs.computeIfAbsent(tp, logFactory);
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
