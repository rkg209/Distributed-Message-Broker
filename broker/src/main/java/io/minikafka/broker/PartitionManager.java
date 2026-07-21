package io.minikafka.broker;

import io.minikafka.log.AppendResult;
import io.minikafka.log.LogRecord;
import io.minikafka.log.PartitionLog;
import java.util.List;

/** Thin facade over {@link TopicRegistry} exposing publish/poll in terms of raw payload bytes. */
public final class PartitionManager {

  private final TopicRegistry registry;

  public PartitionManager(TopicRegistry registry) {
    this.registry = registry;
  }

  /** Appends {@code payload} to the given partition, auto-creating it if necessary. */
  public AppendResult publish(String topic, int partition, byte[] payload) {
    PartitionLog log = registry.getOrCreate(new TopicPartition(topic, partition));
    return log.append(new LogRecord(0, 0, null, payload));
  }

  /** Reads records from the given partition starting at {@code offset}. */
  public List<LogRecord> poll(String topic, int partition, long offset, int maxBytes) {
    PartitionLog log = registry.getOrCreate(new TopicPartition(topic, partition));
    return log.read(offset, maxBytes);
  }
}
