package io.minikafka.broker;

import io.minikafka.log.AppendResult;
import io.minikafka.log.LogRecord;
import io.minikafka.log.PartitionLog;
import java.util.List;

/** Thin facade over {@link TopicRegistry} exposing publish/poll in terms of raw payload bytes. */
public final class PartitionManager {

  private final TopicRegistry registry;
  private final MetadataService metadataService;

  public PartitionManager(TopicRegistry registry, MetadataService metadataService) {
    this.registry = registry;
    this.metadataService = metadataService;
  }

  /**
   * Appends {@code payload} (with optional routing {@code key}) to the given partition,
   * auto-creating it if necessary.
   *
   * @throws UnknownPartitionException if {@code partition} is outside the topic's configured
   *     partition count
   */
  public AppendResult publish(String topic, int partition, byte[] key, byte[] payload) {
    validatePartition(topic, partition);
    metadataService.markTouched(topic);
    PartitionLog log = registry.getOrCreate(new TopicPartition(topic, partition));
    return log.append(new LogRecord(0, System.currentTimeMillis(), key, payload));
  }

  /**
   * Reads records from the given partition starting at {@code offset}.
   *
   * @throws UnknownPartitionException if {@code partition} is outside the topic's configured
   *     partition count
   */
  public List<LogRecord> poll(String topic, int partition, long offset, int maxBytes) {
    validatePartition(topic, partition);
    metadataService.markTouched(topic);
    PartitionLog log = registry.getOrCreate(new TopicPartition(topic, partition));
    return log.read(offset, maxBytes);
  }

  private void validatePartition(String topic, int partition) {
    int partitionCount = metadataService.partitionCountFor(topic);
    if (partition < 0 || partition >= partitionCount) {
      throw new UnknownPartitionException(topic, partition, partitionCount);
    }
  }
}
