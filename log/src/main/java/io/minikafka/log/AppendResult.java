package io.minikafka.log;

/** The offset and timestamp assigned to a record by {@link PartitionLog#append}. */
public record AppendResult(long offset, long timestamp) {}
