package io.minikafka.protocol;

/** One partition of a topic and the broker id currently acting as its leader. */
public record PartitionMetadata(int partitionId, int leaderId) {}
