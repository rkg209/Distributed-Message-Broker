package io.minikafka.chaos.check;

import java.io.IOException;
import java.util.List;

/**
 * Reads a partition directly from one specific broker, bypassing leader redirection, so {@link
 * DivergenceChecker} can compare what each replica actually has on disk. {@code
 * PartitionManager.poll} does not require leadership — it reads the replica's locally-applied log,
 * written exclusively from {@code PartitionReplica#apply}, so it only ever contains committed
 * entries. A raw {@code BrokerConnection} to each broker in turn (rather than {@code
 * ClusterClient}, which always redirects to the leader) is therefore enough; no protocol change is
 * needed.
 */
public interface ReplicaReader {

  int brokerId();

  /** One batch starting at {@code fromOffset}, capped at {@code maxBytes}. Empty past the tail. */
  List<Entry> read(String topic, int partition, long fromOffset, int maxBytes) throws IOException;

  record Entry(long offset, byte[] payload) {}
}
