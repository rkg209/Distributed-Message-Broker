package io.minikafka.broker;

import io.minikafka.log.AppendResult;
import io.minikafka.log.LogRecord;
import io.minikafka.log.PartitionLog;
import io.minikafka.protocol.BrokerInfo;
import io.minikafka.raft.FileRaftLogStore;
import io.minikafka.raft.PersistentState;
import io.minikafka.raft.RaftConfig;
import io.minikafka.raft.RaftNode;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Function;

/**
 * Owns one {@link PartitionReplica} (Raft group + state-machine log) per {@link TopicPartition}
 * this broker replicates. A publish is acknowledged only after majority replication; a poll only
 * ever sees committed data, since {@link PartitionReplica#apply} is the sole writer of the
 * underlying {@link PartitionLog}.
 */
public final class PartitionManager implements AutoCloseable {

  private final BrokerConfig config;
  private final MetadataService metadataService;
  private final ClusterConfig clusterConfig;
  private final int selfId;
  private final TopicRegistry registry;
  private final Map<TopicPartition, PartitionReplica> replicas = new ConcurrentHashMap<>();
  private final ExecutorService vthreads = Executors.newVirtualThreadPerTaskExecutor();

  public PartitionManager(
      BrokerConfig config,
      MetadataService metadataService,
      ClusterConfig clusterConfig,
      Function<TopicPartition, PartitionLog> logFactory) {
    this.config = config;
    this.metadataService = metadataService;
    this.clusterConfig = clusterConfig;
    this.selfId = config.brokerId();
    this.registry = new TopicRegistry(logFactory);
  }

  /**
   * Eagerly creates a Raft group for every {@code (topic, partition)} configured in {@link
   * BrokerConfig#topicConfig()} that this broker replicates. Eager creation (rather than lazy, on
   * first publish/poll) avoids a race on the follower side: an inbound {@code AppendEntries} for a
   * partition this broker hasn't touched yet must find a live {@link RaftNode}.
   */
  public void start() {
    config
        .topicConfig()
        .partitionCounts()
        .forEach(
            (topic, count) -> {
              for (int p = 0; p < count; p++) {
                TopicPartition tp = new TopicPartition(topic, p);
                if (replicaIdsFor(tp).contains(selfId)) {
                  replicaFor(tp);
                }
              }
            });
  }

  /**
   * Appends {@code payload} (with optional routing {@code key}) to the given partition,
   * auto-creating its Raft group if necessary, and blocks until majority-committed.
   *
   * @throws UnknownPartitionException if {@code partition} is outside the topic's configured
   *     partition count
   * @throws io.minikafka.raft.NotLeaderException if this broker is not (or is no longer) the
   *     partition's Raft leader
   */
  public AppendResult publish(String topic, int partition, byte[] key, byte[] payload) {
    validatePartition(topic, partition);
    metadataService.markTouched(topic);
    return replicaFor(new TopicPartition(topic, partition)).append(key, payload);
  }

  /**
   * Reads records from the given partition starting at {@code offset}. Only committed
   * (majority-replicated) records are ever visible, since the backing log is written exclusively
   * from {@link PartitionReplica#apply}.
   *
   * @throws UnknownPartitionException if {@code partition} is outside the topic's configured
   *     partition count
   */
  public List<LogRecord> poll(String topic, int partition, long offset, int maxBytes) {
    validatePartition(topic, partition);
    metadataService.markTouched(topic);
    return replicaFor(new TopicPartition(topic, partition)).read(offset, maxBytes);
  }

  /** The replica for {@code tp}, or {@code null} if this broker doesn't host it. */
  PartitionReplica replica(TopicPartition tp) {
    return replicas.get(tp);
  }

  /** This broker's current leader epoch (Raft term) for {@code tp}, or {@code 0} if unhosted. */
  public long leaderEpochFor(TopicPartition tp) {
    PartitionReplica replica = replicas.get(tp);
    return replica == null ? 0 : replica.currentLeaderEpoch();
  }

  private PartitionReplica replicaFor(TopicPartition tp) {
    return replicas.computeIfAbsent(tp, this::createReplica);
  }

  private List<Integer> replicaIdsFor(TopicPartition tp) {
    return clusterConfig
        .assignmentFor(tp)
        .map(ClusterConfig.PartitionAssignment::replicaIds)
        .orElse(List.of(selfId));
  }

  private PartitionReplica createReplica(TopicPartition tp) {
    List<Integer> replicaIds = replicaIdsFor(tp);
    List<Integer> peerIds = replicaIds.stream().filter(id -> id != selfId).toList();
    Map<Integer, BrokerInfo> peerBrokers = new HashMap<>();
    for (int peerId : peerIds) {
      BrokerInfo info =
          clusterConfig
              .broker(peerId)
              .orElseThrow(
                  () ->
                      new IllegalStateException(
                          "No BrokerInfo for peer " + peerId + " of partition " + tp));
      peerBrokers.put(peerId, info);
    }

    RaftConfig raftConfig = config.raftConfigFor(tp);
    FileRaftLogStore raftLogStore = FileRaftLogStore.open(raftConfig.stateDir());
    PersistentState persistentState = PersistentState.load(raftConfig.stateDir());
    BrokerRaftTransport transport =
        new BrokerRaftTransport(
            tp, peerBrokers, raftConfig.rpcTimeoutMs(), config.peerReconnectBackoffMs(), vthreads);
    PartitionLog partitionLog = registry.getOrCreate(tp);

    PartitionReplica replica =
        new PartitionReplica(
            tp,
            raftLogStore,
            partitionLog,
            transport,
            metadataService,
            config.raftProposeTimeoutMs(),
            config.raftLeaderWaitMs());
    RaftNode raftNode =
        new RaftNode(
            selfId,
            peerIds,
            raftConfig,
            raftLogStore,
            persistentState,
            transport,
            replica,
            System::nanoTime);
    replica.attachRaftNode(raftNode);
    replica.start();
    return replica;
  }

  private void validatePartition(String topic, int partition) {
    int partitionCount = metadataService.partitionCountFor(topic);
    if (partition < 0 || partition >= partitionCount) {
      throw new UnknownPartitionException(topic, partition, partitionCount);
    }
  }

  @Override
  public void close() {
    replicas.values().forEach(PartitionReplica::close);
    vthreads.shutdown();
  }
}
