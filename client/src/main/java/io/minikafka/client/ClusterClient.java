package io.minikafka.client;

import io.minikafka.protocol.BrokerInfo;
import io.minikafka.protocol.PartitionMetadata;
import io.minikafka.protocol.ProtocolException;
import io.minikafka.protocol.TopicMetadata;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Tracks cluster metadata across a bootstrap connection and lazily opens one {@link
 * BrokerConnection} per broker id, so a producer/consumer can find and reconnect to the current
 * partition leader after a failover. Unlike {@link BrokerConnection}, which is scoped to a single
 * socket, a {@code ClusterClient} survives the death of the broker it was bootstrapped from: {@link
 * #refresh()} re-queries metadata from any broker still reachable in the last-known broker list.
 */
public final class ClusterClient implements AutoCloseable {

  private final int maxFrameBytes;
  private final Map<Integer, BrokerConnection> connections = new ConcurrentHashMap<>();

  private volatile List<BrokerInfo> brokers;
  private volatile List<TopicMetadata> topics;

  public ClusterClient(String bootstrapHost, int bootstrapPort, int maxFrameBytes)
      throws IOException {
    this.maxFrameBytes = maxFrameBytes;
    try (BrokerConnection bootstrap =
        new BrokerConnection(bootstrapHost, bootstrapPort, maxFrameBytes)) {
      MetadataClient metadataClient = new MetadataClient(bootstrap);
      this.brokers = metadataClient.fetchMetadata();
      this.topics = metadataClient.cachedTopics();
    }
  }

  /**
   * Re-queries cluster metadata, trying each broker in the last-known list until one answers. This
   * is what lets a client discover a new partition leader even when the broker it originally
   * bootstrapped from has died.
   *
   * @throws ProtocolException if no known broker is reachable
   */
  public synchronized void refresh() throws IOException {
    IOException lastError = null;
    for (BrokerInfo candidate : brokers) {
      try (BrokerConnection conn =
          new BrokerConnection(candidate.host(), candidate.port(), maxFrameBytes)) {
        MetadataClient metadataClient = new MetadataClient(conn);
        List<BrokerInfo> freshBrokers = metadataClient.fetchMetadata();
        this.brokers = freshBrokers;
        this.topics = metadataClient.cachedTopics();
        return;
      } catch (IOException e) {
        lastError = e;
        connections.remove(candidate.brokerId());
      }
    }
    throw new ProtocolException(
        "No reachable broker among " + brokers + " during metadata refresh", lastError);
  }

  /** The current leader broker id for {@code topic}/{@code partition}. */
  public int leaderFor(String topic, int partition) throws IOException {
    return partitionMetadata(topic, partition).leaderId();
  }

  /** The partition count for {@code topic}, refreshing metadata once on a cache miss. */
  public synchronized int partitionCountFor(String topic) throws IOException {
    Optional<Integer> cached = findPartitionCount(topic);
    if (cached.isPresent()) {
      return cached.get();
    }
    refresh();
    return findPartitionCount(topic)
        .orElseThrow(() -> new ProtocolException("Unknown topic: " + topic));
  }

  private Optional<Integer> findPartitionCount(String topic) {
    return topics.stream()
        .filter(t -> t.topic().equals(topic))
        .findFirst()
        .map(t -> t.partitions().size());
  }

  /** An open connection to the current leader of {@code topic}/{@code partition}. */
  public BrokerConnection connectionFor(String topic, int partition) throws IOException {
    return connectionTo(leaderFor(topic, partition));
  }

  private BrokerConnection connectionTo(int brokerId) throws IOException {
    BrokerConnection existing = connections.get(brokerId);
    if (existing != null) {
      return existing;
    }
    BrokerInfo info =
        brokers.stream()
            .filter(b -> b.brokerId() == brokerId)
            .findFirst()
            .orElseThrow(() -> new ProtocolException("Unknown broker id: " + brokerId));
    BrokerConnection created = new BrokerConnection(info.host(), info.port(), maxFrameBytes);
    connections.put(brokerId, created);
    return created;
  }

  private PartitionMetadata partitionMetadata(String topic, int partition) throws IOException {
    Optional<TopicMetadata> topicMetadata =
        topics.stream().filter(t -> t.topic().equals(topic)).findFirst();
    PartitionMetadata partitionMetadata =
        topicMetadata
            .flatMap(
                t -> t.partitions().stream().filter(p -> p.partitionId() == partition).findFirst())
            .orElseThrow(
                () -> new ProtocolException("Unknown partition " + partition + " of " + topic));
    return partitionMetadata;
  }

  @Override
  public void close() {
    for (BrokerConnection conn : connections.values()) {
      try {
        conn.close();
      } catch (IOException ignored) {
        // best-effort close on shutdown
      }
    }
    connections.clear();
  }
}
