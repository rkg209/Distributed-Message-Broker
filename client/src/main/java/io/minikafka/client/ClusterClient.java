package io.minikafka.client;

import io.minikafka.protocol.BrokerInfo;
import io.minikafka.protocol.PartitionMetadata;
import io.minikafka.protocol.ProtocolException;
import io.minikafka.protocol.TopicMetadata;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;

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
  private final Object refreshLock = new Object();
  private CompletableFuture<Void> inFlightRefresh;

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
   * <p>Concurrent callers coalesce onto a single in-flight refresh rather than each doing their own
   * serialized round of connection attempts: a producer/consumer pool sharing one {@code
   * ClusterClient} (as the chaos harness and any multi-threaded client does) can have dozens of
   * threads hit a stale leader at once after a failover, and running that many redundant refreshes
   * back-to-back can exhaust each caller's retry budget before its turn even comes up. Every waiter
   * gets the same outcome (success or failure) as the one refresh that actually ran.
   *
   * @throws ProtocolException if no known broker is reachable
   */
  public void refresh() throws IOException {
    CompletableFuture<Void> ongoing;
    boolean leader;
    synchronized (refreshLock) {
      if (inFlightRefresh == null) {
        inFlightRefresh = new CompletableFuture<>();
        leader = true;
      } else {
        leader = false;
      }
      ongoing = inFlightRefresh;
    }

    if (!leader) {
      try {
        ongoing.get();
        return;
      } catch (ExecutionException e) {
        throw toIOException(e.getCause());
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new ProtocolException("Interrupted while waiting for cluster metadata refresh", e);
      }
    }

    try {
      doRefresh();
      ongoing.complete(null);
    } catch (IOException e) {
      ongoing.completeExceptionally(e);
      throw e;
    } catch (RuntimeException | Error e) {
      // Not just IOException: leave no coalesced waiter blocked forever on a future the leader
      // never completes because it died from something unchecked (e.g. a malformed metadata
      // frame tripping a parse exception).
      ongoing.completeExceptionally(e);
      throw e;
    } finally {
      synchronized (refreshLock) {
        inFlightRefresh = null;
      }
    }
  }

  private void doRefresh() throws IOException {
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

  private static IOException toIOException(Throwable cause) {
    if (cause instanceof IOException io) {
      return io;
    }
    return new ProtocolException("Cluster metadata refresh failed", cause);
  }

  /** The current leader broker id for {@code topic}/{@code partition}. */
  public int leaderFor(String topic, int partition) throws IOException {
    return partitionMetadata(topic, partition).leaderId();
  }

  /**
   * Every broker id known from the last metadata fetch/refresh — used by {@link CoordinatorCall} to
   * pick an initial broker to try before it learns the actual group coordinator (the static
   * controller) from a {@code CODE_NOT_COORDINATOR} redirect.
   */
  public List<Integer> brokerIds() {
    return brokers.stream().map(BrokerInfo::brokerId).toList();
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

  /** An open connection to broker {@code brokerId}, pooled across calls. */
  public BrokerConnection connectionTo(int brokerId) throws IOException {
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

  /**
   * Closes and forgets the pooled connection to {@code brokerId}, if any, so the next {@link
   * #connectionFor} opens a fresh one. Callers must do this after any {@link IOException} on a
   * request over that connection: a read timeout in particular leaves the socket open with a stale,
   * unread response still buffered for a prior correlation id, and reusing it would corrupt every
   * subsequent exchange on that connection with a correlation-id mismatch.
   */
  public void evict(int brokerId) {
    BrokerConnection removed = connections.remove(brokerId);
    if (removed != null) {
      try {
        removed.close();
      } catch (IOException ignored) {
        // best-effort close of a connection we're discarding anyway
      }
    }
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
