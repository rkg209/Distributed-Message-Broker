package io.minikafka.client;

import io.minikafka.protocol.BrokerInfo;
import io.minikafka.protocol.ErrorResp;
import io.minikafka.protocol.Message;
import io.minikafka.protocol.MetadataReq;
import io.minikafka.protocol.MetadataResp;
import io.minikafka.protocol.ProtocolException;
import io.minikafka.protocol.TopicMetadata;
import java.io.IOException;
import java.util.List;
import java.util.Optional;

/**
 * Fetches and caches cluster metadata (broker list, topic partition counts) over a {@link
 * BrokerConnection}.
 */
public final class MetadataClient {

  private final BrokerConnection connection;
  private List<BrokerInfo> cachedBrokers = List.of();
  private List<TopicMetadata> cachedTopics = List.of();

  public MetadataClient(BrokerConnection connection) {
    this.connection = connection;
  }

  /** Fetches the current broker list and topic metadata from the connected broker. */
  public List<BrokerInfo> fetchMetadata() throws IOException {
    MetadataReq req = new MetadataReq(connection.nextCorrelationId());
    Message response = connection.request(req);
    return switch (response) {
      case MetadataResp resp -> {
        cachedBrokers = resp.brokers();
        cachedTopics = resp.topics();
        yield cachedBrokers;
      }
      case ErrorResp err -> throw new ProtocolException("Metadata fetch failed: " + err.message());
      default -> throw new ProtocolException("Unexpected response type: " + response.type());
    };
  }

  /** The broker list from the last successful {@link #fetchMetadata()} call. */
  public List<BrokerInfo> cachedBrokers() {
    return cachedBrokers;
  }

  /** The topic metadata from the last successful {@link #fetchMetadata()} call. */
  public List<TopicMetadata> cachedTopics() {
    return cachedTopics;
  }

  /** The partition count for {@code topic}, fetching fresh metadata on a cache miss. */
  public int partitionCountFor(String topic) throws IOException {
    Optional<Integer> cached = findPartitionCount(topic, cachedTopics);
    if (cached.isPresent()) {
      return cached.get();
    }
    fetchMetadata();
    return findPartitionCount(topic, cachedTopics)
        .orElseThrow(() -> new ProtocolException("Unknown topic: " + topic));
  }

  private static Optional<Integer> findPartitionCount(String topic, List<TopicMetadata> topics) {
    return topics.stream()
        .filter(t -> t.topic().equals(topic))
        .findFirst()
        .map(t -> t.partitions().size());
  }
}
