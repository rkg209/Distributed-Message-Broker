package io.minikafka.client;

import io.minikafka.protocol.ErrorResp;
import io.minikafka.protocol.Message;
import io.minikafka.protocol.ProtocolException;
import io.minikafka.protocol.PublishReq;
import io.minikafka.protocol.PublishResp;
import java.io.IOException;

/**
 * Publishes records to a topic partition, either over a single {@link BrokerConnection} (Specs
 * 02–07: the connected broker must already be the leader) or over a {@link ClusterClient}, which
 * resolves the current leader and transparently redirects/retries across a failover.
 */
public final class ProducerClient {

  /** Default bound on redirect retries before a publish gives up and throws. */
  public static final int DEFAULT_MAX_RETRIES = 5;

  /** Default backoff between a redirect and its retry. */
  public static final long DEFAULT_RETRY_BACKOFF_MS = 100;

  private final BrokerConnection connection;
  private final MetadataClient metadataClient;
  private final ClusterClient clusterClient;
  private final PartitionRouter router;
  private final int maxRetries;
  private final long retryBackoffMs;

  public ProducerClient(BrokerConnection connection) {
    this(connection, new MetadataClient(connection), new PartitionRouter());
  }

  public ProducerClient(
      BrokerConnection connection, MetadataClient metadataClient, PartitionRouter router) {
    this.connection = connection;
    this.metadataClient = metadataClient;
    this.clusterClient = null;
    this.router = router;
    this.maxRetries = 0;
    this.retryBackoffMs = 0;
  }

  /** A redirect-aware producer over {@code clusterClient}, using default retry tunables. */
  public ProducerClient(ClusterClient clusterClient) {
    this(clusterClient, new PartitionRouter(), DEFAULT_MAX_RETRIES, DEFAULT_RETRY_BACKOFF_MS);
  }

  public ProducerClient(
      ClusterClient clusterClient, PartitionRouter router, int maxRetries, long retryBackoffMs) {
    this.connection = null;
    this.metadataClient = null;
    this.clusterClient = clusterClient;
    this.router = router;
    this.maxRetries = maxRetries;
    this.retryBackoffMs = retryBackoffMs;
  }

  /** Publishes {@code payload} to {@code topic}/{@code partition}, returning its offset. */
  public long publish(String topic, int partition, byte[] payload) throws IOException {
    Message response =
        send(
            topic,
            partition,
            conn ->
                conn.request(
                    new PublishReq(conn.nextCorrelationId(), topic, partition, null, payload)));
    return switch (response) {
      case PublishResp resp -> resp.offset();
      case ErrorResp err -> throw new ProtocolException("Publish failed: " + err.message());
      default -> throw new ProtocolException("Unexpected response type: " + response.type());
    };
  }

  /**
   * Publishes {@code payload} to {@code topic}, routing on {@code key} (or round-robin if {@code
   * key} is null) using this topic's partition count.
   */
  public PublishAck publish(String topic, byte[] key, byte[] payload) throws IOException {
    int numPartitions = partitionCountFor(topic);
    int partition = router.route(key, numPartitions);
    Message response =
        send(
            topic,
            partition,
            conn ->
                conn.request(
                    new PublishReq(conn.nextCorrelationId(), topic, partition, key, payload)));
    return switch (response) {
      case PublishResp resp -> new PublishAck(partition, resp.offset());
      case ErrorResp err -> throw new ProtocolException("Publish failed: " + err.message());
      default -> throw new ProtocolException("Unexpected response type: " + response.type());
    };
  }

  private int partitionCountFor(String topic) throws IOException {
    return clusterClient != null
        ? clusterClient.partitionCountFor(topic)
        : metadataClient.partitionCountFor(topic);
  }

  private Message send(String topic, int partition, RedirectingCall.Request request)
      throws IOException {
    if (clusterClient != null) {
      return new RedirectingCall(clusterClient, topic, partition, maxRetries, retryBackoffMs)
          .send(request);
    }
    return request.send(connection);
  }

  /** The partition a keyed {@link #publish(String, byte[], byte[])} landed on, and its offset. */
  public record PublishAck(int partition, long offset) {}
}
