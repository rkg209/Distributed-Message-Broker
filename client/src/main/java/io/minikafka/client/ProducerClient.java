package io.minikafka.client;

import io.minikafka.protocol.ErrorResp;
import io.minikafka.protocol.Message;
import io.minikafka.protocol.ProtocolException;
import io.minikafka.protocol.PublishReq;
import io.minikafka.protocol.PublishResp;
import java.io.IOException;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Publishes records to a topic partition, either over a single {@link BrokerConnection} (Specs
 * 02–07: the connected broker must already be the leader) or over a {@link ClusterClient}, which
 * resolves the current leader and transparently redirects/retries across a failover.
 *
 * <p>Each instance carries a client-generated {@code producerId} and allocates a per-{@code (topic,
 * partition)} sequence number for every publish, once, <b>outside</b> the {@link RedirectingCall}
 * retry loop — so every attempt of a retried publish (whether the retry is triggered by {@code
 * CODE_NOT_LEADER} or a bare {@link IOException}) carries the same {@code (producerId, seqNo)} and
 * the broker's {@code IdempotencyStore} can recognize it as the same logical message. The sequence
 * advances only once the broker confirms the publish with a {@link PublishResp}; on any thrown
 * failure it is retained so the caller's retry of that same message reuses it.
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
  private final long producerId;
  private final Map<TopicPartitionKey, Long> nextSeq = new ConcurrentHashMap<>();

  private record TopicPartitionKey(String topic, int partition) {}

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
    this.producerId = newProducerId();
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
    this.producerId = newProducerId();
  }

  /** Pins {@code producerId} instead of generating one — for tests that need a stable id. */
  public ProducerClient(ClusterClient clusterClient, long producerId) {
    this.connection = null;
    this.metadataClient = null;
    this.clusterClient = clusterClient;
    this.router = new PartitionRouter();
    this.maxRetries = DEFAULT_MAX_RETRIES;
    this.retryBackoffMs = DEFAULT_RETRY_BACKOFF_MS;
    this.producerId = producerId;
  }

  private static long newProducerId() {
    return UUID.randomUUID().getMostSignificantBits();
  }

  /** Publishes {@code payload} to {@code topic}/{@code partition}, returning its offset. */
  public long publish(String topic, int partition, byte[] payload) throws IOException {
    long seqNo = allocateSeq(topic, partition);
    Message response =
        send(
            topic,
            partition,
            conn ->
                conn.request(
                    new PublishReq(
                        conn.nextCorrelationId(),
                        topic,
                        partition,
                        producerId,
                        seqNo,
                        null,
                        payload)));
    long offset = handlePublishResponse(response);
    confirmSeq(topic, partition, seqNo);
    return offset;
  }

  /**
   * Publishes {@code payload} to {@code topic}, routing on {@code key} (or round-robin if {@code
   * key} is null) using this topic's partition count.
   */
  public PublishAck publish(String topic, byte[] key, byte[] payload) throws IOException {
    int numPartitions = partitionCountFor(topic);
    int partition = router.route(key, numPartitions);
    long seqNo = allocateSeq(topic, partition);
    Message response =
        send(
            topic,
            partition,
            conn ->
                conn.request(
                    new PublishReq(
                        conn.nextCorrelationId(),
                        topic,
                        partition,
                        producerId,
                        seqNo,
                        key,
                        payload)));
    long offset = handlePublishResponse(response);
    confirmSeq(topic, partition, seqNo);
    return new PublishAck(partition, offset);
  }

  private static long handlePublishResponse(Message response) throws IOException {
    return switch (response) {
      case PublishResp resp -> resp.offset();
      case ErrorResp err when err.errorCode() == ErrorResp.CODE_SEQUENCE_GAP ->
          throw new SequenceGapException("Publish failed: " + err.message());
      case ErrorResp err -> throw new ProtocolException("Publish failed: " + err.message());
      default -> throw new ProtocolException("Unexpected response type: " + response.type());
    };
  }

  private long allocateSeq(String topic, int partition) {
    return nextSeq.getOrDefault(new TopicPartitionKey(topic, partition), 0L);
  }

  private void confirmSeq(String topic, int partition, long seqNo) {
    nextSeq.put(new TopicPartitionKey(topic, partition), seqNo + 1);
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
