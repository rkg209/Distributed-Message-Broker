package io.minikafka.client;

import io.minikafka.protocol.CommitOffsetReq;
import io.minikafka.protocol.CommitOffsetResp;
import io.minikafka.protocol.ErrorResp;
import io.minikafka.protocol.FetchOffsetReq;
import io.minikafka.protocol.FetchOffsetResp;
import io.minikafka.protocol.Message;
import io.minikafka.protocol.PollReq;
import io.minikafka.protocol.PollResp;
import io.minikafka.protocol.ProtocolException;
import java.io.IOException;
import java.util.List;

/**
 * Polls records from one topic partition, tracking the next offset to read from. With a group id,
 * resumes from the group's last committed offset on construction and can durably commit its current
 * offset via {@link #commitOffset()}. Consumers are one per partition; multi-partition consumption
 * is a {@link StaticAssignor} list plus one {@link ConsumerClient} each.
 *
 * <p>Constructed over a {@link ClusterClient} instead of a single {@link BrokerConnection}, every
 * RPC redirects to and retries against the current partition leader on {@code CODE_NOT_LEADER} or a
 * broken socket. {@link #currentOffset} only ever advances on a successful {@link PollResp}, so a
 * retry after redirect resumes from the same offset — no gap, no skip.
 */
public final class ConsumerClient {

  private final BrokerConnection connection;
  private final ClusterClient clusterClient;
  private final int maxRetries;
  private final long retryBackoffMs;
  private final String topic;
  private final int partition;
  private final String group;
  private long currentOffset;

  public ConsumerClient(
      BrokerConnection connection, String topic, int partition, long startOffset) {
    this.connection = connection;
    this.clusterClient = null;
    this.maxRetries = 0;
    this.retryBackoffMs = 0;
    this.topic = topic;
    this.partition = partition;
    this.group = null;
    this.currentOffset = startOffset;
  }

  /**
   * Joins consumer group {@code group}: on construction, fetches the group's last committed offset
   * for this topic partition and resumes there ({@link FetchOffsetResp#NO_OFFSET} starts at 0).
   */
  public ConsumerClient(BrokerConnection connection, String topic, int partition, String group)
      throws IOException {
    this.connection = connection;
    this.clusterClient = null;
    this.maxRetries = 0;
    this.retryBackoffMs = 0;
    this.topic = topic;
    this.partition = partition;
    this.group = group;
    this.currentOffset = fetchCommittedOffset();
  }

  /**
   * A redirect-aware, groupless consumer over {@code clusterClient} starting at {@code
   * startOffset}.
   */
  public ConsumerClient(
      ClusterClient clusterClient,
      String topic,
      int partition,
      long startOffset,
      int maxRetries,
      long retryBackoffMs) {
    this.connection = null;
    this.clusterClient = clusterClient;
    this.maxRetries = maxRetries;
    this.retryBackoffMs = retryBackoffMs;
    this.topic = topic;
    this.partition = partition;
    this.group = null;
    this.currentOffset = startOffset;
  }

  /** A redirect-aware consumer over {@code clusterClient}, using default retry tunables. */
  public ConsumerClient(ClusterClient clusterClient, String topic, int partition, String group)
      throws IOException {
    this(
        clusterClient,
        topic,
        partition,
        group,
        ProducerClient.DEFAULT_MAX_RETRIES,
        ProducerClient.DEFAULT_RETRY_BACKOFF_MS);
  }

  public ConsumerClient(
      ClusterClient clusterClient,
      String topic,
      int partition,
      String group,
      int maxRetries,
      long retryBackoffMs)
      throws IOException {
    this.connection = null;
    this.clusterClient = clusterClient;
    this.maxRetries = maxRetries;
    this.retryBackoffMs = retryBackoffMs;
    this.topic = topic;
    this.partition = partition;
    this.group = group;
    this.currentOffset = fetchCommittedOffset();
  }

  /**
   * Polls the next batch starting at {@link #currentOffset()}. On a non-empty batch, advances the
   * offset past the last record returned; an empty batch leaves the offset unchanged.
   */
  public List<PollResp.Record> poll() throws IOException {
    Message response =
        send(
            conn ->
                conn.request(
                    new PollReq(conn.nextCorrelationId(), topic, partition, currentOffset)));
    return switch (response) {
      case PollResp resp -> {
        List<PollResp.Record> records = resp.records();
        if (!records.isEmpty()) {
          currentOffset = records.get(records.size() - 1).offset() + 1;
        }
        yield records;
      }
      case ErrorResp err -> throw new ProtocolException("Poll failed: " + err.message());
      default -> throw new ProtocolException("Unexpected response type: " + response.type());
    };
  }

  /** Durably commits {@link #currentOffset()} for this group/topic/partition. */
  public void commitOffset() throws IOException {
    if (group == null) {
      throw new IllegalStateException("commitOffset() requires a consumer group");
    }
    Message response =
        send(
            conn ->
                conn.request(
                    new CommitOffsetReq(
                        conn.nextCorrelationId(), group, topic, partition, currentOffset)));
    switch (response) {
      case CommitOffsetResp resp -> {
        if (!resp.ok()) {
          throw new ProtocolException("Commit offset rejected for group " + group);
        }
      }
      case ErrorResp err -> throw new ProtocolException("Commit offset failed: " + err.message());
      default -> throw new ProtocolException("Unexpected response type: " + response.type());
    }
  }

  /** The offset the next {@link #poll()} will start reading from. */
  public long currentOffset() {
    return currentOffset;
  }

  private long fetchCommittedOffset() throws IOException {
    Message response =
        send(
            conn ->
                conn.request(
                    new FetchOffsetReq(conn.nextCorrelationId(), group, topic, partition)));
    return switch (response) {
      case FetchOffsetResp resp -> resp.offset() == FetchOffsetResp.NO_OFFSET ? 0 : resp.offset();
      case ErrorResp err -> throw new ProtocolException("Fetch offset failed: " + err.message());
      default -> throw new ProtocolException("Unexpected response type: " + response.type());
    };
  }

  private Message send(RedirectingCall.Request request) throws IOException {
    if (clusterClient != null) {
      return new RedirectingCall(clusterClient, topic, partition, maxRetries, retryBackoffMs)
          .send(request);
    }
    return request.send(connection);
  }
}
