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
 */
public final class ConsumerClient {

  private final BrokerConnection connection;
  private final String topic;
  private final int partition;
  private final String group;
  private long currentOffset;

  public ConsumerClient(
      BrokerConnection connection, String topic, int partition, long startOffset) {
    this.connection = connection;
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
    PollReq req = new PollReq(connection.nextCorrelationId(), topic, partition, currentOffset);
    Message response = connection.request(req);
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
    CommitOffsetReq req =
        new CommitOffsetReq(connection.nextCorrelationId(), group, topic, partition, currentOffset);
    Message response = connection.request(req);
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
    FetchOffsetReq req =
        new FetchOffsetReq(connection.nextCorrelationId(), group, topic, partition);
    Message response = connection.request(req);
    return switch (response) {
      case FetchOffsetResp resp -> resp.offset() == FetchOffsetResp.NO_OFFSET ? 0 : resp.offset();
      case ErrorResp err -> throw new ProtocolException("Fetch offset failed: " + err.message());
      default -> throw new ProtocolException("Unexpected response type: " + response.type());
    };
  }
}
