package io.minikafka.client;

import io.minikafka.protocol.ErrorResp;
import io.minikafka.protocol.Message;
import io.minikafka.protocol.PollReq;
import io.minikafka.protocol.PollResp;
import io.minikafka.protocol.ProtocolException;
import java.io.IOException;
import java.util.List;

/** Polls records from one topic partition, tracking the next offset to read from. */
public final class ConsumerClient {

  private final BrokerConnection connection;
  private final String topic;
  private final int partition;
  private long currentOffset;

  public ConsumerClient(
      BrokerConnection connection, String topic, int partition, long startOffset) {
    this.connection = connection;
    this.topic = topic;
    this.partition = partition;
    this.currentOffset = startOffset;
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

  /** The offset the next {@link #poll()} will start reading from. */
  public long currentOffset() {
    return currentOffset;
  }
}
