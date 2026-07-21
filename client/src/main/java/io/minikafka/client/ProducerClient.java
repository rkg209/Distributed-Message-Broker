package io.minikafka.client;

import io.minikafka.protocol.ErrorResp;
import io.minikafka.protocol.Message;
import io.minikafka.protocol.ProtocolException;
import io.minikafka.protocol.PublishReq;
import io.minikafka.protocol.PublishResp;
import java.io.IOException;

/** Publishes records to a topic partition over a {@link BrokerConnection}. */
public final class ProducerClient {

  private final BrokerConnection connection;
  private final MetadataClient metadataClient;
  private final PartitionRouter router;

  public ProducerClient(BrokerConnection connection) {
    this(connection, new MetadataClient(connection), new PartitionRouter());
  }

  public ProducerClient(
      BrokerConnection connection, MetadataClient metadataClient, PartitionRouter router) {
    this.connection = connection;
    this.metadataClient = metadataClient;
    this.router = router;
  }

  /** Publishes {@code payload} to {@code topic}/{@code partition}, returning its offset. */
  public long publish(String topic, int partition, byte[] payload) throws IOException {
    PublishReq req =
        new PublishReq(connection.nextCorrelationId(), topic, partition, null, payload);
    Message response = connection.request(req);
    return switch (response) {
      case PublishResp resp -> resp.offset();
      case ErrorResp err -> throw new ProtocolException("Publish failed: " + err.message());
      default -> throw new ProtocolException("Unexpected response type: " + response.type());
    };
  }

  /**
   * Publishes {@code payload} to {@code topic}, routing on {@code key} (or round-robin if {@code
   * key} is null) using this topic's partition count from {@link MetadataClient}.
   */
  public PublishAck publish(String topic, byte[] key, byte[] payload) throws IOException {
    int numPartitions = metadataClient.partitionCountFor(topic);
    int partition = router.route(key, numPartitions);
    PublishReq req = new PublishReq(connection.nextCorrelationId(), topic, partition, key, payload);
    Message response = connection.request(req);
    return switch (response) {
      case PublishResp resp -> new PublishAck(partition, resp.offset());
      case ErrorResp err -> throw new ProtocolException("Publish failed: " + err.message());
      default -> throw new ProtocolException("Unexpected response type: " + response.type());
    };
  }

  /** The partition a keyed {@link #publish(String, byte[], byte[])} landed on, and its offset. */
  public record PublishAck(int partition, long offset) {}
}
