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

  public ProducerClient(BrokerConnection connection) {
    this.connection = connection;
  }

  /** Publishes {@code payload} to {@code topic}/{@code partition}, returning its offset. */
  public long publish(String topic, int partition, byte[] payload) throws IOException {
    PublishReq req = new PublishReq(connection.nextCorrelationId(), topic, partition, payload);
    Message response = connection.request(req);
    return switch (response) {
      case PublishResp resp -> resp.offset();
      case ErrorResp err -> throw new ProtocolException("Publish failed: " + err.message());
      default -> throw new ProtocolException("Unexpected response type: " + response.type());
    };
  }
}
