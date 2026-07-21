package io.minikafka.client;

import io.minikafka.protocol.BrokerInfo;
import io.minikafka.protocol.ErrorResp;
import io.minikafka.protocol.Message;
import io.minikafka.protocol.MetadataReq;
import io.minikafka.protocol.MetadataResp;
import io.minikafka.protocol.ProtocolException;
import java.io.IOException;
import java.util.List;

/** Fetches cluster metadata (the broker list) over a {@link BrokerConnection}. */
public final class MetadataClient {

  private final BrokerConnection connection;
  private List<BrokerInfo> cachedBrokers = List.of();

  public MetadataClient(BrokerConnection connection) {
    this.connection = connection;
  }

  /** Fetches the current broker list from the connected broker. */
  public List<BrokerInfo> fetchMetadata() throws IOException {
    MetadataReq req = new MetadataReq(connection.nextCorrelationId());
    Message response = connection.request(req);
    return switch (response) {
      case MetadataResp resp -> {
        cachedBrokers = resp.brokers();
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
}
