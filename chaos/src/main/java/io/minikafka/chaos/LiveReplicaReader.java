package io.minikafka.chaos;

import io.minikafka.chaos.check.ReplicaReader;
import io.minikafka.client.BrokerConnection;
import io.minikafka.protocol.ErrorResp;
import io.minikafka.protocol.Message;
import io.minikafka.protocol.PollReq;
import io.minikafka.protocol.PollResp;
import io.minikafka.protocol.ProtocolException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * A {@link ReplicaReader} backed by a single raw {@link BrokerConnection} to one specific broker.
 * Unlike {@code ClusterClient}, this never redirects to the leader — every {@code POLL} lands on
 * exactly this broker, reading its own locally-applied (committed-only) log.
 */
public final class LiveReplicaReader implements ReplicaReader {

  private final int brokerId;
  private final BrokerConnection connection;

  public LiveReplicaReader(int brokerId, BrokerConnection connection) {
    this.brokerId = brokerId;
    this.connection = connection;
  }

  @Override
  public int brokerId() {
    return brokerId;
  }

  @Override
  public List<Entry> read(String topic, int partition, long fromOffset, int maxBytes)
      throws IOException {
    Message response =
        connection.request(
            new PollReq(connection.nextCorrelationId(), topic, partition, fromOffset));
    return switch (response) {
      case PollResp resp -> toEntries(resp);
      case ErrorResp err when err.errorCode() == ErrorResp.CODE_OFFSET_OUT_OF_RANGE -> List.of();
      case ErrorResp err ->
          throw new ProtocolException("Poll failed on broker " + brokerId + ": " + err.message());
      default -> throw new ProtocolException("Unexpected response type: " + response.type());
    };
  }

  private static List<Entry> toEntries(PollResp resp) {
    List<Entry> entries = new ArrayList<>(resp.records().size());
    for (PollResp.Record record : resp.records()) {
      entries.add(new Entry(record.offset(), record.payload()));
    }
    return entries;
  }
}
