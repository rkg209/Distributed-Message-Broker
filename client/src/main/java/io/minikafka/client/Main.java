package io.minikafka.client;

import io.minikafka.protocol.BrokerInfo;
import io.minikafka.protocol.Message;
import io.minikafka.protocol.MetadataReq;
import io.minikafka.protocol.MetadataResp;
import io.minikafka.protocol.ProtocolConfig;

/**
 * Manual smoke-test CLI: connects to a broker, sends a {@code METADATA_REQ}, and prints the
 * response. Not a real producer/consumer client — those arrive in Spec 02+.
 *
 * <p>Usage: {@code Main <host> <port>}
 */
public final class Main {

  public static void main(String[] args) throws Exception {
    if (args.length != 2) {
      System.err.println("Usage: Main <host> <port>");
      System.exit(1);
    }
    String host = args[0];
    int port = Integer.parseInt(args[1]);

    try (BrokerConnection conn =
        new BrokerConnection(host, port, ProtocolConfig.DEFAULT_MAX_FRAME_BYTES)) {
      Message response = conn.request(new MetadataReq(conn.nextCorrelationId()));
      if (response instanceof MetadataResp resp) {
        System.out.println("METADATA_RESP correlationId=" + resp.correlationId());
        for (BrokerInfo b : resp.brokers()) {
          System.out.println("  broker " + b.brokerId() + " at " + b.host() + ":" + b.port());
        }
      } else {
        System.out.println("Unexpected response: " + response);
      }
    }
  }

  private Main() {}
}
