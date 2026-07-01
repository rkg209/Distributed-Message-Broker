package io.minikafka.broker;

import io.minikafka.protocol.BrokerInfo;
import io.minikafka.protocol.ErrorResp;
import io.minikafka.protocol.Message;
import io.minikafka.protocol.MetadataReq;
import io.minikafka.protocol.MetadataResp;
import java.util.List;

/**
 * Spec 01 placeholder handler: answers {@code METADATA_REQ} with this broker's own coordinates and
 * rejects everything else with an {@code ERROR_RESP}. It exists purely to prove the wire path works
 * end to end; later specs replace it with real request handling.
 */
public final class StubRequestHandler implements RequestHandler {

  private final BrokerInfo self;

  public StubRequestHandler(BrokerInfo self) {
    this.self = self;
  }

  @Override
  public Message handle(Message request) {
    if (request instanceof MetadataReq req) {
      return new MetadataResp(req.correlationId(), List.of(self));
    }
    return new ErrorResp(
        request.correlationId(),
        ErrorResp.CODE_UNSUPPORTED,
        "Request type not supported by stub handler: " + request.type());
  }
}
