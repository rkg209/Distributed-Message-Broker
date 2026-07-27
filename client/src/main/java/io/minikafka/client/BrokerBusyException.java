package io.minikafka.client;

import java.io.IOException;

/**
 * Thrown when a publish's busy-retry loop exhausts its attempts after repeated {@link
 * io.minikafka.protocol.ErrorResp#CODE_BROKER_BUSY} responses. The message was <b>not</b> appended
 * — the caller must not treat it as delivered.
 */
public final class BrokerBusyException extends IOException {

  public BrokerBusyException(String message) {
    super(message);
  }
}
