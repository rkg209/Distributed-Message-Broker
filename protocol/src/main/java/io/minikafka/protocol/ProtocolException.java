package io.minikafka.protocol;

import java.io.IOException;

/**
 * Thrown when a frame or message cannot be parsed: bad length, unknown type byte, truncated
 * payload, or malformed field encoding. Never swallowed — the connection handler must translate
 * this into an {@code ERROR_RESP} and must not crash the server.
 */
public class ProtocolException extends IOException {

  public ProtocolException(String message) {
    super(message);
  }

  public ProtocolException(String message, Throwable cause) {
    super(message, cause);
  }
}
