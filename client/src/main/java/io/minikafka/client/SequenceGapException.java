package io.minikafka.client;

import io.minikafka.protocol.ProtocolException;

/**
 * Thrown when the broker rejects a publish with {@link
 * io.minikafka.protocol.ErrorResp#CODE_SEQUENCE_GAP}: this producer's sequence number skipped ahead
 * of the broker's last committed sequence. The message was <b>not</b> appended — the caller must
 * not treat it as delivered nor silently drop it.
 */
public final class SequenceGapException extends ProtocolException {

  public SequenceGapException(String message) {
    super(message);
  }
}
