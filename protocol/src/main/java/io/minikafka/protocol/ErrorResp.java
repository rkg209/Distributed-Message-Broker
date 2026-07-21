package io.minikafka.protocol;

/**
 * Broker → Client: signals a malformed request or a request the (stubbed) server cannot yet handle.
 * A {@code correlationId} of {@link #NO_CORRELATION} is used when the id could not be recovered
 * (e.g. the frame was too corrupt to decode).
 */
public record ErrorResp(long correlationId, int errorCode, String message) implements Message {

  /** Used when the request's correlation id could not be recovered from a malformed frame. */
  public static final long NO_CORRELATION = -1L;

  /** The frame or payload could not be parsed. */
  public static final int CODE_PROTOCOL_ERROR = 1;

  /** The request type is valid but not handled by this (stub) server. */
  public static final int CODE_UNSUPPORTED = 2;

  /** The requested offset is below the partition's retained range (deleted by retention). */
  public static final int CODE_OFFSET_OUT_OF_RANGE = 3;

  public ErrorResp {
    if (message == null) {
      throw new IllegalArgumentException("message must not be null");
    }
  }

  @Override
  public MessageType type() {
    return MessageType.ERROR_RESP;
  }
}
