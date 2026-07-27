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

  /** The requested partition id is outside the topic's configured partition count. */
  public static final int CODE_UNKNOWN_PARTITION = 4;

  /** This broker is not the Raft leader for the requested partition. */
  public static final int CODE_NOT_LEADER = 5;

  /**
   * The requesting broker's leader epoch (Raft term) is below this replica's current term — it is a
   * deposed leader and is fenced.
   */
  public static final int CODE_STALE_LEADER_EPOCH = 6;

  /**
   * The broker rejected a publish because its sequence number skipped ahead of {@code lastCommitted
   * + 1}. The message was <b>not</b> appended and the producer must not treat it as delivered.
   */
  public static final int CODE_SEQUENCE_GAP = 7;

  /**
   * The broker's per-partition publish admission gate is saturated; the message was <b>not</b>
   * appended. The producer should back off and retry.
   */
  public static final int CODE_BROKER_BUSY = 8;

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
