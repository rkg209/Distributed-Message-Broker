package io.minikafka.protocol;

/**
 * Every wire message type and its 1-byte type code. The type byte is the second field of every
 * frame (after the 4-byte length). Codes are fixed and must never be reused once assigned.
 */
public enum MessageType {
  PUBLISH_REQ(0x01),
  PUBLISH_RESP(0x02),
  POLL_REQ(0x03),
  POLL_RESP(0x04),
  COMMIT_OFFSET_REQ(0x05),
  COMMIT_OFFSET_RESP(0x06),
  METADATA_REQ(0x07),
  METADATA_RESP(0x08),
  FETCH_OFFSET_REQ(0x09),
  FETCH_OFFSET_RESP(0x0A),
  APPEND_ENTRIES_REQ(0x10),
  APPEND_ENTRIES_RESP(0x11),
  REQUEST_VOTE_REQ(0x12),
  REQUEST_VOTE_RESP(0x13),
  HEARTBEAT_REQ(0x14),
  HEARTBEAT_RESP(0x15),
  ERROR_RESP(0xFF);

  private static final MessageType[] BY_CODE = new MessageType[256];

  static {
    for (MessageType t : values()) {
      BY_CODE[t.code & 0xFF] = t;
    }
  }

  private final byte code;

  MessageType(int code) {
    this.code = (byte) code;
  }

  /** The 1-byte type code as written on the wire. */
  public byte code() {
    return code;
  }

  /**
   * Resolves a type byte to its {@link MessageType}.
   *
   * @throws ProtocolException if the byte does not map to a known type
   */
  public static MessageType fromCode(byte code) throws ProtocolException {
    MessageType t = BY_CODE[code & 0xFF];
    if (t == null) {
      throw new ProtocolException(String.format("Unknown message type byte: 0x%02X", code & 0xFF));
    }
    return t;
  }
}
