package io.minikafka.log;

import java.nio.ByteBuffer;
import java.util.zip.CRC32;

/**
 * Single source of truth for the on-disk {@link LogRecord} layout: {@code [8B offset][8B
 * timestamp][4B keyLen][key][4B valLen][value][4B CRC32]}, big-endian. {@code keyLen == -1} encodes
 * a null key. The CRC covers every preceding byte of the record.
 */
final class RecordCodec {

  private static final int FIXED_HEADER_BYTES = 8 + 8 + 4; // offset + timestamp + keyLen
  private static final int NULL_KEY_LEN = -1;

  private RecordCodec() {}

  /** Thrown by {@link #decode(ByteBuffer)} when a complete record's CRC does not match. */
  static final class CorruptRecordException extends RuntimeException {
    CorruptRecordException(String message) {
      super(message);
    }
  }

  static int sizeOf(LogRecord record) {
    int keyLen = record.key() == null ? 0 : record.key().length;
    return FIXED_HEADER_BYTES + keyLen + 4 + record.value().length + 4;
  }

  static byte[] encode(LogRecord record) {
    byte[] key = record.key();
    byte[] value = record.value();
    int keyLen = key == null ? NULL_KEY_LEN : key.length;

    ByteBuffer buf = ByteBuffer.allocate(sizeOf(record));
    buf.putLong(record.offset());
    buf.putLong(record.timestamp());
    buf.putInt(keyLen);
    if (key != null) {
      buf.put(key);
    }
    buf.putInt(value.length);
    buf.put(value);

    ByteBuffer forCrc = buf.duplicate().flip();
    CRC32 crc = new CRC32();
    crc.update(forCrc);
    buf.putInt((int) crc.getValue());
    return buf.array();
  }

  /**
   * Attempts to decode one record starting at {@code buf}'s current position, advancing the
   * position past it on success. Returns {@code null} (position left unchanged) if fewer bytes
   * remain than a complete record needs — the caller's signal that this is a torn trailing write,
   * not corruption. Throws {@link CorruptRecordException} if a complete record is present but its
   * CRC does not match.
   */
  static LogRecord decode(ByteBuffer buf) {
    int start = buf.position();
    if (buf.remaining() < FIXED_HEADER_BYTES) {
      return null;
    }
    long offset = buf.getLong();
    long timestamp = buf.getLong();
    int keyLen = buf.getInt();
    if (keyLen < NULL_KEY_LEN) {
      buf.position(start);
      return null;
    }
    if (keyLen > 0 && buf.remaining() < keyLen) {
      buf.position(start);
      return null;
    }
    byte[] key = null;
    if (keyLen >= 0) {
      key = new byte[keyLen];
      buf.get(key);
    }
    if (buf.remaining() < 4) {
      buf.position(start);
      return null;
    }
    int valLen = buf.getInt();
    if (valLen < 0 || buf.remaining() < valLen + 4) {
      buf.position(start);
      return null;
    }
    byte[] value = new byte[valLen];
    buf.get(value);

    int crcFieldPos = buf.position();
    int expectedCrc = buf.getInt();

    ByteBuffer forCrc = buf.duplicate();
    forCrc.limit(crcFieldPos);
    forCrc.position(start);
    CRC32 crc = new CRC32();
    crc.update(forCrc);
    if ((int) crc.getValue() != expectedCrc) {
      throw new CorruptRecordException("CRC mismatch for record at offset " + offset);
    }

    return new LogRecord(offset, timestamp, key, value);
  }
}
