package io.minikafka.broker;

import io.minikafka.log.LogRecord;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;

/**
 * Encodes/decodes the opaque {@code byte[] command} carried by a Raft entry for a partition
 * publish: {@code [8-byte timestamp][8-byte producerId][8-byte seqNo][4-byte keyLen or
 * -1][key][4-byte valueLen][value]}, big-endian. The timestamp is stamped by the leader at propose
 * time and replicated as part of the command, so every replica's {@link
 * io.minikafka.log.PartitionLog#append} sees an identical record — required for the three logs to
 * be byte-identical.
 */
final class PartitionCommandCodec {

  private PartitionCommandCodec() {}

  static byte[] encode(long timestamp, long producerId, long seqNo, byte[] key, byte[] payload) {
    ByteArrayOutputStream buffer = new ByteArrayOutputStream();
    try (DataOutputStream out = new DataOutputStream(buffer)) {
      out.writeLong(timestamp);
      out.writeLong(producerId);
      out.writeLong(seqNo);
      if (key == null) {
        out.writeInt(-1);
      } else {
        out.writeInt(key.length);
        out.write(key);
      }
      out.writeInt(payload.length);
      out.write(payload);
    } catch (IOException e) {
      throw new UncheckedIOException("Failed to encode partition command", e);
    }
    return buffer.toByteArray();
  }

  /**
   * Decodes {@code command} into a {@link LogRecord} with {@code offset=0} — assigned at apply
   * time.
   */
  static LogRecord decode(byte[] command) {
    DataInputStream in = new DataInputStream(new ByteArrayInputStream(command));
    try {
      long timestamp = in.readLong();
      long producerId = in.readLong();
      long seqNo = in.readLong();
      int keyLen = in.readInt();
      byte[] key = null;
      if (keyLen >= 0) {
        key = new byte[keyLen];
        in.readFully(key);
      }
      int valueLen = in.readInt();
      byte[] value = new byte[valueLen];
      in.readFully(value);
      return new LogRecord(0, timestamp, producerId, seqNo, key, value);
    } catch (IOException e) {
      throw new UncheckedIOException("Failed to decode partition command", e);
    }
  }
}
