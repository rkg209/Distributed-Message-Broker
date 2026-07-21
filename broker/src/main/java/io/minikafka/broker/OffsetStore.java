package io.minikafka.broker;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.HashMap;
import java.util.Map;
import java.util.zip.CRC32;

/**
 * A durable, append-only committed-offset log for one consumer group: {@code
 * {offsetDir}/{groupId}.offsets}. Record layout: {@code [partitionId 4B][topicLen 2B][topic
 * UTF-8][committedOffset 8B][timestamp 8B][CRC32 4B]}, big-endian. Every append is followed by
 * {@code force(true)} — durability is EVERY_WRITE, no exceptions.
 *
 * <p>On construction, the file is scanned sequentially, keeping the last value per {@code (topic,
 * partition)}. A CRC failure or a short trailing record is the expected shape of a crash mid-append
 * (this file has no "already-rolled, must-be-intact" segments the way {@code log/} does) — the scan
 * stops there and the file is truncated at the last valid record boundary, mirroring {@code
 * log/.../LogRecovery}.
 */
final class OffsetStore implements AutoCloseable {

  private static final int PARTITION_BYTES = 4;
  private static final int TOPIC_LEN_BYTES = 2;
  private static final int OFFSET_BYTES = 8;
  private static final int TIMESTAMP_BYTES = 8;
  private static final int CRC_BYTES = 4;

  private final FileChannel channel;
  private final Map<TopicPartition, Long> recoveredOffsets;

  OffsetStore(Path dir, String groupId) throws IOException {
    validateGroupId(groupId);
    Files.createDirectories(dir);
    Path file = dir.resolve(groupId + ".offsets");
    this.recoveredOffsets = recoverAndTruncate(file);
    this.channel = FileChannel.open(file, StandardOpenOption.CREATE, StandardOpenOption.APPEND);
  }

  /** The {@code (topic, partition) -> offset} state recovered from disk at construction time. */
  Map<TopicPartition, Long> recoveredOffsets() {
    return recoveredOffsets;
  }

  /** Appends and fsyncs a committed-offset record. Callers must serialize calls per group. */
  synchronized void append(String topic, int partition, long offset) throws IOException {
    byte[] record = encode(partition, topic, offset, System.currentTimeMillis());
    ByteBuffer buf = ByteBuffer.wrap(record);
    while (buf.hasRemaining()) {
      channel.write(buf);
    }
    channel.force(true);
  }

  @Override
  public void close() throws IOException {
    channel.close();
  }

  private static byte[] encode(int partition, String topic, long offset, long timestamp) {
    byte[] topicBytes = topic.getBytes(StandardCharsets.UTF_8);
    int size =
        PARTITION_BYTES
            + TOPIC_LEN_BYTES
            + topicBytes.length
            + OFFSET_BYTES
            + TIMESTAMP_BYTES
            + CRC_BYTES;
    ByteBuffer buf = ByteBuffer.allocate(size);
    buf.putInt(partition);
    buf.putShort((short) topicBytes.length);
    buf.put(topicBytes);
    buf.putLong(offset);
    buf.putLong(timestamp);

    ByteBuffer forCrc = buf.duplicate().flip();
    CRC32 crc = new CRC32();
    crc.update(forCrc);
    buf.putInt((int) crc.getValue());
    return buf.array();
  }

  private static Map<TopicPartition, Long> recoverAndTruncate(Path file) throws IOException {
    Map<TopicPartition, Long> offsets = new HashMap<>();
    if (!Files.exists(file)) {
      return offsets;
    }
    byte[] content = Files.readAllBytes(file);
    ByteBuffer buf = ByteBuffer.wrap(content);
    long validEnd = 0;
    while (buf.hasRemaining()) {
      Entry entry;
      try {
        entry = decodeEntry(buf);
      } catch (CorruptEntryException e) {
        break;
      }
      if (entry == null) {
        break;
      }
      offsets.put(new TopicPartition(entry.topic(), entry.partition()), entry.offset());
      validEnd = buf.position();
    }
    if (validEnd < content.length) {
      try (FileChannel truncateChannel = FileChannel.open(file, StandardOpenOption.WRITE)) {
        truncateChannel.truncate(validEnd);
      }
    }
    return offsets;
  }

  private record Entry(int partition, String topic, long offset) {}

  private static final class CorruptEntryException extends RuntimeException {}

  private static Entry decodeEntry(ByteBuffer buf) {
    int start = buf.position();
    if (buf.remaining() < PARTITION_BYTES + TOPIC_LEN_BYTES) {
      return null;
    }
    int partition = buf.getInt();
    short topicLen = buf.getShort();
    if (topicLen < 0 || buf.remaining() < topicLen) {
      buf.position(start);
      return null;
    }
    byte[] topicBytes = new byte[topicLen];
    buf.get(topicBytes);
    if (buf.remaining() < OFFSET_BYTES + TIMESTAMP_BYTES + CRC_BYTES) {
      buf.position(start);
      return null;
    }
    long offset = buf.getLong();
    buf.getLong(); // timestamp, not needed by the in-memory view
    int crcFieldPos = buf.position();
    int expectedCrc = buf.getInt();

    ByteBuffer forCrc = buf.duplicate();
    forCrc.limit(crcFieldPos);
    forCrc.position(start);
    CRC32 crc = new CRC32();
    crc.update(forCrc);
    if ((int) crc.getValue() != expectedCrc) {
      throw new CorruptEntryException();
    }
    return new Entry(partition, new String(topicBytes, StandardCharsets.UTF_8), offset);
  }

  private static void validateGroupId(String groupId) {
    if (groupId == null || groupId.isBlank()) {
      throw new IllegalArgumentException("group id must not be blank");
    }
    if (groupId.contains("/") || groupId.contains("..")) {
      throw new IllegalArgumentException("group id must not contain '/' or '..': " + groupId);
    }
    for (int i = 0; i < groupId.length(); i++) {
      if (Character.isISOControl(groupId.charAt(i))) {
        throw new IllegalArgumentException(
            "group id must not contain control characters: " + groupId);
      }
    }
  }
}
