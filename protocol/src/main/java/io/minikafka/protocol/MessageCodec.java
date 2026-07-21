package io.minikafka.protocol;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

/**
 * Hand-written binary serialization for every {@link Message} type. This is the only place field
 * layout lives. All integers are big-endian (guaranteed by {@link DataOutputStream}); strings and
 * byte arrays are length-prefixed with a big-endian int32.
 *
 * <p>No JSON, Protobuf, or reflection — the custom binary format is an explicit portfolio signal.
 */
public final class MessageCodec {

  /** Serializes a message into a type-tagged {@link Frame}. */
  public Frame encode(Message message) {
    ByteArrayOutputStream buffer = new ByteArrayOutputStream();
    try (DataOutputStream out = new DataOutputStream(buffer)) {
      switch (message) {
        case PublishReq m -> {
          out.writeLong(m.correlationId());
          writeString(out, m.topic());
          out.writeInt(m.partition());
          writeBytes(out, m.payload());
        }
        case PublishResp m -> {
          out.writeLong(m.correlationId());
          out.writeLong(m.offset());
        }
        case PollReq m -> {
          out.writeLong(m.correlationId());
          writeString(out, m.topic());
          out.writeInt(m.partition());
          out.writeLong(m.offset());
        }
        case PollResp m -> {
          out.writeLong(m.correlationId());
          out.writeInt(m.records().size());
          for (PollResp.Record r : m.records()) {
            out.writeLong(r.offset());
            writeBytes(out, r.payload());
          }
        }
        case CommitOffsetReq m -> {
          out.writeLong(m.correlationId());
          writeString(out, m.group());
          writeString(out, m.topic());
          out.writeInt(m.partition());
          out.writeLong(m.offset());
        }
        case CommitOffsetResp m -> {
          out.writeLong(m.correlationId());
          out.writeBoolean(m.ok());
        }
        case MetadataReq m -> out.writeLong(m.correlationId());
        case MetadataResp m -> {
          out.writeLong(m.correlationId());
          out.writeInt(m.brokers().size());
          for (BrokerInfo b : m.brokers()) {
            out.writeInt(b.brokerId());
            writeString(out, b.host());
            out.writeInt(b.port());
          }
        }
        case AppendEntriesReq m -> {
          out.writeLong(m.correlationId());
          out.writeLong(m.term());
          out.writeInt(m.leaderId());
        }
        case AppendEntriesResp m -> {
          out.writeLong(m.correlationId());
          out.writeLong(m.term());
          out.writeBoolean(m.success());
        }
        case RequestVoteReq m -> {
          out.writeLong(m.correlationId());
          out.writeLong(m.term());
          out.writeInt(m.candidateId());
        }
        case RequestVoteResp m -> {
          out.writeLong(m.correlationId());
          out.writeLong(m.term());
          out.writeBoolean(m.voteGranted());
        }
        case HeartbeatReq m -> {
          out.writeLong(m.correlationId());
          out.writeLong(m.term());
          out.writeInt(m.leaderId());
        }
        case HeartbeatResp m -> {
          out.writeLong(m.correlationId());
          out.writeLong(m.term());
        }
        case ErrorResp m -> {
          out.writeLong(m.correlationId());
          out.writeInt(m.errorCode());
          writeString(out, m.message());
        }
      }
    } catch (IOException e) {
      // ByteArrayOutputStream never performs real I/O, so this is unreachable.
      throw new UncheckedIOException("Failed to encode " + message.type(), e);
    }
    return new Frame(message.type(), buffer.toByteArray());
  }

  /**
   * Decodes a {@link Frame} into a typed {@link Message}.
   *
   * @throws ProtocolException if the payload is truncated, over-long, or otherwise malformed
   */
  public Message decode(Frame frame) throws ProtocolException {
    DataInputStream in = new DataInputStream(new ByteArrayInputStream(frame.payload()));
    try {
      Message message =
          switch (frame.type()) {
            case PUBLISH_REQ ->
                new PublishReq(in.readLong(), readString(in), in.readInt(), readBytes(in));
            case PUBLISH_RESP -> new PublishResp(in.readLong(), in.readLong());
            case POLL_REQ ->
                new PollReq(in.readLong(), readString(in), in.readInt(), in.readLong());
            case POLL_RESP -> decodePollResp(in);
            case COMMIT_OFFSET_REQ ->
                new CommitOffsetReq(
                    in.readLong(), readString(in), readString(in), in.readInt(), in.readLong());
            case COMMIT_OFFSET_RESP -> new CommitOffsetResp(in.readLong(), in.readBoolean());
            case METADATA_REQ -> new MetadataReq(in.readLong());
            case METADATA_RESP -> decodeMetadataResp(in);
            case APPEND_ENTRIES_REQ ->
                new AppendEntriesReq(in.readLong(), in.readLong(), in.readInt());
            case APPEND_ENTRIES_RESP ->
                new AppendEntriesResp(in.readLong(), in.readLong(), in.readBoolean());
            case REQUEST_VOTE_REQ -> new RequestVoteReq(in.readLong(), in.readLong(), in.readInt());
            case REQUEST_VOTE_RESP ->
                new RequestVoteResp(in.readLong(), in.readLong(), in.readBoolean());
            case HEARTBEAT_REQ -> new HeartbeatReq(in.readLong(), in.readLong(), in.readInt());
            case HEARTBEAT_RESP -> new HeartbeatResp(in.readLong(), in.readLong());
            case ERROR_RESP -> new ErrorResp(in.readLong(), in.readInt(), readString(in));
          };
      if (in.read() != -1) {
        throw new ProtocolException("Trailing bytes after " + frame.type() + " payload");
      }
      return message;
    } catch (EOFException e) {
      throw new ProtocolException("Truncated " + frame.type() + " payload", e);
    } catch (IllegalArgumentException e) {
      throw new ProtocolException("Invalid " + frame.type() + " field: " + e.getMessage(), e);
    } catch (IOException e) {
      throw new ProtocolException("Failed to decode " + frame.type(), e);
    }
  }

  private static MetadataResp decodeMetadataResp(DataInputStream in) throws IOException {
    long correlationId = in.readLong();
    int count = in.readInt();
    if (count < 0) {
      throw new ProtocolException("Negative broker count: " + count);
    }
    List<BrokerInfo> brokers = new ArrayList<>(Math.min(count, 1024));
    for (int i = 0; i < count; i++) {
      int brokerId = in.readInt();
      String host = readString(in);
      int port = in.readInt();
      brokers.add(new BrokerInfo(brokerId, host, port));
    }
    return new MetadataResp(correlationId, brokers);
  }

  private static PollResp decodePollResp(DataInputStream in) throws IOException {
    long correlationId = in.readLong();
    int count = in.readInt();
    if (count < 0) {
      throw new ProtocolException("Negative record count: " + count);
    }
    List<PollResp.Record> records = new ArrayList<>(Math.min(count, 1024));
    for (int i = 0; i < count; i++) {
      records.add(new PollResp.Record(in.readLong(), readBytes(in)));
    }
    return new PollResp(correlationId, records);
  }

  private static void writeString(DataOutputStream out, String value) throws IOException {
    byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
    out.writeInt(bytes.length);
    out.write(bytes);
  }

  private static String readString(DataInputStream in) throws IOException {
    int length = in.readInt();
    return new String(readN(in, length), StandardCharsets.UTF_8);
  }

  private static void writeBytes(DataOutputStream out, byte[] value) throws IOException {
    out.writeInt(value.length);
    out.write(value);
  }

  private static byte[] readBytes(DataInputStream in) throws IOException {
    return readN(in, in.readInt());
  }

  private static byte[] readN(DataInputStream in, int length) throws IOException {
    if (length < 0) {
      throw new ProtocolException("Negative length prefix: " + length);
    }
    // The frame is already bounded by FrameDecoder's maxFrameBytes, so available() is an exact,
    // small upper bound here. Rejecting an over-long prefix before allocating prevents a single
    // bad length field from forcing a huge buffer allocation.
    if (length > in.available()) {
      throw new ProtocolException(
          "Length prefix " + length + " exceeds remaining payload " + in.available());
    }
    byte[] bytes = new byte[length];
    in.readFully(bytes);
    return bytes;
  }

  private MessageCodec() {
    // Codec is stateless; use the static instance.
  }

  private static final MessageCodec INSTANCE = new MessageCodec();

  /** Shared stateless codec instance. */
  public static MessageCodec instance() {
    return INSTANCE;
  }
}
