package io.minikafka.protocol;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.List;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * AC-4: every message type round-trips through the codec with byte-stable, value-equal fidelity.
 */
class MessageCodecTest {

  private final MessageCodec codec = MessageCodec.instance();

  static Stream<Message> everyMessageType() {
    return Stream.of(
        new PublishReq(1L, "orders", 3, new byte[] {5, 6}, new byte[] {1, 2, 3, 4}),
        new PublishResp(2L, 42L),
        new PollReq(3L, "orders", 3, 100L),
        new PollResp(
            4L,
            List.of(
                new PollResp.Record(100L, new byte[] {9, 8, 7}),
                new PollResp.Record(101L, new byte[] {6}))),
        new CommitOffsetReq(5L, "group-a", "orders", 3, 99L),
        new CommitOffsetResp(6L, true),
        new MetadataReq(7L),
        new MetadataResp(
            8L,
            List.of(new BrokerInfo(1, "broker-1", 9092), new BrokerInfo(2, "broker-2", 9093)),
            List.of(
                new TopicMetadata(
                    "orders",
                    List.of(
                        new PartitionMetadata(0, 1, List.of(1, 2, 3)),
                        new PartitionMetadata(1, 1, List.of()))))),
        new AppendEntriesReq(
            9L,
            "orders",
            3,
            5L,
            1,
            10L,
            4L,
            List.of(
                new AppendEntriesReq.Entry(5L, 11L, new byte[] {1, 2}),
                new AppendEntriesReq.Entry(5L, 12L, new byte[0])),
            10L),
        new AppendEntriesResp(10L, "orders", 3, 5L, true, 0L, 0L, 12L),
        new RequestVoteReq(11L, "orders", 3, 6L, 2, 12L, 5L),
        new RequestVoteResp(12L, "orders", 3, 6L, false),
        new HeartbeatReq(13L, 7L, 1),
        new HeartbeatResp(14L, 7L),
        new ErrorResp(15L, ErrorResp.CODE_PROTOCOL_ERROR, "bad frame"),
        new FetchOffsetReq(16L, "group-a", "orders", 3),
        new FetchOffsetResp(17L, 99L));
  }

  @ParameterizedTest
  @MethodSource("everyMessageType")
  void roundTripsToEqualMessage(Message original) throws ProtocolException {
    Frame frame = codec.encode(original);
    assertEquals(original.type(), frame.type());

    Message decoded = codec.decode(frame);
    assertEquals(original, decoded);
  }

  @ParameterizedTest
  @MethodSource("everyMessageType")
  void encodingIsByteStable(Message original) throws ProtocolException {
    Frame first = codec.encode(original);
    Frame second = codec.encode(codec.decode(first));
    assertArrayEquals(first.payload(), second.payload());
  }

  @Test
  void allTypesAreCovered() {
    assertEquals(MessageType.values().length, everyMessageType().count());
  }

  @Test
  void emptyPayloadRoundTrips() throws ProtocolException {
    PublishReq original = new PublishReq(1L, "t", 0, null, new byte[0]);
    assertEquals(original, codec.decode(codec.encode(original)));
  }

  @Test
  void nullKeyRoundTrips() throws ProtocolException {
    PublishReq original = new PublishReq(1L, "t", 0, null, new byte[] {1});
    Message decoded = codec.decode(codec.encode(original));
    assertEquals(original, decoded);
    assertEquals(null, ((PublishReq) decoded).key());
  }

  @Test
  void metadataRespWithNoBrokersOrTopicsRoundTrips() throws ProtocolException {
    MetadataResp original = new MetadataResp(1L, List.of(), List.of());
    assertEquals(original, codec.decode(codec.encode(original)));
  }

  @Test
  void pollRespWithEmptyBatchRoundTrips() throws ProtocolException {
    PollResp original = new PollResp(1L, List.of());
    assertEquals(original, codec.decode(codec.encode(original)));
  }

  @Test
  void appendEntriesHeartbeatWithEmptyEntriesRoundTrips() throws ProtocolException {
    AppendEntriesReq original = new AppendEntriesReq(1L, "orders", 0, 3L, 1, 5L, 2L, List.of(), 5L);
    assertEquals(original, codec.decode(codec.encode(original)));
  }

  @Test
  void appendEntriesReqWithMultipleEntriesRoundTrips() throws ProtocolException {
    AppendEntriesReq original =
        new AppendEntriesReq(
            1L,
            "orders",
            2,
            3L,
            1,
            5L,
            2L,
            List.of(
                new AppendEntriesReq.Entry(3L, 6L, new byte[] {1, 2, 3}),
                new AppendEntriesReq.Entry(3L, 7L, new byte[] {4}),
                new AppendEntriesReq.Entry(3L, 8L, new byte[0])),
            6L);
    assertEquals(original, codec.decode(codec.encode(original)));
  }

  @Test
  void truncatedPayloadThrowsProtocolException() {
    // A PUBLISH_RESP payload should be 16 bytes (two longs); give it 4.
    Frame truncated = new Frame(MessageType.PUBLISH_RESP, new byte[] {0, 0, 0, 1});
    assertThrows(ProtocolException.class, () -> codec.decode(truncated));
  }

  @Test
  void trailingBytesThrowProtocolException() {
    // METADATA_REQ payload is exactly one long (8 bytes); append a stray byte.
    Frame padded = new Frame(MessageType.METADATA_REQ, new byte[9]);
    assertThrows(ProtocolException.class, () -> codec.decode(padded));
  }

  @Test
  void overLongLengthPrefixThrowsProtocolException() {
    // A PUBLISH_REQ whose topic length prefix (0x7FFFFFFF) far exceeds the payload.
    byte[] payload = new byte[] {0, 0, 0, 0, 0, 0, 0, 0, 0x7F, -1, -1, -1};
    Frame bad = new Frame(MessageType.PUBLISH_REQ, payload);
    assertThrows(ProtocolException.class, () -> codec.decode(bad));
  }
}
