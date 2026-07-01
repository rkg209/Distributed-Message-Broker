package io.minikafka.protocol;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import org.junit.jupiter.api.Test;

/** FrameEncoder/FrameDecoder framing: round-trip fidelity and malformed-input rejection. */
class FrameCodecTest {

  private static final int MAX = ProtocolConfig.DEFAULT_MAX_FRAME_BYTES;

  private static byte[] encode(Frame frame) throws IOException {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    new FrameEncoder(out).write(frame);
    return out.toByteArray();
  }

  private static FrameDecoder decoderOf(byte[] bytes) {
    return new FrameDecoder(new ByteArrayInputStream(bytes), MAX);
  }

  @Test
  void roundTripsAFrame() throws IOException {
    Frame original = new Frame(MessageType.METADATA_REQ, new byte[] {0, 0, 0, 0, 0, 0, 0, 7});
    Frame decoded = decoderOf(encode(original)).read();
    assertEquals(original, decoded);
  }

  @Test
  void roundTripsEmptyPayload() throws IOException {
    Frame original = new Frame(MessageType.HEARTBEAT_RESP, new byte[0]);
    assertEquals(original, decoderOf(encode(original)).read());
  }

  @Test
  void readsMultipleFramesInSequence() throws IOException {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    FrameEncoder encoder = new FrameEncoder(out);
    Frame a = new Frame(MessageType.HEARTBEAT_REQ, new byte[] {1});
    Frame b = new Frame(MessageType.HEARTBEAT_RESP, new byte[] {2});
    encoder.write(a);
    encoder.write(b);

    FrameDecoder decoder = decoderOf(out.toByteArray());
    assertEquals(a, decoder.read());
    assertEquals(b, decoder.read());
    assertNull(decoder.read()); // clean EOF at frame boundary
  }

  @Test
  void cleanEofReturnsNull() throws IOException {
    assertNull(decoderOf(new byte[0]).read());
  }

  @Test
  void unknownTypeByteThrows() {
    // length=1 (type only), type=0x00 which is not a known code.
    byte[] bytes = {0, 0, 0, 1, 0x00};
    assertThrows(ProtocolException.class, () -> decoderOf(bytes).read());
  }

  @Test
  void zeroLengthThrows() {
    byte[] bytes = {0, 0, 0, 0};
    assertThrows(ProtocolException.class, () -> decoderOf(bytes).read());
  }

  @Test
  void negativeLengthThrows() {
    byte[] bytes = {(byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF};
    assertThrows(ProtocolException.class, () -> decoderOf(bytes).read());
  }

  @Test
  void overMaxLengthThrowsWithoutAllocating() {
    // Declares a 2 GB frame but supplies almost nothing; must reject on the length check alone.
    byte[] bytes = {0x7F, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, 0x07};
    FrameDecoder decoder = new FrameDecoder(new ByteArrayInputStream(bytes), 1024);
    assertThrows(ProtocolException.class, decoder::read);
  }

  @Test
  void truncatedPayloadThrows() {
    // length=8 but only 3 payload bytes follow the type byte.
    byte[] bytes = {0, 0, 0, 8, MessageType.HEARTBEAT_RESP.code(), 1, 2, 3};
    assertThrows(IOException.class, () -> decoderOf(bytes).read());
  }

  @Test
  void truncatedLengthPrefixThrows() {
    byte[] bytes = {0, 0}; // only two of four length bytes
    assertThrows(ProtocolException.class, () -> decoderOf(bytes).read());
  }
}
