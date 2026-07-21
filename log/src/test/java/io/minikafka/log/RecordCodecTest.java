package io.minikafka.log;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.nio.ByteBuffer;
import java.util.Random;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class RecordCodecTest {

  @Test
  void roundTripsWithNullKey() {
    LogRecord record = new LogRecord(7, 12345L, null, "hello".getBytes());
    byte[] encoded = RecordCodec.encode(record);
    assertEquals(RecordCodec.sizeOf(record), encoded.length);

    LogRecord decoded = RecordCodec.decode(ByteBuffer.wrap(encoded));
    assertEquals(record, decoded);
  }

  @Test
  void roundTripsWithKey() {
    LogRecord record = new LogRecord(3, 99L, "k".getBytes(), "v".getBytes());
    byte[] encoded = RecordCodec.encode(record);

    LogRecord decoded = RecordCodec.decode(ByteBuffer.wrap(encoded));
    assertEquals(record, decoded);
  }

  @Test
  void roundTripsWithEmptyValue() {
    LogRecord record = new LogRecord(0, 1L, null, new byte[0]);
    byte[] encoded = RecordCodec.encode(record);

    LogRecord decoded = RecordCodec.decode(ByteBuffer.wrap(encoded));
    assertEquals(record, decoded);
  }

  @Test
  void roundTripsWithLargeValue() {
    byte[] value = new byte[64 * 1024];
    new Random(42).nextBytes(value);
    LogRecord record = new LogRecord(1, 1L, null, value);
    byte[] encoded = RecordCodec.encode(record);

    LogRecord decoded = RecordCodec.decode(ByteBuffer.wrap(encoded));
    assertEquals(record, decoded);
  }

  @Test
  void detectsCrcMismatch() {
    LogRecord record = new LogRecord(0, 1L, null, "hello".getBytes());
    byte[] encoded = RecordCodec.encode(record);
    encoded[encoded.length - 1] ^= 0xFF; // flip a byte in the CRC field

    assertThrows(
        RecordCodec.CorruptRecordException.class,
        () -> RecordCodec.decode(ByteBuffer.wrap(encoded)));
  }

  @Test
  void detectsCorruptionInPayload() {
    LogRecord record = new LogRecord(0, 1L, null, "hello".getBytes());
    byte[] encoded = RecordCodec.encode(record);
    encoded[26] ^= 0xFF; // flip a byte inside the value ("hello" occupies bytes 24-28)

    assertThrows(
        RecordCodec.CorruptRecordException.class,
        () -> RecordCodec.decode(ByteBuffer.wrap(encoded)));
  }

  @Test
  void returnsNullOnTruncatedBuffer() {
    LogRecord record = new LogRecord(0, 1L, null, "hello world".getBytes());
    byte[] encoded = RecordCodec.encode(record);
    byte[] truncated = new byte[encoded.length - 3];
    System.arraycopy(encoded, 0, truncated, 0, truncated.length);

    ByteBuffer buf = ByteBuffer.wrap(truncated);
    int startPos = buf.position();
    assertNull(RecordCodec.decode(buf));
    assertEquals(startPos, buf.position());
  }

  @Test
  void returnsNullWhenFewerBytesThanHeader() {
    ByteBuffer buf = ByteBuffer.wrap(new byte[] {1, 2, 3});
    assertNull(RecordCodec.decode(buf));
  }

  @ParameterizedTest
  @ValueSource(ints = {0, 1, 7, 16, 100, 1000, 8191, 8192})
  void roundTripsRandomSizes(int size) {
    byte[] value = new byte[size];
    new Random(size).nextBytes(value);
    LogRecord record = new LogRecord(size, size, null, value);
    byte[] encoded = RecordCodec.encode(record);

    LogRecord decoded = RecordCodec.decode(ByteBuffer.wrap(encoded));
    assertEquals(record, decoded);
  }
}
