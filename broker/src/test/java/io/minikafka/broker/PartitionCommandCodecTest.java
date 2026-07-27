package io.minikafka.broker;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

import io.minikafka.log.LogRecord;
import org.junit.jupiter.api.Test;

/**
 * Round-trip and byte-stability tests for {@link PartitionCommandCodec} — the Raft command envelope
 * that must decode identically on every replica so the three logs stay byte-identical.
 */
class PartitionCommandCodecTest {

  @Test
  void roundTripsWithKeyAndRealProducer() {
    byte[] command = PartitionCommandCodec.encode(12345L, 7L, 42L, "k".getBytes(), "v".getBytes());
    LogRecord decoded = PartitionCommandCodec.decode(command);

    assertEquals(0, decoded.offset());
    assertEquals(12345L, decoded.timestamp());
    assertEquals(7L, decoded.producerId());
    assertEquals(42L, decoded.seqNo());
    assertArrayEquals("k".getBytes(), decoded.key());
    assertArrayEquals("v".getBytes(), decoded.value());
  }

  @Test
  void roundTripsWithNullKeyAndSentinelProducer() {
    byte[] command =
        PartitionCommandCodec.encode(
            999L, LogRecord.NO_PRODUCER_ID, LogRecord.NO_SEQ, null, "v".getBytes());
    LogRecord decoded = PartitionCommandCodec.decode(command);

    assertEquals(LogRecord.NO_PRODUCER_ID, decoded.producerId());
    assertEquals(LogRecord.NO_SEQ, decoded.seqNo());
    assertEquals(null, decoded.key());
    assertArrayEquals("v".getBytes(), decoded.value());
  }

  @Test
  void encodingIsByteStable() {
    byte[] first = PartitionCommandCodec.encode(1L, 2L, 3L, "k".getBytes(), "v".getBytes());
    LogRecord decoded = PartitionCommandCodec.decode(first);
    byte[] second =
        PartitionCommandCodec.encode(
            decoded.timestamp(),
            decoded.producerId(),
            decoded.seqNo(),
            decoded.key(),
            decoded.value());
    assertArrayEquals(first, second);
  }
}
