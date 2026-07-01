package io.minikafka.protocol;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.Random;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * AC-5: any byte sequence fed to {@link FrameDecoder} either parses into a {@link Frame} or throws
 * {@link ProtocolException}/EOF — it never hangs, never crashes with an unexpected exception, and
 * never allocates unboundedly. Implemented as a seeded (reproducible) randomized test; no external
 * property-testing library.
 */
class FrameDecoderPropertyTest {

  private static final long SEED = 0xC0FFEE123L;
  private static final int ITERATIONS = 50_000;
  private static final int MAX_FRAME = 64 * 1024;

  @Test
  @Timeout(30) // must never hang, even on adversarial input
  void randomBytesEitherParseOrThrowProtocolException() {
    Random random = new Random(SEED);
    MessageCodec codec = MessageCodec.instance();

    for (int i = 0; i < ITERATIONS; i++) {
      byte[] input = new byte[random.nextInt(0, 64)];
      random.nextBytes(input);

      FrameDecoder decoder = new FrameDecoder(new ByteArrayInputStream(input), MAX_FRAME);
      try {
        Frame frame = decoder.read();
        if (frame != null) {
          // A frame that decodes at the framing layer must also decode (or cleanly reject) at the
          // message layer — never an unchecked crash.
          assertNotNull(frame.payload());
          try {
            codec.decode(frame);
          } catch (ProtocolException expected) {
            // acceptable: payload was well-framed but not a valid message body
          }
        }
      } catch (ProtocolException | java.io.EOFException expected) {
        // acceptable outcomes for malformed input
      } catch (IOException e) {
        fail("Unexpected IOException on seed=" + SEED + " iteration=" + i + ": " + e);
      } catch (RuntimeException e) {
        fail("Decoder crashed on seed=" + SEED + " iteration=" + i + ": " + e);
      }
    }
  }

  @Test
  @Timeout(10)
  void hugeLengthPrefixIsRejectedNotAllocated() {
    // 0x7FFFFFFF length with an empty body: must throw immediately, not attempt a 2 GB allocation.
    byte[] input = {0x7F, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF};
    FrameDecoder decoder = new FrameDecoder(new ByteArrayInputStream(input), MAX_FRAME);
    boolean threw = false;
    try {
      decoder.read();
    } catch (IOException e) {
      threw = true;
    }
    assertTrue(threw, "Over-long frame must be rejected");
  }
}
