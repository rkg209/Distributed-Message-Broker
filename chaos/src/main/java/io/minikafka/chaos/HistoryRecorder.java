package io.minikafka.chaos;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * Thread-safe recorder of every producer and consumer event during a chaos run. No locks on the hot
 * path: each producer/consumer virtual thread appends immutable event records to its own lock-free
 * queue.
 */
public final class HistoryRecorder {

  private final ConcurrentLinkedQueue<ProducerEvent> producerEvents = new ConcurrentLinkedQueue<>();
  private final ConcurrentLinkedQueue<ConsumerEvent> consumerEvents = new ConcurrentLinkedQueue<>();

  /** Outcome of a single publish attempt as observed by the producer. */
  public enum ProducerOutcome {
    ACKED,
    TIMEOUT,
    REJECTED
  }

  /**
   * One publish attempt. {@code offset} is only meaningful when {@code outcome == ACKED}; other
   * outcomes carry {@code -1}.
   */
  public record ProducerEvent(
      long producerId,
      long seqNo,
      int partition,
      long invokedNanos,
      long respondedNanos,
      ProducerOutcome outcome,
      long offset) {}

  /** One record observed by a consumer, self-describing its origin via the payload encoding. */
  public record ConsumerEvent(
      String consumerId,
      long producerId,
      long seqNo,
      int partition,
      long offset,
      long invokedNanos,
      long respondedNanos) {}

  public void recordProducer(
      long producerId,
      long seqNo,
      int partition,
      long invokedNanos,
      long respondedNanos,
      ProducerOutcome outcome,
      long offset) {
    producerEvents.add(
        new ProducerEvent(
            producerId, seqNo, partition, invokedNanos, respondedNanos, outcome, offset));
  }

  public void recordConsumer(
      String consumerId,
      long producerId,
      long seqNo,
      int partition,
      long offset,
      long invokedNanos,
      long respondedNanos) {
    consumerEvents.add(
        new ConsumerEvent(
            consumerId, producerId, seqNo, partition, offset, invokedNanos, respondedNanos));
  }

  /** An immutable point-in-time snapshot suitable for handing to a {@code Checker}. */
  public History snapshot() {
    return new History(new ArrayList<>(producerEvents), new ArrayList<>(consumerEvents));
  }

  /** Writes a CSV dump of the full history for post-mortem analysis of a failing run. */
  public void dump(Path path) throws IOException {
    dump(snapshot(), path);
  }

  /** Writes a CSV dump of {@code history} for post-mortem analysis of a failing run. */
  public static void dump(History history, Path path) throws IOException {
    Files.createDirectories(path.toAbsolutePath().getParent());
    try (BufferedWriter writer = Files.newBufferedWriter(path, StandardCharsets.UTF_8)) {
      writer.write(
          "kind,producerId,seqNo,partition,offset,consumerId,invokedNanos,respondedNanos,outcome");
      writer.newLine();
      for (ProducerEvent e : history.producerEvents()) {
        writer.write(
            String.join(
                ",",
                "PRODUCER",
                Long.toString(e.producerId()),
                Long.toString(e.seqNo()),
                Integer.toString(e.partition()),
                Long.toString(e.offset()),
                "",
                Long.toString(e.invokedNanos()),
                Long.toString(e.respondedNanos()),
                e.outcome().name()));
        writer.newLine();
      }
      for (ConsumerEvent e : history.consumerEvents()) {
        writer.write(
            String.join(
                ",",
                "CONSUMER",
                Long.toString(e.producerId()),
                Long.toString(e.seqNo()),
                Integer.toString(e.partition()),
                Long.toString(e.offset()),
                e.consumerId(),
                Long.toString(e.invokedNanos()),
                Long.toString(e.respondedNanos()),
                ""));
        writer.newLine();
      }
    }
  }

  /** Encodes a self-describing payload: {@code producerId|seqNo|partition}. */
  public static byte[] encodePayload(long producerId, long seqNo, int partition, int paddingBytes) {
    String header = producerId + "|" + seqNo + "|" + partition + "|";
    byte[] headerBytes = header.getBytes(StandardCharsets.UTF_8);
    byte[] payload = new byte[headerBytes.length + Math.max(0, paddingBytes)];
    System.arraycopy(headerBytes, 0, payload, 0, headerBytes.length);
    return payload;
  }

  /** Decodes a payload written by {@link #encodePayload}. */
  public static DecodedPayload decodePayload(byte[] payload) {
    String s = new String(payload, StandardCharsets.UTF_8);
    String[] parts = s.split("\\|", 4);
    return new DecodedPayload(
        Long.parseLong(parts[0]), Long.parseLong(parts[1]), Integer.parseInt(parts[2]));
  }

  public record DecodedPayload(long producerId, long seqNo, int partition) {}

  /** Immutable snapshot of everything recorded so far. */
  public record History(List<ProducerEvent> producerEvents, List<ConsumerEvent> consumerEvents) {

    public History {
      producerEvents = List.copyOf(producerEvents);
      consumerEvents = List.copyOf(consumerEvents);
    }
  }
}
