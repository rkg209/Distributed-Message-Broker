package io.minikafka.chaos;

import static org.junit.jupiter.api.Assertions.assertEquals;

import io.minikafka.chaos.HistoryRecorder.History;
import io.minikafka.chaos.HistoryRecorder.ProducerOutcome;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class HistoryRecorderTest {

  @Test
  void concurrentRecordingLosesNoEvents() throws InterruptedException {
    HistoryRecorder recorder = new HistoryRecorder();
    int threads = 16;
    int perThread = 200;
    try (ExecutorService pool = Executors.newVirtualThreadPerTaskExecutor()) {
      for (int t = 0; t < threads; t++) {
        int producerId = t;
        pool.submit(
            () -> {
              for (int i = 0; i < perThread; i++) {
                recorder.recordProducer(producerId, i, 0, 0, 1, ProducerOutcome.ACKED, i);
                recorder.recordConsumer("c1", producerId, i, 0, i, 0, 1);
              }
            });
      }
      pool.shutdown();
      pool.awaitTermination(30, TimeUnit.SECONDS);
    }

    History history = recorder.snapshot();
    assertEquals(threads * perThread, history.producerEvents().size());
    assertEquals(threads * perThread, history.consumerEvents().size());
  }

  @Test
  void payloadRoundTrips() {
    byte[] payload = HistoryRecorder.encodePayload(42, 7, 2, 16);
    HistoryRecorder.DecodedPayload decoded = HistoryRecorder.decodePayload(payload);
    assertEquals(42, decoded.producerId());
    assertEquals(7, decoded.seqNo());
    assertEquals(2, decoded.partition());
  }

  @Test
  void dumpWritesCsvWithHeaderAndAllEvents(@TempDir Path dir) throws IOException {
    HistoryRecorder recorder = new HistoryRecorder();
    recorder.recordProducer(1, 0, 0, 0, 1, ProducerOutcome.ACKED, 0);
    recorder.recordConsumer("c1", 1, 0, 0, 0, 2, 3);

    Path csv = dir.resolve("history.csv");
    recorder.dump(csv);

    List<String> lines = Files.readAllLines(csv);
    assertEquals(3, lines.size());
    assertEquals(
        "kind,producerId,seqNo,partition,offset,consumerId,invokedNanos,respondedNanos,outcome",
        lines.get(0));
  }
}
