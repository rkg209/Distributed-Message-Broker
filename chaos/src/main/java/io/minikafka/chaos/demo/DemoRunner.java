package io.minikafka.chaos.demo;

import io.minikafka.chaos.HistoryRecorder;
import io.minikafka.chaos.HistoryRecorder.History;
import io.minikafka.chaos.check.CheckResult;
import io.minikafka.chaos.check.DuplicationChecker;
import io.minikafka.chaos.check.LossChecker;
import io.minikafka.client.BrokerBusyException;
import io.minikafka.client.ClusterClient;
import io.minikafka.client.ConsumerClient;
import io.minikafka.client.PartitionRouter;
import io.minikafka.client.ProducerClient;
import io.minikafka.client.SequenceGapException;
import io.minikafka.protocol.PollResp;
import io.minikafka.protocol.ProtocolConfig;
import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Standalone one-command failover demo: {@code demo.producers} virtual-thread {@link
 * ProducerClient}s publish self-describing payloads against an already-running Compose cluster (the
 * {@code scripts/demo.sh} wrapper owns cluster lifecycle, this class owns load, the kill, and the
 * verdict), one {@link ConsumerClient} per partition drains them under a shared consumer group, and
 * once {@code demo.killAtMessages} acks land a watcher thread SIGKILLs {@code demo.killService}
 * mid-publish. Verified afterward against {@link LossChecker} (INV-1), {@link DuplicationChecker}
 * (INV-3), and {@link OffsetContiguityCheck} (the demo's literal "zero gaps" claim). Closely
 * modeled on {@code ChaosOrchestrator}, but with a single scheduled kill instead of a fault
 * schedule.
 */
public final class DemoRunner {

  private static final String GROUP = "demo-group";
  private static final int PAYLOAD_PADDING_BYTES = 64;

  // Real container kill needs seconds of failover headroom, same reasoning as
  // ChaosOrchestrator.REDIRECT_MAX_RETRIES/REDIRECT_BACKOFF_MS.
  private static final int REDIRECT_MAX_RETRIES = 30;
  private static final long REDIRECT_BACKOFF_MS = 200;

  private final DemoConfig config;
  private final ClusterClient bootstrapClient;
  private final HistoryRecorder recorder = new HistoryRecorder();

  private final AtomicLong sent = new AtomicLong();
  private final AtomicLong acked = new AtomicLong();
  private final AtomicLong received = new AtomicLong();
  private final AtomicBoolean crashInjected = new AtomicBoolean(false);

  public DemoRunner(DemoConfig config, ClusterClient bootstrapClient) {
    this.config = config;
    this.bootstrapClient = bootstrapClient;
  }

  public static void main(String[] args) throws Exception {
    DemoConfig config = DemoConfig.fromSystemProperties();
    try (ClusterClient bootstrapClient =
        new ClusterClient(
            config.bootstrapHost(),
            config.bootstrapPort(),
            ProtocolConfig.DEFAULT_MAX_FRAME_BYTES)) {
      boolean passed = new DemoRunner(config, bootstrapClient).run();
      System.exit(passed ? 0 : 1);
    }
  }

  /** Runs the demo end to end, prints the verdict, and returns whether every check passed. */
  public boolean run() throws Exception {
    long start = System.nanoTime();
    AtomicBoolean producersDone = new AtomicBoolean(false);
    AtomicBoolean stopConsumers = new AtomicBoolean(false);

    try (ExecutorService producerPool = Executors.newVirtualThreadPerTaskExecutor();
        ExecutorService consumerPool = Executors.newVirtualThreadPerTaskExecutor();
        ExecutorService watcherPool = Executors.newVirtualThreadPerTaskExecutor()) {

      CountDownLatch producersLatch = new CountDownLatch(config.producers());
      long perProducer = Math.max(1, config.messages() / config.producers());
      for (int p = 0; p < config.producers(); p++) {
        producerPool.submit(() -> runProducer(perProducer, producersLatch));
      }

      List<CountDownLatch> consumerStarted = new ArrayList<>();
      for (int partition = 0; partition < config.partitions(); partition++) {
        CountDownLatch latch = new CountDownLatch(1);
        consumerStarted.add(latch);
        int finalPartition = partition;
        consumerPool.submit(() -> runConsumer(finalPartition, stopConsumers, latch));
      }
      for (CountDownLatch latch : consumerStarted) {
        latch.await();
      }

      watcherPool.submit(() -> runKillWatcher(producersDone));

      producersLatch.await(config.runTimeoutMs(), TimeUnit.MILLISECONDS);
      producersDone.set(true);

      // Let consumers drain to end-of-log: wait until no partition yields new records for a
      // full settle window.
      long deadline = System.nanoTime() + Duration.ofMillis(config.settleMs() * 3).toNanos();
      long lastReceived = -1;
      while (System.nanoTime() < deadline) {
        Thread.sleep(config.settleMs());
        long now = received.get();
        if (now == lastReceived) {
          break;
        }
        lastReceived = now;
      }
      stopConsumers.set(true);
      consumerPool.shutdown();
      consumerPool.awaitTermination(30, TimeUnit.SECONDS);
      watcherPool.shutdownNow();
    }

    if (config.restartAfter()) {
      restartKilledService();
    }

    long durationMs = (System.nanoTime() - start) / 1_000_000;
    return verify(durationMs);
  }

  private void runProducer(long count, CountDownLatch latch) {
    try {
      ProducerClient producer =
          new ProducerClient(
              bootstrapClient, new PartitionRouter(), REDIRECT_MAX_RETRIES, REDIRECT_BACKOFF_MS);
      long producerId = producer.hashCode() & 0xFFFFFFFFL;
      Random random = new Random(Thread.currentThread().threadId());
      for (long i = 0; i < count; i++) {
        int partition = random.nextInt(config.partitions());
        long seqNo = i;
        byte[] payload =
            HistoryRecorder.encodePayload(producerId, seqNo, partition, PAYLOAD_PADDING_BYTES);
        long invoked = System.nanoTime();
        sent.incrementAndGet();
        try {
          long offset = producer.publish(config.topic(), partition, payload);
          acked.incrementAndGet();
          recorder.recordProducer(
              producerId,
              seqNo,
              partition,
              invoked,
              System.nanoTime(),
              HistoryRecorder.ProducerOutcome.ACKED,
              offset);
        } catch (SequenceGapException | BrokerBusyException e) {
          recorder.recordProducer(
              producerId,
              seqNo,
              partition,
              invoked,
              System.nanoTime(),
              HistoryRecorder.ProducerOutcome.REJECTED,
              -1);
        } catch (IOException e) {
          recorder.recordProducer(
              producerId,
              seqNo,
              partition,
              invoked,
              System.nanoTime(),
              HistoryRecorder.ProducerOutcome.TIMEOUT,
              -1);
        }
      }
    } finally {
      latch.countDown();
    }
  }

  private void runConsumer(int partition, AtomicBoolean stop, CountDownLatch started) {
    try {
      ConsumerClient consumer =
          new ConsumerClient(
              bootstrapClient,
              config.topic(),
              partition,
              GROUP,
              REDIRECT_MAX_RETRIES,
              REDIRECT_BACKOFF_MS);
      started.countDown();
      String consumerId = "consumer-" + partition;
      long lastCommitNanos = System.nanoTime();
      while (!stop.get()) {
        try {
          List<PollResp.Record> records = consumer.poll();
          for (PollResp.Record record : records) {
            HistoryRecorder.DecodedPayload decoded =
                HistoryRecorder.decodePayload(record.payload());
            recorder.recordConsumer(
                consumerId,
                decoded.producerId(),
                decoded.seqNo(),
                partition,
                record.offset(),
                System.nanoTime(),
                System.nanoTime());
            received.incrementAndGet();
          }
          if (records.isEmpty()) {
            sleepQuietly(50);
          }
          if (System.nanoTime() - lastCommitNanos > Duration.ofSeconds(1).toNanos()) {
            consumer.commitOffset();
            lastCommitNanos = System.nanoTime();
          }
        } catch (IOException e) {
          sleepQuietly(100);
        }
      }
      try {
        consumer.commitOffset();
      } catch (IOException ignored) {
        // best-effort final commit
      }
    } catch (IOException e) {
      throw new IllegalStateException(
          "consumer for partition " + partition + " failed to start", e);
    } finally {
      started.countDown();
    }
  }

  private static void sleepQuietly(long millis) {
    try {
      Thread.sleep(millis);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }

  private void runKillWatcher(AtomicBoolean producersDone) {
    try {
      while (!producersDone.get()) {
        if (acked.get() >= config.killAtMessages() && crashInjected.compareAndSet(false, true)) {
          System.out.printf(
              ">>> killing %s mid-publish (%d messages acked)%n",
              config.killService(), acked.get());
          runCompose("kill", "-s", config.killSignal(), config.killService());
          return;
        }
        Thread.sleep(20);
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }

  private void restartKilledService() {
    runCompose("start", config.killService());
  }

  private void runCompose(String... args) {
    String[] full = new String[args.length + 4];
    full[0] = "docker";
    full[1] = "compose";
    full[2] = "-f";
    full[3] = config.composeFile();
    System.arraycopy(args, 0, full, 4, args.length);
    try {
      Process process =
          new ProcessBuilder(full).directory(new File(config.composeDir())).inheritIO().start();
      int exitCode = process.waitFor();
      if (exitCode != 0) {
        throw new IllegalStateException(
            "docker compose " + String.join(" ", args) + " exited " + exitCode);
      }
    } catch (IOException | InterruptedException e) {
      throw new IllegalStateException("docker compose " + String.join(" ", args) + " failed", e);
    }
  }

  private boolean verify(long durationMs) throws IOException {
    History history = recorder.snapshot();
    CheckResult loss = new LossChecker().check(history);
    CheckResult duplication = new DuplicationChecker().check(history);
    CheckResult contiguity = new OffsetContiguityCheck().check(history);

    boolean passed =
        loss.status() == CheckResult.Status.PASS
            && duplication.status() == CheckResult.Status.PASS
            && contiguity.status() == CheckResult.Status.PASS;

    System.out.println("=== Demo summary ===");
    System.out.printf("produced:        %d%n", sent.get());
    System.out.printf("acked:           %d%n", acked.get());
    System.out.printf("received:        %d%n", received.get());
    System.out.printf("crashes injected: %d%n", crashInjected.get() ? 1 : 0);
    System.out.printf("elapsed:         %d ms%n", durationMs);
    System.out.printf("%s: %s%n", loss.name(), loss.status());
    System.out.printf("%s: %s%n", duplication.name(), duplication.status());
    System.out.printf("%s: %s%n", contiguity.name(), contiguity.status());

    if (!passed) {
      HistoryRecorder.dump(history, Path.of("build", "demo-history.csv"));
      for (CheckResult result : List.of(loss, duplication, contiguity)) {
        if (result.status() != CheckResult.Status.PASS) {
          System.out.println(result.name() + " violations:");
          result.violations().forEach(v -> System.out.println("  - " + v.description()));
        }
      }
    }

    System.out.println(passed ? "DEMO RESULT: PASS" : "DEMO RESULT: FAIL");
    return passed;
  }
}
