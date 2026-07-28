package io.minikafka.bench;

import io.minikafka.client.ClusterClient;
import io.minikafka.client.ConsumerClient;
import io.minikafka.client.ProducerClient;
import io.minikafka.protocol.PollResp;
import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;

/**
 * Standalone (non-JMH), multi-threaded producer + consumer that sustains load for a configurable
 * duration, printing a periodic and final summary. Not used by the chaos harness — see the plan's
 * scope note: {@code ChaosOrchestrator} already has its own load loop with history recording that
 * this tool deliberately omits, and wiring the two together would be a new {@code chaos -> bench}
 * module dependency the module map doesn't allow.
 *
 * <p>One {@link ProducerClient} per thread, each over its own {@link ClusterClient}: sharing a
 * {@link io.minikafka.client.BrokerConnection} across threads is unsafe (it isn't synchronized),
 * and {@link ClusterClient} pools exactly one connection per broker id, so two producer threads
 * sharing a {@code ClusterClient} could end up issuing concurrent requests over the same socket.
 */
public final class LoadGenerator {

  private static final String GROUP = "bench-loadgen";
  private static final long SUMMARY_INTERVAL_MS = 5_000;

  public static void main(String[] args) throws Exception {
    LoadGeneratorArgs parsed = LoadGeneratorArgs.parse(args);
    BenchConfig base = BenchConfig.fromSystemProperties();
    BenchConfig config =
        new BenchConfig(
            parsed.bootstrapHost() != null ? parsed.bootstrapHost() : base.bootstrapHost(),
            parsed.bootstrapPort() != null ? parsed.bootstrapPort() : base.bootstrapPort(),
            parsed.topic() != null ? parsed.topic() : base.topic(),
            parsed.partitions() != null ? parsed.partitions() : base.partitions(),
            parsed.rf() != null ? parsed.rf() : base.replicationFactor(),
            parsed.payloadSize() != null ? parsed.payloadSize() : base.payloadBytes(),
            parsed.threads() != null ? parsed.threads() : base.threads(),
            base.maxFrameBytes());

    run(config, parsed);
  }

  static void run(BenchConfig config, LoadGeneratorArgs args) throws Exception {
    byte[] payload = new byte[config.payloadBytes()];
    new Random(42).nextBytes(payload);

    AtomicLong sent = new AtomicLong();
    LongAdder acked = new LongAdder();
    LongAdder errors = new LongAdder();
    LongAdder received = new LongAdder();
    LatencyHistogram latencies = new LatencyHistogram();

    long deadlineNanos = System.nanoTime() + args.duration().toNanos();
    long messageBudget = args.messages() > 0 ? args.messages() : Long.MAX_VALUE;

    try (ExecutorService producerPool = Executors.newVirtualThreadPerTaskExecutor();
        ExecutorService consumerPool = Executors.newVirtualThreadPerTaskExecutor();
        ExecutorService reporter = Executors.newSingleThreadExecutor()) {

      List<Integer> partitions = new ArrayList<>();
      for (int p = 0; p < config.partitions(); p++) {
        partitions.add(p);
      }
      java.util.concurrent.atomic.AtomicBoolean stop =
          new java.util.concurrent.atomic.AtomicBoolean(false);
      for (int p : partitions) {
        consumerPool.submit(() -> runConsumer(config, p, received, stop));
      }

      reporter.submit(() -> printPeriodicSummary(stop, sent, acked, errors, received));

      List<java.util.concurrent.Future<?>> producers = new ArrayList<>();
      for (int t = 0; t < config.threads(); t++) {
        final int partition = partitions.get(t % partitions.size());
        producers.add(
            producerPool.submit(
                () ->
                    runProducer(
                        config,
                        partition,
                        payload,
                        deadlineNanos,
                        messageBudget,
                        sent,
                        acked,
                        errors,
                        latencies)));
      }
      for (var f : producers) {
        f.get();
      }
      stop.set(true);
      consumerPool.shutdown();
      if (!consumerPool.awaitTermination(10, TimeUnit.SECONDS)) {
        consumerPool.shutdownNow();
      }
    }

    printFinalSummary(args.duration(), sent, acked, errors, received, latencies);
  }

  private static void runProducer(
      BenchConfig config,
      int partition,
      byte[] payload,
      long deadlineNanos,
      long messageBudget,
      AtomicLong sent,
      LongAdder acked,
      LongAdder errors,
      LatencyHistogram latencies) {
    try {
      ClusterClient clusterClient =
          new ClusterClient(config.bootstrapHost(), config.bootstrapPort(), config.maxFrameBytes());
      ProducerClient producer = new ProducerClient(clusterClient);
      try {
        while (System.nanoTime() < deadlineNanos && sent.get() < messageBudget) {
          sent.incrementAndGet();
          long start = System.nanoTime();
          try {
            producer.publish(config.topic(), partition, payload);
            acked.increment();
            latencies.record((System.nanoTime() - start) / 1_000_000);
          } catch (IOException e) {
            errors.increment();
          }
        }
      } finally {
        clusterClient.close();
      }
    } catch (IOException e) {
      throw new java.io.UncheckedIOException("producer failed to start", e);
    }
  }

  private static void runConsumer(
      BenchConfig config,
      int partition,
      LongAdder received,
      java.util.concurrent.atomic.AtomicBoolean stop) {
    try {
      ClusterClient clusterClient =
          new ClusterClient(config.bootstrapHost(), config.bootstrapPort(), config.maxFrameBytes());
      try {
        ConsumerClient consumer =
            new ConsumerClient(clusterClient, config.topic(), partition, GROUP);
        while (!stop.get() && !Thread.currentThread().isInterrupted()) {
          List<PollResp.Record> records = consumer.poll();
          received.add(records.size());
          if (records.isEmpty()) {
            Thread.sleep(20);
          }
        }
      } finally {
        clusterClient.close();
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    } catch (IOException e) {
      throw new java.io.UncheckedIOException("consumer failed to start", e);
    }
  }

  private static void printPeriodicSummary(
      java.util.concurrent.atomic.AtomicBoolean stop,
      AtomicLong sent,
      LongAdder acked,
      LongAdder errors,
      LongAdder received) {
    try {
      while (!stop.get()) {
        Thread.sleep(SUMMARY_INTERVAL_MS);
        System.out.printf(
            "[loadgen] sent=%d acked=%d errors=%d received=%d%n",
            sent.get(), acked.sum(), errors.sum(), received.sum());
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }

  private static void printFinalSummary(
      Duration duration,
      AtomicLong sent,
      LongAdder acked,
      LongAdder errors,
      LongAdder received,
      LatencyHistogram latencies) {
    double seconds = Math.max(1, duration.toSeconds());
    System.out.println("=== LoadGenerator summary ===");
    System.out.printf("sent:       %d%n", sent.get());
    System.out.printf("acked:      %d%n", acked.sum());
    System.out.printf("errors:     %d%n", errors.sum());
    System.out.printf("received:   %d%n", received.sum());
    System.out.printf(
        "throughput: %.1f msgs/sec (acked / wall-clock duration)%n", acked.sum() / seconds);
    System.out.printf(
        "latency:    p50=%dms p99=%dms (publish-to-ack, this producer's wait only)%n",
        latencies.percentile(50), latencies.percentile(99));
  }
}
