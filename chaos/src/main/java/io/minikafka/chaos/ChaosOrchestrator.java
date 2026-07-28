package io.minikafka.chaos;

import io.minikafka.client.BrokerBusyException;
import io.minikafka.client.ClusterClient;
import io.minikafka.client.ConsumerClient;
import io.minikafka.client.PartitionRouter;
import io.minikafka.client.ProducerClient;
import io.minikafka.client.SequenceGapException;
import io.minikafka.protocol.PollResp;
import java.io.IOException;
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
 * Drives one chaos run: {@code producers} virtual-thread {@link ProducerClient}s publish
 * self-describing payloads, one {@link ConsumerClient} per partition drains them under a shared
 * consumer group, and a fault-schedule thread injects leader kills on a cadence derived from {@link
 * ChaosConfig#faultIntervalMessages()}. Every event is captured by a {@link HistoryRecorder} for
 * the checkers to verify afterward.
 */
public final class ChaosOrchestrator {

  private static final String GROUP = "chaos-group";
  private static final int PAYLOAD_PADDING_BYTES = 64;

  // ProducerClient/ConsumerClient's own DEFAULT_MAX_RETRIES (5) / DEFAULT_RETRY_BACKOFF_MS (100)
  // budget ~0.5-1s of redirect retries — enough for a fast in-process failover test, but a chaos
  // run's faults are real `docker kill`/container-recreate events: the replacement container needs
  // real wall-clock time to start, rejoin, and (if it was leader) lose an election to a peer before
  // any client can succeed again. Every producer/consumer thread in a run redirects through the
  // same
  // disruption window, so the budget here is sized for that, not for a single client's failover.
  private static final int REDIRECT_MAX_RETRIES = 30;
  private static final long REDIRECT_BACKOFF_MS = 200;

  private final ChaosConfig config;
  private final ClusterClient bootstrapClient;
  private final FaultInjector faultInjector;
  private final HistoryRecorder recorder = new HistoryRecorder();

  private final AtomicLong sent = new AtomicLong();
  private final AtomicLong acked = new AtomicLong();
  private final AtomicLong received = new AtomicLong();
  private final AtomicLong crashesInjected = new AtomicLong();

  public ChaosOrchestrator(
      ChaosConfig config, ClusterClient bootstrapClient, FaultInjector faultInjector) {
    this.config = config;
    this.bootstrapClient = bootstrapClient;
    this.faultInjector = faultInjector;
  }

  public ChaosReport run() throws Exception {
    long start = System.nanoTime();
    AtomicBoolean producersDone = new AtomicBoolean(false);
    AtomicBoolean stopConsumers = new AtomicBoolean(false);

    try (ExecutorService producerPool = Executors.newVirtualThreadPerTaskExecutor();
        ExecutorService consumerPool = Executors.newVirtualThreadPerTaskExecutor();
        ExecutorService faultPool = Executors.newVirtualThreadPerTaskExecutor()) {

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

      faultPool.submit(() -> runFaultSchedule(producersDone));

      producersLatch.await(config.runTimeoutMs(), TimeUnit.MILLISECONDS);
      producersDone.set(true);

      // Let consumers drain to end-of-log: wait until no partition yields new records for a
      // full settle window.
      long deadline =
          System.nanoTime() + Duration.ofMillis(config.recoverySettleMs() * 3).toNanos();
      long lastReceived = -1;
      while (System.nanoTime() < deadline) {
        Thread.sleep(config.recoverySettleMs());
        long now = received.get();
        if (now == lastReceived) {
          break;
        }
        lastReceived = now;
      }
      stopConsumers.set(true);
      consumerPool.shutdown();
      consumerPool.awaitTermination(30, TimeUnit.SECONDS);
      faultPool.shutdownNow();
    } finally {
      faultInjector.healAll();
    }

    long durationMs = (System.nanoTime() - start) / 1_000_000;
    return new ChaosReport(
        config,
        (int) crashesInjected.get(),
        0,
        sent.get(),
        acked.get(),
        received.get(),
        durationMs,
        recorder.snapshot());
  }

  private void runProducer(long count, CountDownLatch latch) {
    try {
      ProducerClient producer =
          new ProducerClient(
              bootstrapClient, new PartitionRouter(), REDIRECT_MAX_RETRIES, REDIRECT_BACKOFF_MS);
      long producerId = producer.hashCode() & 0xFFFFFFFFL;
      Random random = new Random(config.seed() ^ Thread.currentThread().threadId());
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
          // Leader moved mid-poll; ConsumerClient over a ClusterClient already retries/redirects
          // internally, so a surfaced IOException means the retry budget was exhausted — back off
          // and let the next loop iteration re-resolve the leader.
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

  private void runFaultSchedule(AtomicBoolean producersDone) {
    Random random = new Random(config.seed());
    try {
      long nextThreshold = config.faultIntervalMessages();
      while (!producersDone.get() && crashesInjected.get() < config.crashes()) {
        if (acked.get() >= nextThreshold) {
          int partition = random.nextInt(config.partitions());
          int killed = faultInjector.killLeader(bootstrapClient, config.topic(), partition);
          crashesInjected.incrementAndGet();
          Thread.sleep(config.recoverySettleMs());
          faultInjector.restart(killed, Duration.ofSeconds(30));
          bootstrapClient.refresh();
          nextThreshold += config.faultIntervalMessages();
        } else {
          Thread.sleep(50);
        }
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    } catch (IOException e) {
      throw new IllegalStateException("fault injection failed", e);
    }
  }
}
