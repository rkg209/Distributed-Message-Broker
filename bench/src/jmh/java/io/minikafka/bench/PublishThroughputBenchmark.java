package io.minikafka.bench;

import io.minikafka.client.ClusterClient;
import io.minikafka.client.ProducerClient;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;

/**
 * Sustained publish throughput at 1KB payloads against the real Docker Compose cluster brought up
 * by {@code benchClusterUp} (RF chosen by which compose overlay is running — {@code rf} is recorded
 * as a JMH parameter purely so it's self-describing in {@code results.json}; it does not change
 * what this benchmark does).
 */
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
public class PublishThroughputBenchmark {

  /**
   * Records which replication factor the running cluster is configured for. Not read at runtime.
   */
  @State(Scope.Benchmark)
  public static class ClusterState {

    @Param({"3"})
    public String rf;

    BenchConfig config;
    ClusterClient bootstrapClient;
    byte[] payload;

    @Setup(Level.Trial)
    public void setup() throws Exception {
      config = BenchConfig.fromSystemProperties();
      bootstrapClient =
          new ClusterClient(config.bootstrapHost(), config.bootstrapPort(), config.maxFrameBytes());
      payload = new byte[config.payloadBytes()];
      new Random(7).nextBytes(payload);
    }

    @TearDown(Level.Trial)
    public void teardown() {
      bootstrapClient.close();
    }
  }

  @State(Scope.Thread)
  public static class ProducerState {

    private static final AtomicInteger NEXT_PARTITION = new AtomicInteger();

    ProducerClient producer;
    ClusterClient perThreadClient;
    int partition;

    @Setup(Level.Trial)
    public void setup(ClusterState cluster) throws Exception {
      perThreadClient =
          new ClusterClient(
              cluster.config.bootstrapHost(),
              cluster.config.bootstrapPort(),
              cluster.config.maxFrameBytes());
      producer = new ProducerClient(perThreadClient);
      partition = NEXT_PARTITION.getAndIncrement() % cluster.config.partitions();
    }

    @TearDown(Level.Trial)
    public void teardown() {
      perThreadClient.close();
    }
  }

  @Benchmark
  public long publish(ClusterState cluster, ProducerState state) throws Exception {
    return state.producer.publish(cluster.config.topic(), state.partition, cluster.payload);
  }
}
