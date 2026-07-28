package io.minikafka.bench;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class BenchResultsWriterTest {

  private static final String RF3_JSON =
      """
      [
        {
          "benchmark": "io.minikafka.bench.PublishThroughputBenchmark.publish",
          "mode": "thrpt",
          "params": { "rf": "3" },
          "primaryMetric": { "score": 50000.0, "scoreUnit": "ops/s" }
        },
        {
          "benchmark": "io.minikafka.bench.PublishLatencyBenchmark.publish",
          "mode": "sample",
          "params": { "rf": "3" },
          "primaryMetric": {
            "score": 4.2,
            "scoreUnit": "ms/op",
            "scorePercentiles": { "50.0": 3.1, "99.0": 7.8, "99.9": 12.4 }
          }
        }
      ]
      """;

  private static final String RF1_JSON =
      """
      [
        {
          "benchmark": "io.minikafka.bench.PublishThroughputBenchmark.publish",
          "mode": "thrpt",
          "params": { "rf": "1" },
          "primaryMetric": { "score": 80000.0, "scoreUnit": "ops/s" }
        }
      ]
      """;

  private static final String RESULTS_MD =
      """
      # Results — Distributed Message Broker

      ---

      ## Chaos / Fault-Injection Results

      | Run | Date | Result |
      |-----|------|--------|
      | 1 | 2026-07-28 | PASS |

      ---

      ## Throughput (1KB payload, 3-broker cluster)

      | Metric              | RF=1        | RF=3        | RF=3 overhead |
      |---------------------|-------------|-------------|---------------|
      | Throughput (msgs/s) | PENDING     | PENDING     | PENDING       |

      **Target:** ≥ 200,000 msgs/sec at RF=3.

      ---

      ## Publish-to-commit Latency (RF=3, 1KB payload)

      | Percentile | Latency   |
      |------------|-----------|
      | p50        | PENDING   |
      | p99        | PENDING   |
      | p999       | PENDING   |

      **Target:** p99 ≤ 8ms at RF=3 under target throughput.

      ---

      ## Spec Completion Tracker

      | Spec | Status |
      |------|--------|
      | 12   | todo   |
      """;

  @Test
  void replacesThroughputAndLatencySectionsAndComputesOverhead(@TempDir Path tmp) throws Exception {
    Path rf3 = tmp.resolve("rf3.json");
    Path rf1 = tmp.resolve("rf1.json");
    Path resultsMd = tmp.resolve("results.md");
    Files.writeString(rf3, RF3_JSON, StandardCharsets.UTF_8);
    Files.writeString(rf1, RF1_JSON, StandardCharsets.UTF_8);
    Files.writeString(resultsMd, RESULTS_MD, StandardCharsets.UTF_8);

    new BenchResultsWriter().write(List.of(rf3, rf1), resultsMd);

    String updated = Files.readString(resultsMd, StandardCharsets.UTF_8);

    assertTrue(updated.contains("50,000/s"), updated);
    assertTrue(updated.contains("80,000/s"), updated);
    assertTrue(updated.contains("37.5%"), updated); // (80000-50000)/80000
    assertTrue(updated.contains("3.10 ms"), updated);
    assertTrue(updated.contains("7.80 ms"), updated);
    assertTrue(updated.contains("12.40 ms"), updated);
  }

  @Test
  void leavesOtherSectionsByteIdentical(@TempDir Path tmp) throws Exception {
    Path rf3 = tmp.resolve("rf3.json");
    Path resultsMd = tmp.resolve("results.md");
    Files.writeString(rf3, RF3_JSON, StandardCharsets.UTF_8);
    Files.writeString(resultsMd, RESULTS_MD, StandardCharsets.UTF_8);

    new BenchResultsWriter().write(List.of(rf3), resultsMd);

    String updated = Files.readString(resultsMd, StandardCharsets.UTF_8);
    assertTrue(updated.contains("## Chaos / Fault-Injection Results"));
    assertTrue(updated.contains("| 1 | 2026-07-28 | PASS |"));
    assertTrue(updated.contains("## Spec Completion Tracker"));
    assertTrue(updated.contains("| 12   | todo   |"));
  }

  @Test
  void singleFileLeavesMissingRfPending(@TempDir Path tmp) throws Exception {
    Path rf3 = tmp.resolve("rf3.json");
    Path resultsMd = tmp.resolve("results.md");
    Files.writeString(rf3, RF3_JSON, StandardCharsets.UTF_8);
    Files.writeString(resultsMd, RESULTS_MD, StandardCharsets.UTF_8);

    new BenchResultsWriter().write(List.of(rf3), resultsMd);

    String updated = Files.readString(resultsMd, StandardCharsets.UTF_8);
    List<String> lines = updated.lines().toList();
    int throughputLine =
        lines.indexOf(
            lines.stream()
                .filter(l -> l.contains("Throughput (msgs/s)"))
                .findFirst()
                .orElseThrow());
    assertTrue(lines.get(throughputLine).contains("PENDING"));
    assertTrue(lines.get(throughputLine).contains("50,000/s"));
    assertEquals(
        1,
        lines.stream()
            .filter(l -> l.equals("## Throughput (1KB payload, 3-broker cluster)"))
            .count());
  }
}
