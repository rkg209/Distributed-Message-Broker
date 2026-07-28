package io.minikafka.bench;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Parses one or two JMH {@code results.json} files (one per replication factor) and replaces the
 * {@code ## Throughput} and {@code ## Publish-to-commit Latency} sections of {@code
 * docs/results.md} with the measured numbers, computing the RF=3-over-RF=1 overhead. Does not reuse
 * {@code chaos/.../check/ResultsWriter} (that would violate the module map's {@code bench -> chaos}
 * restriction) and behaves differently anyway: this replaces its sections outright rather than
 * appending a row, since there's exactly one live throughput/latency number per RF, not a growing
 * history of runs.
 *
 * <p>Which RF a {@code results.json} file measured is read from each benchmark's own {@code
 * params.rf} JMH parameter (set via {@code @Param} on {@code PublishThroughputBenchmark}/{@code
 * PublishLatencyBenchmark}), not inferred from the file's position on the command line — so the two
 * files can be passed in either order.
 */
public final class BenchResultsWriter {

  private static final String THROUGHPUT_HEADER = "## Throughput (1KB payload, 3-broker cluster)";
  private static final String LATENCY_HEADER = "## Publish-to-commit Latency (RF=3, 1KB payload)";
  private static final String THROUGHPUT_BENCHMARK_SUFFIX = "PublishThroughputBenchmark.publish";
  private static final String LATENCY_BENCHMARK_SUFFIX = "PublishLatencyBenchmark.publish";

  private final ObjectMapper mapper = new ObjectMapper();

  /** One replication factor's measured throughput and/or latency, however much was found. */
  record RfResult(String rf, Double throughputOpsPerSec, Map<String, Double> latencyPercentiles) {}

  public static void main(String[] args) throws IOException {
    if (args.length < 2) {
      throw new IllegalArgumentException(
          "Usage: BenchResultsWriter <results.json>... <docs/results.md>");
    }
    Path resultsMd = Path.of(args[args.length - 1]);
    List<Path> jsonFiles = new java.util.ArrayList<>();
    for (int i = 0; i < args.length - 1; i++) {
      Path p = Path.of(args[i]);
      if (Files.exists(p)) {
        jsonFiles.add(p);
      } else {
        System.err.println("Skipping missing results file: " + p);
      }
    }
    if (jsonFiles.isEmpty()) {
      throw new IllegalStateException("No JMH results.json files found among: " + List.of(args));
    }
    new BenchResultsWriter().write(jsonFiles, resultsMd);
  }

  public void write(List<Path> jsonFiles, Path resultsMd) throws IOException {
    Map<String, RfResult> byRf = new HashMap<>();
    for (Path jsonFile : jsonFiles) {
      for (RfResult result : parse(jsonFile)) {
        byRf.merge(result.rf(), result, BenchResultsWriter::merge);
      }
    }

    List<String> lines =
        new java.util.ArrayList<>(Files.readAllLines(resultsMd, StandardCharsets.UTF_8));
    replaceSection(lines, THROUGHPUT_HEADER, throughputTable(byRf));
    replaceSection(lines, LATENCY_HEADER, latencyTable(byRf.get("3")));
    Files.write(resultsMd, lines, StandardCharsets.UTF_8);
  }

  private static RfResult merge(RfResult a, RfResult b) {
    Double throughput =
        a.throughputOpsPerSec() != null ? a.throughputOpsPerSec() : b.throughputOpsPerSec();
    Map<String, Double> latency =
        !a.latencyPercentiles().isEmpty() ? a.latencyPercentiles() : b.latencyPercentiles();
    return new RfResult(a.rf(), throughput, latency);
  }

  private List<RfResult> parse(Path jsonFile) throws IOException {
    JsonNode root = mapper.readTree(jsonFile.toFile());
    Map<String, RfResult> byRf = new HashMap<>();
    for (JsonNode entry : root) {
      String benchmark = entry.path("benchmark").asText("");
      String rf = entry.path("params").path("rf").asText(null);
      if (rf == null) {
        continue;
      }
      JsonNode primary = entry.path("primaryMetric");
      if (benchmark.endsWith(THROUGHPUT_BENCHMARK_SUFFIX)) {
        double score = primary.path("score").asDouble();
        byRf.merge(rf, new RfResult(rf, score, Map.of()), BenchResultsWriter::merge);
      } else if (benchmark.endsWith(LATENCY_BENCHMARK_SUFFIX)) {
        Map<String, Double> percentiles = new HashMap<>();
        primary
            .path("scorePercentiles")
            .fields()
            .forEachRemaining(e -> percentiles.put(e.getKey(), e.getValue().asDouble()));
        byRf.merge(rf, new RfResult(rf, null, percentiles), BenchResultsWriter::merge);
      }
    }
    return List.copyOf(byRf.values());
  }

  private static List<String> throughputTable(Map<String, RfResult> byRf) {
    RfResult rf1 = byRf.get("1");
    RfResult rf3 = byRf.get("3");
    String rf1Str = formatThroughput(rf1);
    String rf3Str = formatThroughput(rf3);
    String overhead = overheadPercent(rf1, rf3);
    return List.of(
        "| Metric              | RF=1        | RF=3        | RF=3 overhead |",
        "|---------------------|-------------|-------------|---------------|",
        "| Throughput (msgs/s) | " + rf1Str + " | " + rf3Str + " | " + overhead + " |",
        "",
        "**Target:** ≥ 200,000 msgs/sec at RF=3.");
  }

  private static List<String> latencyTable(RfResult rf3) {
    if (rf3 == null || rf3.latencyPercentiles().isEmpty()) {
      return List.of(
          "| Percentile | Latency   |",
          "|------------|-----------|",
          "| p50        | PENDING   |",
          "| p99        | PENDING   |",
          "| p999       | PENDING   |",
          "",
          "**Target:** p99 ≤ 8ms at RF=3 under target throughput.");
    }
    Map<String, Double> p = rf3.latencyPercentiles();
    return List.of(
        "| Percentile | Latency   |",
        "|------------|-----------|",
        "| p50        | " + formatMs(p.get("50.0")) + " |",
        "| p99        | " + formatMs(p.get("99.0")) + " |",
        "| p999       | " + formatMs(p.get("99.9")) + " |",
        "",
        "**Target:** p99 ≤ 8ms at RF=3 under target throughput.");
  }

  private static String formatThroughput(RfResult r) {
    return r == null || r.throughputOpsPerSec() == null
        ? "PENDING    "
        : String.format("%,.0f/s", r.throughputOpsPerSec());
  }

  private static String formatMs(Double ms) {
    return ms == null ? "PENDING" : String.format("%.2f ms", ms);
  }

  private static String overheadPercent(RfResult rf1, RfResult rf3) {
    if (rf1 == null
        || rf3 == null
        || rf1.throughputOpsPerSec() == null
        || rf3.throughputOpsPerSec() == null) {
      return "PENDING       ";
    }
    double rf1v = rf1.throughputOpsPerSec();
    double rf3v = rf3.throughputOpsPerSec();
    if (rf1v == 0) {
      return "N/A           ";
    }
    double overhead = (rf1v - rf3v) / rf1v * 100.0;
    return String.format("%.1f%%", overhead);
  }

  /**
   * Replaces every line from {@code header} up to (not including) the next {@code ##} header, or
   * end of file, with {@code header} followed by a blank line and {@code newBody}.
   */
  private static void replaceSection(List<String> lines, String header, List<String> newBody) {
    int headerIndex = -1;
    for (int i = 0; i < lines.size(); i++) {
      if (lines.get(i).strip().equals(header)) {
        headerIndex = i;
        break;
      }
    }
    if (headerIndex < 0) {
      throw new IllegalStateException("Header not found in results file: " + header);
    }
    int end = headerIndex + 1;
    while (end < lines.size() && !lines.get(end).strip().startsWith("##")) {
      end++;
    }
    List<String> replacement = new java.util.ArrayList<>();
    replacement.add(header);
    replacement.add("");
    replacement.addAll(newBody);
    replacement.add("");
    replacement.add("---");
    replacement.add("");
    lines.subList(headerIndex, end).clear();
    lines.addAll(headerIndex, replacement);
  }
}
