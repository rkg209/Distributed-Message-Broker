package io.minikafka.chaos.check;

import io.minikafka.chaos.ChaosReport;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDate;
import java.util.List;

/**
 * Appends one row to each of the "Chaos / Fault-Injection Results" and "Network Partition Tests"
 * tables in {@code docs/results.md}, located by their header line so this survives edits elsewhere
 * in the file. Only runs that actually executed get written here — no placeholder numbers.
 */
public final class ResultsWriter {

  private static final String CHAOS_HEADER = "## Chaos / Fault-Injection Results";
  private static final String PARTITION_HEADER = "## Network Partition Tests";

  public void writeChaosRun(
      Path resultsFile,
      ChaosReport report,
      CheckResult loss,
      CheckResult duplication,
      CheckResult linearizability,
      CheckResult divergence)
      throws IOException {
    int nextRun = countExistingRuns(resultsFile, CHAOS_HEADER) + 1;
    String row =
        "| "
            + nextRun
            + " | "
            + LocalDate.now()
            + " | "
            + report.crashesInjected()
            + " | "
            + report.messagesSent()
            + " | "
            + summarize(loss)
            + " | "
            + summarize(duplication)
            + " | "
            + linearizability.status()
            + " | "
            + divergence.status()
            + " | "
            + overallResult(loss, duplication, linearizability, divergence)
            + " |";
    insertRowAfterTable(resultsFile, CHAOS_HEADER, row);
  }

  public void writeNetworkPartitionRun(
      Path resultsFile, ChaosReport report, boolean splitBrainDetected, CheckResult.Status result)
      throws IOException {
    int nextRun = countExistingRuns(resultsFile, PARTITION_HEADER) + 1;
    String row =
        "| "
            + nextRun
            + " | "
            + LocalDate.now()
            + " | "
            + report.partitionsInjected()
            + " | "
            + (splitBrainDetected ? "1" : "0")
            + " | "
            + result
            + " |";
    insertRowAfterTable(resultsFile, PARTITION_HEADER, row);
  }

  private static String summarize(CheckResult result) {
    return result.status() == CheckResult.Status.PASS
        ? "0"
        : Integer.toString(result.violations().size());
  }

  private static String overallResult(CheckResult... results) {
    for (CheckResult r : results) {
      if (r.status() != CheckResult.Status.PASS) {
        return r.status() == CheckResult.Status.UNKNOWN ? "UNKNOWN" : "FAIL";
      }
    }
    return "PASS";
  }

  private static int countExistingRuns(Path resultsFile, String header) throws IOException {
    List<String> lines = Files.readAllLines(resultsFile, StandardCharsets.UTF_8);
    int headerIndex = indexOfHeader(lines, header);
    int count = 0;
    for (int i = headerIndex + 1; i < lines.size(); i++) {
      String line = lines.get(i).strip();
      if (line.startsWith("##")) {
        break;
      }
      if (line.startsWith("| —") || line.startsWith("|—")) {
        continue; // the PENDING placeholder row doesn't count
      }
      if (line.startsWith("|")
          && !line.startsWith("|-")
          && !line.toLowerCase().contains("| run |")) {
        count++;
      }
    }
    return count;
  }

  private static void insertRowAfterTable(Path resultsFile, String header, String row)
      throws IOException {
    List<String> lines = Files.readAllLines(resultsFile, StandardCharsets.UTF_8);
    int headerIndex = indexOfHeader(lines, header);

    int placeholderIndex = -1;
    int lastRowIndex = -1;
    for (int i = headerIndex + 1; i < lines.size(); i++) {
      String line = lines.get(i).strip();
      if (line.startsWith("##")) {
        break;
      }
      if (line.startsWith("| —") || line.startsWith("|—")) {
        placeholderIndex = i;
      } else if (line.startsWith("|")
          && !line.startsWith("|-")
          && !line.toLowerCase().contains("| run |")) {
        lastRowIndex = i;
      }
    }

    if (placeholderIndex >= 0) {
      lines.set(placeholderIndex, row);
    } else if (lastRowIndex >= 0) {
      lines.add(lastRowIndex + 1, row);
    } else {
      throw new IllegalStateException("Could not locate a table under header: " + header);
    }
    Files.write(resultsFile, lines, StandardCharsets.UTF_8);
  }

  private static int indexOfHeader(List<String> lines, String header) {
    for (int i = 0; i < lines.size(); i++) {
      if (lines.get(i).strip().equals(header)) {
        return i;
      }
    }
    throw new IllegalStateException("Header not found in results file: " + header);
  }
}
