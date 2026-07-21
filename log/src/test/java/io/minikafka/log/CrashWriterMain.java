package io.minikafka.log;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;

/**
 * Test fixture forked as a real subprocess by {@link CrashRecoveryTest}. Appends {@code count}
 * records with {@link FsyncPolicy#EVERY_WRITE}, printing each acknowledged offset to stdout, then
 * blocks forever so the parent can {@code destroyForcibly()} it (SIGKILL) — no shutdown hook, no
 * graceful flush.
 */
public final class CrashWriterMain {

  public static void main(String[] args) throws InterruptedException {
    Path dir = Path.of(args[0]);
    int count = Integer.parseInt(args[1]);

    LogConfig config = LogConfig.defaultsFor(dir);
    DiskPartitionLog log = new DiskPartitionLog(config);

    for (int i = 0; i < count; i++) {
      byte[] value = ("record-" + i).getBytes(StandardCharsets.UTF_8);
      AppendResult result = log.append(new LogRecord(0, 0, null, value));
      System.out.println(result.offset());
      System.out.flush();
    }

    Thread.currentThread().join();
  }

  private CrashWriterMain() {}
}
