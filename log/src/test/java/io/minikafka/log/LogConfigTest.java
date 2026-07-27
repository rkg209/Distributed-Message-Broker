package io.minikafka.log;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.nio.file.Path;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class LogConfigTest {

  @Test
  void sevenArgConstructorDefaultsFsyncDelayToZero(@TempDir Path dir) {
    LogConfig config = new LogConfig(dir, FsyncPolicy.OS_MANAGED, 1000, 1024 * 1024, 4096, -1, -1);
    assertEquals(0, config.fsyncDelayMs());
  }

  @Test
  void defaultsForUsesZeroFsyncDelay(@TempDir Path dir) {
    assertEquals(0, LogConfig.defaultsFor(dir).fsyncDelayMs());
  }

  @Test
  void negativeFsyncDelayRejected(@TempDir Path dir) {
    assertThrows(
        IllegalArgumentException.class,
        () -> new LogConfig(dir, FsyncPolicy.OS_MANAGED, 1000, 1024 * 1024, 4096, -1, -1, -5));
  }

  @Test
  void positiveFsyncDelayAccepted(@TempDir Path dir) {
    LogConfig config =
        new LogConfig(dir, FsyncPolicy.OS_MANAGED, 1000, 1024 * 1024, 4096, -1, -1, 250);
    assertEquals(250, config.fsyncDelayMs());
  }
}
