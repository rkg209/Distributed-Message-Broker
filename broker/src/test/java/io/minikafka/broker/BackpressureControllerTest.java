package io.minikafka.broker;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/** Unit tests for {@link BackpressureController}, independent of any broker wiring. */
class BackpressureControllerTest {

  private static final TopicPartition TP_A = new TopicPartition("orders", 0);
  private static final TopicPartition TP_B = new TopicPartition("orders", 1);

  @Test
  void enforcesCapacityAndTimesOutRatherThanBlockingForever() {
    BackpressureController controller = new BackpressureController(2, 50);

    assertTrue(controller.tryAcquire(TP_A));
    assertTrue(controller.tryAcquire(TP_A));
    assertEquals(2, controller.inFlight(TP_A));

    long start = System.nanoTime();
    boolean acquired = controller.tryAcquire(TP_A);
    long elapsedMs = (System.nanoTime() - start) / 1_000_000;

    assertFalse(acquired);
    assertTrue(elapsedMs >= 40, "should not return before roughly the acquire timeout elapsed");
  }

  @Test
  void releaseFreesAPermitForTheNextAcquire() {
    BackpressureController controller = new BackpressureController(1, 50);

    assertTrue(controller.tryAcquire(TP_A));
    assertFalse(controller.tryAcquire(TP_A));

    controller.release(TP_A);
    assertTrue(controller.tryAcquire(TP_A));
  }

  @Test
  void permitIsReleasedEvenWhenTheGuardedWorkThrows() {
    BackpressureController controller = new BackpressureController(1, 50);

    try {
      guardedWork(controller, TP_A, true);
    } catch (RuntimeException expected) {
      // ignore — asserting the finally-release below
    }

    assertEquals(0, controller.inFlight(TP_A));
    assertTrue(controller.tryAcquire(TP_A), "permit must not have leaked");
  }

  @Test
  void partitionsAreIsolated() {
    BackpressureController controller = new BackpressureController(1, 50);

    assertTrue(controller.tryAcquire(TP_A));
    assertFalse(controller.tryAcquire(TP_A));
    // A saturated partition A must not affect partition B's independent permit pool.
    assertTrue(controller.tryAcquire(TP_B));
  }

  @Test
  @Timeout(5)
  void interruptedAcquireIsTreatedAsAFailedAcquireAndRestoresTheFlag() throws InterruptedException {
    BackpressureController controller = new BackpressureController(1, 2000);
    assertTrue(controller.tryAcquire(TP_A));

    CountDownLatch started = new CountDownLatch(1);
    boolean[] result = new boolean[1];
    boolean[] interruptFlagRestored = new boolean[1];
    Thread waiter =
        new Thread(
            () -> {
              started.countDown();
              result[0] = controller.tryAcquire(TP_A);
              interruptFlagRestored[0] = Thread.currentThread().isInterrupted();
            });
    waiter.start();
    started.await();
    Thread.sleep(50);
    waiter.interrupt();
    waiter.join(TimeUnit.SECONDS.toMillis(2));

    assertFalse(result[0]);
    assertTrue(interruptFlagRestored[0]);
  }

  private void guardedWork(BackpressureController controller, TopicPartition tp, boolean fail) {
    if (!controller.tryAcquire(tp)) {
      throw new IllegalStateException("could not acquire");
    }
    try {
      if (fail) {
        throw new RuntimeException("simulated work failure");
      }
    } finally {
      controller.release(tp);
    }
  }
}
