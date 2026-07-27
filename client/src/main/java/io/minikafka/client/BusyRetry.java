package io.minikafka.client;

import io.minikafka.protocol.ErrorResp;
import io.minikafka.protocol.Message;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Retries a request with full-jitter exponential backoff while the broker keeps responding {@link
 * ErrorResp#CODE_BROKER_BUSY} — the partition's publish admission gate is saturated, not that the
 * broker is the wrong leader, so this deliberately does not touch {@link ClusterClient#refresh()}
 * (see {@link RedirectingCall}, which owns leadership redirects). Jitter matters: without it, N
 * producers rejected in the same instant would retry in lockstep and re-collide.
 */
final class BusyRetry {

  /** A request that returns a {@link Message} and may throw {@link IOException}. */
  @FunctionalInterface
  interface Call {
    Message send() throws IOException;
  }

  private final int maxBusyRetries;
  private final long backoffBaseMs;
  private final long backoffMaxMs;

  BusyRetry(int maxBusyRetries, long backoffBaseMs, long backoffMaxMs) {
    this.maxBusyRetries = maxBusyRetries;
    this.backoffBaseMs = backoffBaseMs;
    this.backoffMaxMs = backoffMaxMs;
  }

  Message send(Call call) throws IOException {
    for (int attempt = 0; ; attempt++) {
      Message response = call.send();
      if (!(response instanceof ErrorResp err && err.errorCode() == ErrorResp.CODE_BROKER_BUSY)) {
        return response;
      }
      if (attempt >= maxBusyRetries) {
        return response;
      }
      backoff(attempt);
    }
  }

  private void backoff(int attempt) throws IOException {
    long capped = Math.min(backoffMaxMs, backoffBaseMs << attempt);
    long sleepMs = ThreadLocalRandom.current().nextLong(capped + 1);
    try {
      Thread.sleep(sleepMs);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new InterruptedIOException("Interrupted during busy-retry backoff");
    }
  }
}
