package io.minikafka.client;

import io.minikafka.protocol.ErrorResp;
import io.minikafka.protocol.Message;
import io.minikafka.protocol.ProtocolException;
import java.io.EOFException;
import java.io.IOException;

/**
 * Shared retry logic for {@link ProducerClient} and {@link ConsumerClient} when constructed over a
 * {@link ClusterClient}: on {@code CODE_NOT_LEADER} or a broken socket (the old leader's connection
 * dies on crash), refreshes cluster metadata and retries against the newly resolved leader. Bounded
 * by {@code maxRetries} with a fixed backoff between attempts — externalized per CLAUDE.md rather
 * than hardcoded, since a chaos harness or slow election needs more headroom than a unit test.
 *
 * <p><b>Known gap (closed by Spec 09):</b> a retry triggered by an {@link IOException} — as opposed
 * to a {@code CODE_NOT_LEADER} response — cannot distinguish "the request never reached the broker"
 * from "it committed but the response never made it back before the socket died." Producer retries
 * are therefore not yet exactly-once: a publish that actually committed just before a crash can be
 * re-sent and committed a second time under a fresh offset. Spec 09's idempotent producer
 * (producer-id + per-partition sequence, deduped before {@code RaftNode.propose()}) closes this;
 * until then, callers must not assert exactly-once delivery across an {@code IOException} retry.
 */
final class RedirectingCall {

  /** A request that sends over a {@link BrokerConnection} and may throw {@link IOException}. */
  @FunctionalInterface
  interface Request {
    Message send(BrokerConnection connection) throws IOException;
  }

  private final ClusterClient clusterClient;
  private final String topic;
  private final int partition;
  private final int maxRetries;
  private final long retryBackoffMs;

  RedirectingCall(
      ClusterClient clusterClient,
      String topic,
      int partition,
      int maxRetries,
      long retryBackoffMs) {
    this.clusterClient = clusterClient;
    this.topic = topic;
    this.partition = partition;
    this.maxRetries = maxRetries;
    this.retryBackoffMs = retryBackoffMs;
  }

  /** Sends one request, redirecting to the current leader and retrying on failover signals. */
  Message send(Request request) throws IOException {
    Exception lastError = null;
    for (int attempt = 0; attempt <= maxRetries; attempt++) {
      try {
        BrokerConnection conn = clusterClient.connectionFor(topic, partition);
        Message response = request.send(conn);
        if (response instanceof ErrorResp err && err.errorCode() == ErrorResp.CODE_NOT_LEADER) {
          lastError = new ProtocolException("Not leader for " + topic + "/" + partition);
          redirectAndBackoff();
          continue;
        }
        return response;
      } catch (IOException e) {
        lastError = e;
        redirectAndBackoff();
      }
    }
    throw new ProtocolException(
        "Exhausted "
            + maxRetries
            + " redirect retries for "
            + topic
            + "/"
            + partition
            + ": "
            + (lastError == null ? "unknown error" : lastError.getMessage()),
        lastError);
  }

  private void redirectAndBackoff() throws IOException {
    clusterClient.refresh();
    try {
      Thread.sleep(retryBackoffMs);
    } catch (InterruptedException ie) {
      Thread.currentThread().interrupt();
      throw new EOFException("Interrupted while backing off before redirect retry");
    }
  }
}
