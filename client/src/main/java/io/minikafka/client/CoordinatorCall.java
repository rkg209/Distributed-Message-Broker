package io.minikafka.client;

import io.minikafka.protocol.ErrorResp;
import io.minikafka.protocol.Message;
import io.minikafka.protocol.ProtocolException;
import java.io.EOFException;
import java.io.IOException;
import java.util.List;

/**
 * Sibling of {@link RedirectingCall} for group-coordination requests: instead of resolving a
 * partition leader, it resolves the group coordinator (the static controller broker). There is no
 * metadata call that names the controller up front, so the first attempt guesses an arbitrary known
 * broker and learns the real id from a {@code CODE_NOT_COORDINATOR} error's message, exactly
 * mirroring the {@code CODE_NOT_LEADER} redirect idiom.
 */
final class CoordinatorCall {

  /** A request that sends over a {@link BrokerConnection} and may throw {@link IOException}. */
  @FunctionalInterface
  interface Request {
    Message send(BrokerConnection connection) throws IOException;
  }

  private static final String NOT_COORDINATOR_PREFIX = "coordinator is ";

  private final ClusterClient clusterClient;
  private final int maxRetries;
  private final long retryBackoffMs;
  private volatile int coordinatorId;

  CoordinatorCall(ClusterClient clusterClient, int maxRetries, long retryBackoffMs) {
    this.clusterClient = clusterClient;
    this.maxRetries = maxRetries;
    this.retryBackoffMs = retryBackoffMs;
    this.coordinatorId = firstKnownBroker(clusterClient);
  }

  /** Sends one request, redirecting to the real coordinator and retrying on failover signals. */
  Message send(Request request) throws IOException {
    Exception lastError = null;
    for (int attempt = 0; attempt <= maxRetries; attempt++) {
      try {
        BrokerConnection conn = clusterClient.connectionTo(coordinatorId);
        Message response = request.send(conn);
        if (response instanceof ErrorResp err
            && err.errorCode() == ErrorResp.CODE_NOT_COORDINATOR) {
          lastError = new ProtocolException("Not coordinator: " + err.message());
          coordinatorId = parseCoordinatorId(err.message());
          backoff();
          continue;
        }
        return response;
      } catch (IOException e) {
        clusterClient.evict(coordinatorId);
        lastError = e;
        clusterClient.refresh();
        coordinatorId = firstKnownBroker(clusterClient);
        backoff();
      }
    }
    throw new ProtocolException(
        "Exhausted "
            + maxRetries
            + " coordinator redirect retries: "
            + (lastError == null ? "unknown error" : lastError.getMessage()),
        lastError);
  }

  private static int firstKnownBroker(ClusterClient clusterClient) {
    List<Integer> ids = clusterClient.brokerIds();
    if (ids.isEmpty()) {
      throw new IllegalStateException("No known brokers to guess a coordinator from");
    }
    return ids.get(0);
  }

  private static int parseCoordinatorId(String message) throws IOException {
    int idx = message.indexOf(NOT_COORDINATOR_PREFIX);
    if (idx < 0) {
      throw new ProtocolException("Cannot parse coordinator id from: " + message);
    }
    try {
      return Integer.parseInt(message.substring(idx + NOT_COORDINATOR_PREFIX.length()).trim());
    } catch (NumberFormatException e) {
      throw new ProtocolException("Cannot parse coordinator id from: " + message, e);
    }
  }

  private void backoff() throws IOException {
    try {
      Thread.sleep(retryBackoffMs);
    } catch (InterruptedException ie) {
      Thread.currentThread().interrupt();
      throw new EOFException("Interrupted while backing off before coordinator retry");
    }
  }
}
