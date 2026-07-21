package io.minikafka.broker;

/** Liveness state of a peer broker, as tracked by {@link HeartbeatMonitor}. */
public enum PeerState {
  /** No successful heartbeat exchange has happened yet. */
  UNKNOWN,
  /** Last heartbeat succeeded within the timeout window. */
  ALIVE,
  /** No successful heartbeat within {@code heartbeatTimeoutMs}. */
  SUSPECTED
}
