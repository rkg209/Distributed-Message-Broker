package io.minikafka.broker;

import io.minikafka.protocol.BrokerInfo;
import io.minikafka.protocol.HeartbeatReq;
import io.minikafka.protocol.HeartbeatResp;
import io.minikafka.protocol.Message;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongSupplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Sends periodic heartbeats to every peer broker on its own virtual thread and tracks each peer's
 * {@link PeerState} based on how recently a heartbeat succeeded. Every state transition is logged
 * at INFO, since failure detection is asserted against broker logs in tests and in the Docker
 * cluster.
 */
public final class HeartbeatMonitor implements AutoCloseable {

  private static final Logger log = LoggerFactory.getLogger(HeartbeatMonitor.class);

  private final BrokerInfo self;
  private final List<BrokerInfo> peers;
  private final long heartbeatIntervalMs;
  private final long heartbeatTimeoutMs;
  private final LongSupplier nanoClock;
  private final Map<Integer, PeerConnection> connections = new ConcurrentHashMap<>();
  private final Map<Integer, PeerState> states = new ConcurrentHashMap<>();
  private final Map<Integer, Long> lastSuccessNanos = new ConcurrentHashMap<>();
  private final AtomicLong correlationCounter = new AtomicLong();

  private Thread[] senderThreads;
  private volatile boolean running;

  public HeartbeatMonitor(
      BrokerInfo self,
      List<BrokerInfo> peers,
      long heartbeatIntervalMs,
      long heartbeatTimeoutMs,
      long peerReconnectBackoffMs) {
    this(
        self,
        peers,
        heartbeatIntervalMs,
        heartbeatTimeoutMs,
        System::nanoTime,
        peers.stream()
            .collect(
                java.util.stream.Collectors.toMap(
                    BrokerInfo::brokerId,
                    p -> new PeerConnection(p, heartbeatTimeoutMs, peerReconnectBackoffMs))));
  }

  HeartbeatMonitor(
      BrokerInfo self,
      List<BrokerInfo> peers,
      long heartbeatIntervalMs,
      long heartbeatTimeoutMs,
      LongSupplier nanoClock,
      Map<Integer, PeerConnection> connections) {
    this.self = self;
    this.peers = List.copyOf(peers);
    this.heartbeatIntervalMs = heartbeatIntervalMs;
    this.heartbeatTimeoutMs = heartbeatTimeoutMs;
    this.nanoClock = nanoClock;
    this.connections.putAll(connections);
    for (BrokerInfo peer : peers) {
      states.put(peer.brokerId(), PeerState.UNKNOWN);
    }
  }

  /** Starts one virtual thread per peer that heartbeats it every {@code heartbeatIntervalMs}. */
  public synchronized void start() {
    if (running) {
      throw new IllegalStateException("Already started");
    }
    running = true;
    senderThreads = new Thread[peers.size()];
    for (int i = 0; i < peers.size(); i++) {
      BrokerInfo peer = peers.get(i);
      senderThreads[i] =
          Thread.ofVirtual()
              .name("heartbeat-" + self.brokerId() + "->" + peer.brokerId())
              .start(() -> heartbeatLoop(peer));
    }
  }

  private void heartbeatLoop(BrokerInfo peer) {
    while (running) {
      sendOnce(peer);
      checkTimeout(peer);
      try {
        Thread.sleep(heartbeatIntervalMs);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        return;
      }
    }
  }

  private void sendOnce(BrokerInfo peer) {
    PeerConnection connection = connections.get(peer.brokerId());
    HeartbeatReq req = new HeartbeatReq(correlationCounter.incrementAndGet(), 0L, self.brokerId());
    try {
      Message response = connection.send(req);
      if (response instanceof HeartbeatResp) {
        lastSuccessNanos.put(peer.brokerId(), nanoClock.getAsLong());
        markAlive(peer);
      }
    } catch (IOException e) {
      log.debug("Heartbeat to peer {} failed: {}", peer.brokerId(), e.getMessage());
    }
  }

  private void checkTimeout(BrokerInfo peer) {
    Long lastSuccess = lastSuccessNanos.get(peer.brokerId());
    if (lastSuccess == null) {
      return; // never succeeded yet: stays UNKNOWN until first contact
    }
    long elapsedMs = (nanoClock.getAsLong() - lastSuccess) / 1_000_000L;
    if (elapsedMs >= heartbeatTimeoutMs) {
      markSuspected(peer, elapsedMs);
    }
  }

  private void markAlive(BrokerInfo peer) {
    PeerState previous = states.put(peer.brokerId(), PeerState.ALIVE);
    if (previous != PeerState.ALIVE) {
      log.info("Peer {} ALIVE", peer.brokerId());
    }
  }

  private void markSuspected(BrokerInfo peer, long elapsedMs) {
    PeerState previous = states.put(peer.brokerId(), PeerState.SUSPECTED);
    if (previous != PeerState.SUSPECTED) {
      log.info("Peer {} SUSPECTED after {}ms without response", peer.brokerId(), elapsedMs);
    }
  }

  /** The current liveness state of {@code brokerId}, or {@link PeerState#UNKNOWN} if not a peer. */
  public PeerState stateOf(int brokerId) {
    return states.getOrDefault(brokerId, PeerState.UNKNOWN);
  }

  /** A snapshot of every peer's current liveness state. */
  public Map<Integer, PeerState> peerStates() {
    return Map.copyOf(states);
  }

  @Override
  public synchronized void close() {
    running = false;
    if (senderThreads != null) {
      for (Thread t : senderThreads) {
        t.interrupt();
      }
      for (Thread t : senderThreads) {
        try {
          t.join(2000);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
        }
      }
    }
    for (PeerConnection connection : connections.values()) {
      connection.close();
    }
  }
}
