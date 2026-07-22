package io.minikafka.raft;

/**
 * Drives {@link RaftNode#replicateTo} for one peer on its own virtual thread: sends as fast as
 * there are entries to catch the peer up, then falls back to periodic empty heartbeats. Created on
 * {@code becomeLeader}, closed on step-down.
 */
final class ReplicationPipeline {

  private final RaftNode node;
  private final int peerId;
  private final RaftConfig config;

  private volatile Thread thread;
  private volatile boolean running;

  ReplicationPipeline(RaftNode node, int peerId, RaftConfig config) {
    this.node = node;
    this.peerId = peerId;
    this.config = config;
  }

  void start() {
    running = true;
    thread =
        Thread.ofVirtual()
            .name("raft-replicate-" + node.selfId() + "->" + peerId)
            .start(this::loop);
  }

  private void loop() {
    while (running) {
      RaftNode.ReplicationOutcome outcome = node.replicateTo(peerId);
      if (outcome == RaftNode.ReplicationOutcome.STOPPED) {
        return;
      }
      if (outcome == RaftNode.ReplicationOutcome.PROGRESSED) {
        continue; // more entries may remain; retry immediately
      }
      if (!sleep(config.heartbeatIntervalMs())) {
        return;
      }
    }
  }

  private boolean sleep(long ms) {
    try {
      Thread.sleep(ms);
      return true;
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      return false;
    }
  }

  void close() {
    running = false;
    Thread t = thread;
    if (t != null) {
      t.interrupt();
    }
  }
}
