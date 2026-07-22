package io.minikafka.broker;

import io.minikafka.log.AppendResult;
import io.minikafka.log.LogRecord;
import io.minikafka.log.PartitionLog;
import io.minikafka.raft.ApplyResult;
import io.minikafka.raft.FileRaftLogStore;
import io.minikafka.raft.NotLeaderException;
import io.minikafka.raft.PersistentState;
import io.minikafka.raft.RaftNode;
import io.minikafka.raft.RaftRole;
import io.minikafka.raft.StateMachine;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Owns one {@link RaftNode} + one {@link PartitionLog} + one {@link FileRaftLogStore} + {@link
 * PersistentState} + one {@link BrokerRaftTransport} for a single {@link TopicPartition}. The Raft
 * log and the state machine's durable log are deliberately separate stores (see Spec 07 plan): the
 * {@link PartitionLog} is written *only* from {@link #apply}, so a follower physically cannot serve
 * uncommitted data — this makes INV-5 structural rather than a runtime check.
 */
final class PartitionReplica implements StateMachine, AutoCloseable {

  private final TopicPartition tp;
  private final FileRaftLogStore raftLogStore;
  private final PartitionLog partitionLog;
  private final BrokerRaftTransport transport;
  private final long proposeTimeoutMs;
  private final long leaderWaitMs;

  // Set once via attachRaftNode: RaftNode's constructor requires this StateMachine, so the two
  // objects have a construction-order cycle that a plain final field can't express.
  private RaftNode raftNode;

  // RaftNode always replays committed entries from index 1 on every restart (it persists no
  // apply-progress marker of its own — see raft.RaftNode#lastApplied). The durable PartitionLog,
  // however, already recovered whatever it had previously applied. This counter — how many
  // non-empty entries were already applied before this run — lets apply() skip re-appending
  // exactly that many historical entries so a restart doesn't duplicate committed records
  // (INV-3). It MUST be nextOffset() alone, not nextOffset() - firstOffset(): nextOffset() is the
  // total count of records ever assigned (monotonic, offsets are 0-based and sequential), while
  // firstOffset() shrinks the "count" once retention deletes old segments — subtracting it would
  // under-count already-applied entries and re-duplicate the retained tail on restart.
  private final AtomicLong skipRemaining;

  PartitionReplica(
      TopicPartition tp,
      FileRaftLogStore raftLogStore,
      PartitionLog partitionLog,
      BrokerRaftTransport transport,
      long proposeTimeoutMs,
      long leaderWaitMs) {
    this.tp = tp;
    this.raftLogStore = raftLogStore;
    this.partitionLog = partitionLog;
    this.transport = transport;
    this.proposeTimeoutMs = proposeTimeoutMs;
    this.leaderWaitMs = leaderWaitMs;
    this.skipRemaining = new AtomicLong(partitionLog.nextOffset());
  }

  /** Completes construction once the {@link RaftNode} wrapping this state machine exists. */
  void attachRaftNode(RaftNode raftNode) {
    this.raftNode = raftNode;
  }

  @Override
  public ApplyResult apply(long index, byte[] command) {
    if (command.length == 0) {
      // The leader's per-term no-op entry (RaftNode.becomeLeader) — applying it as a record would
      // corrupt every replica's log.
      return ApplyResult.ok(new byte[0]);
    }
    if (skipRemaining.getAndUpdate(n -> n > 0 ? n - 1 : 0) > 0) {
      // Already durably applied in a prior run; no proposer is awaiting this replay, so the
      // returned offset is never observed.
      return ApplyResult.ok(new byte[0]);
    }
    LogRecord decoded = PartitionCommandCodec.decode(command);
    AppendResult result = partitionLog.append(decoded);
    return ApplyResult.ok(encodeLong(result.offset()));
  }

  /** The publish path: waits for leadership, proposes, and blocks until the entry is committed. */
  AppendResult append(byte[] key, byte[] payload) {
    awaitLeadership();
    long timestamp = System.currentTimeMillis();
    byte[] command = PartitionCommandCodec.encode(timestamp, key, payload);
    CompletableFuture<ApplyResult> future = raftNode.propose(command);
    ApplyResult result;
    try {
      result = future.get(proposeTimeoutMs, TimeUnit.MILLISECONDS);
    } catch (TimeoutException e) {
      throw new IllegalStateException(
          "Publish to " + tp + " timed out waiting for commit after " + proposeTimeoutMs + "ms", e);
    } catch (ExecutionException e) {
      if (e.getCause() instanceof NotLeaderException nle) {
        throw nle;
      }
      throw new IllegalStateException("Publish to " + tp + " failed to commit", e.getCause());
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IllegalStateException("Publish to " + tp + " interrupted while awaiting commit", e);
    }
    if (!result.isOk()) {
      throw new IllegalStateException("Publish to " + tp + " failed to apply: " + result.error());
    }
    return new AppendResult(decodeLong(result.value()), timestamp);
  }

  List<LogRecord> read(long offset, int maxBytes) {
    return partitionLog.read(offset, maxBytes);
  }

  private void awaitLeadership() {
    long deadline = System.nanoTime() + leaderWaitMs * 1_000_000L;
    while (raftNode.role() != RaftRole.LEADER) {
      if (System.nanoTime() >= deadline) {
        throw new NotLeaderException(raftNode.leaderId());
      }
      try {
        Thread.sleep(5);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new NotLeaderException(raftNode.leaderId());
      }
    }
  }

  private static byte[] encodeLong(long value) {
    return ByteBuffer.allocate(Long.BYTES).putLong(value).array();
  }

  private static long decodeLong(byte[] bytes) {
    return ByteBuffer.wrap(bytes).getLong();
  }

  RaftNode raftNode() {
    return raftNode;
  }

  PartitionLog partitionLog() {
    return partitionLog;
  }

  boolean isLeader() {
    return raftNode.role() == RaftRole.LEADER;
  }

  int leaderId() {
    return raftNode.leaderId();
  }

  long commitIndex() {
    return raftNode.commitIndex();
  }

  void start() {
    raftNode.start();
  }

  @Override
  public void close() {
    raftNode.close();
    raftLogStore.close();
    transport.close();
    partitionLog.close();
  }
}
