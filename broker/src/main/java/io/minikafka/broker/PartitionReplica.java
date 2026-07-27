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
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

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
  private final MetadataService metadataService;
  private final long proposeTimeoutMs;
  private final long leaderWaitMs;

  // Set once via attachRaftNode: RaftNode's constructor requires this StateMachine, so the two
  // objects have a construction-order cycle that a plain final field can't express.
  private RaftNode raftNode;

  // RaftNode always replays committed entries from index 1 on every restart (it persists no
  // apply-progress marker of its own — see raft.RaftNode#lastApplied). This durable marker records
  // the highest Raft index whose apply() outcome was already decided in a prior run, so apply()
  // can skip re-deciding exactly those entries on replay. It must be index-based, not derived from
  // PartitionLog#nextOffset(): nextOffset() only advances on an APPEND outcome, so it undercounts
  // whenever a committed DUPLICATE or GAP entry (occupies a Raft index, appends nothing) is
  // interleaved with APPENDs — see AppliedIndexStore's javadoc.
  private final AppliedIndexStore appliedIndexStore;
  private final long lastAppliedIndexAtStartup;

  // Rebuilt from the recovered log (not Raft replay) in the constructor — every replica converges
  // on the same idempotency state independent of appliedIndexStore's replay-skip boundary.
  private final IdempotencyStore idempotency;

  /** Prefix on {@link ApplyResult#error()} identifying a {@link SequenceGapException}. */
  private static final String SEQUENCE_GAP_PREFIX = "SEQUENCE_GAP:";

  PartitionReplica(
      TopicPartition tp,
      FileRaftLogStore raftLogStore,
      PartitionLog partitionLog,
      BrokerRaftTransport transport,
      MetadataService metadataService,
      Path appliedIndexFile,
      long proposeTimeoutMs,
      long leaderWaitMs) {
    this.tp = tp;
    this.raftLogStore = raftLogStore;
    this.partitionLog = partitionLog;
    this.transport = transport;
    this.metadataService = metadataService;
    this.proposeTimeoutMs = proposeTimeoutMs;
    this.leaderWaitMs = leaderWaitMs;
    this.appliedIndexStore = new AppliedIndexStore(appliedIndexFile);
    this.lastAppliedIndexAtStartup = appliedIndexStore.lastAppliedIndexAtStartup();
    this.idempotency = IdempotencyStore.rebuildFrom(partitionLog);
  }

  /**
   * Completes construction once the {@link RaftNode} wrapping this state machine exists, and
   * subscribes {@link MetadataService} to push-style leadership-change notifications.
   */
  void attachRaftNode(RaftNode raftNode) {
    this.raftNode = raftNode;
    raftNode.onLeadershipChange(
        (term, leaderId) -> metadataService.onLeadershipChange(tp, leaderId, term));
  }

  /** This replica's current leader epoch — the Raft term, with no separate counter. */
  long currentLeaderEpoch() {
    return raftNode.currentTerm();
  }

  @Override
  public ApplyResult apply(long index, byte[] command) {
    if (index <= lastAppliedIndexAtStartup) {
      // Already durably decided in a prior run; no proposer is awaiting this replay, so the
      // returned outcome is never observed.
      return ApplyResult.ok(new byte[0]);
    }
    ApplyResult result;
    if (command.length == 0) {
      // The leader's per-term no-op entry (RaftNode.becomeLeader) — applying it as a record would
      // corrupt every replica's log.
      result = ApplyResult.ok(new byte[0]);
    } else {
      LogRecord decoded = PartitionCommandCodec.decode(command);
      IdempotencyStore.Verdict verdict = idempotency.check(decoded.producerId(), decoded.seqNo());
      result =
          switch (verdict.decision()) {
            case DUPLICATE -> ApplyResult.ok(encodeLong(verdict.cachedOffset()));
            case GAP -> ApplyResult.error(SEQUENCE_GAP_PREFIX + verdict.expectedSeq());
            case APPEND -> {
              AppendResult appendResult = partitionLog.append(decoded);
              idempotency.record(decoded.producerId(), decoded.seqNo(), appendResult.offset());
              yield ApplyResult.ok(encodeLong(appendResult.offset()));
            }
          };
    }
    // Ordered after partitionLog.append() above returns, so under the default EVERY_WRITE fsync
    // policy this marker is never durable ahead of the record it corresponds to.
    appliedIndexStore.record(index);
    return result;
  }

  /** The publish path: waits for leadership, proposes, and blocks until the entry is committed. */
  AppendResult append(long producerId, long seqNo, byte[] key, byte[] payload) {
    awaitLeadership();
    long timestamp = System.currentTimeMillis();
    byte[] command = PartitionCommandCodec.encode(timestamp, producerId, seqNo, key, payload);
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
      if (result.error().startsWith(SEQUENCE_GAP_PREFIX)) {
        long expectedSeq = Long.parseLong(result.error().substring(SEQUENCE_GAP_PREFIX.length()));
        throw new SequenceGapException(tp, producerId, expectedSeq, seqNo);
      }
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
    appliedIndexStore.close();
  }
}
