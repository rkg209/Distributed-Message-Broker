package io.minikafka.raft;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.LongSupplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A single Raft consensus participant. All state transitions occur under one coarse lock; the
 * election timer and one {@link ReplicationPipeline} per peer run on virtual threads and take the
 * lock to mutate state. See Raft (Ongaro &amp; Ousterhout, 2014) §5 for the algorithm this
 * implements.
 */
public final class RaftNode implements AutoCloseable {

  private static final Logger log = LoggerFactory.getLogger(RaftNode.class);

  /** Outcome of one {@link #replicateTo} attempt, driving a {@link ReplicationPipeline}'s loop. */
  enum ReplicationOutcome {
    PROGRESSED,
    HEARTBEAT,
    RETRY,
    STOPPED
  }

  private final int selfId;
  private final List<Integer> peerIds;
  private final RaftConfig config;
  private final RaftLogStore logStore;
  private final PersistentState persistentState;
  private final RaftTransport transport;
  private final StateMachine stateMachine;
  private final LongSupplier clockNanos;

  private final ReentrantLock lock = new ReentrantLock();
  private final ElectionTimer electionTimer;
  private final Map<Integer, ReplicationPipeline> pipelines = new HashMap<>();
  private final Map<Integer, Long> nextIndex = new HashMap<>();
  private final Map<Integer, Long> matchIndex = new HashMap<>();
  private final NavigableMap<Long, CompletableFuture<ApplyResult>> pendingProposals =
      new TreeMap<>();
  private final Set<Integer> votesGranted = new HashSet<>();

  private RaftRole role = RaftRole.FOLLOWER;
  private long commitIndex = 0;
  private long lastApplied = 0;
  private int leaderId = PersistentState.NONE;
  private boolean started = false;
  private volatile java.util.function.BiConsumer<Long, Integer> leaderListener;

  /** Test hook: invoked with {@code (term, selfId)} whenever this node becomes leader. */
  void setLeaderListener(java.util.function.BiConsumer<Long, Integer> listener) {
    this.leaderListener = listener;
  }

  public RaftNode(
      int selfId,
      List<Integer> peerIds,
      RaftConfig config,
      RaftLogStore logStore,
      PersistentState persistentState,
      RaftTransport transport,
      StateMachine stateMachine,
      LongSupplier clockNanos) {
    this.selfId = selfId;
    this.peerIds = List.copyOf(peerIds);
    this.config = config;
    this.logStore = logStore;
    this.persistentState = persistentState;
    this.transport = transport;
    this.stateMachine = stateMachine;
    this.clockNanos = clockNanos;
    this.electionTimer = new ElectionTimer(config, clockNanos, this::startElection);
  }

  public void start() {
    lock.lock();
    try {
      if (started) {
        throw new IllegalStateException("RaftNode " + selfId + " already started");
      }
      started = true;
      electionTimer.start();
    } finally {
      lock.unlock();
    }
  }

  @Override
  public void close() {
    lock.lock();
    try {
      electionTimer.close();
      for (ReplicationPipeline pipeline : pipelines.values()) {
        pipeline.close();
      }
      pipelines.clear();
    } finally {
      lock.unlock();
    }
  }

  // ---- observability ----

  public RaftRole role() {
    lock.lock();
    try {
      return role;
    } finally {
      lock.unlock();
    }
  }

  public long currentTerm() {
    return persistentState.currentTerm();
  }

  public long commitIndex() {
    lock.lock();
    try {
      return commitIndex;
    } finally {
      lock.unlock();
    }
  }

  public long lastApplied() {
    lock.lock();
    try {
      return lastApplied;
    } finally {
      lock.unlock();
    }
  }

  public int leaderId() {
    lock.lock();
    try {
      return leaderId;
    } finally {
      lock.unlock();
    }
  }

  public int selfId() {
    return selfId;
  }

  // ---- client API ----

  public CompletableFuture<ApplyResult> propose(byte[] command) {
    lock.lock();
    try {
      if (role != RaftRole.LEADER) {
        return CompletableFuture.failedFuture(new NotLeaderException(leaderId));
      }
      long index = logStore.lastIndex() + 1;
      RaftEntry entry = new RaftEntry(persistentState.currentTerm(), index, command);
      logStore.append(entry);
      matchIndex.put(selfId, index);
      CompletableFuture<ApplyResult> future = new CompletableFuture<>();
      pendingProposals.put(index, future);
      maybeAdvanceCommit();
      return future;
    } finally {
      lock.unlock();
    }
  }

  // ---- RPC handlers ----

  public AppendEntriesResponse handleAppendEntries(AppendEntriesRequest req) {
    lock.lock();
    try {
      if (req.term() < persistentState.currentTerm()) {
        return new AppendEntriesResponse(
            persistentState.currentTerm(), false, 0, 0, logStore.lastIndex());
      }
      if (req.term() > persistentState.currentTerm()) {
        stepDownToFollower(req.term(), PersistentState.NONE);
      } else if (role == RaftRole.LEADER) {
        // Defensive: this should be unreachable under election safety (two leaders can never
        // share a term), but if it ever is, a bare `role = FOLLOWER` here would silently leak
        // running ReplicationPipelines, leave the election timer permanently suppressed, and
        // never fail pending propose() futures. Demote through the same path stepDownToFollower
        // uses, just without bumping the term.
        stepDownToFollower(persistentState.currentTerm(), persistentState.votedFor());
      } else if (role == RaftRole.CANDIDATE) {
        role = RaftRole.FOLLOWER;
      }
      leaderId = req.leaderId();
      electionTimer.reset();

      if (req.prevLogIndex() > 0) {
        RaftEntry prev = logStore.get(req.prevLogIndex());
        if (prev == null) {
          return new AppendEntriesResponse(
              persistentState.currentTerm(),
              false,
              logStore.lastIndex() + 1,
              0,
              logStore.lastIndex());
        }
        if (prev.term() != req.prevLogTerm()) {
          long conflictTerm = prev.term();
          long conflictIndex = firstIndexOfTerm(conflictTerm, req.prevLogIndex());
          return new AppendEntriesResponse(
              persistentState.currentTerm(),
              false,
              conflictIndex,
              conflictTerm,
              logStore.lastIndex());
        }
      }

      long lastNewIndex = req.prevLogIndex();
      for (RaftEntry entry : req.entries()) {
        RaftEntry existing = logStore.get(entry.index());
        if (existing == null) {
          logStore.append(entry);
        } else if (existing.term() != entry.term()) {
          logStore.truncateFrom(entry.index());
          logStore.append(entry);
        } // else: identical entry already present — do not touch it
        lastNewIndex = entry.index();
      }

      if (req.leaderCommit() > commitIndex) {
        commitIndex = Math.min(req.leaderCommit(), lastNewIndex);
        applyCommitted();
      }
      return new AppendEntriesResponse(
          persistentState.currentTerm(), true, 0, 0, logStore.lastIndex());
    } finally {
      lock.unlock();
    }
  }

  public RequestVoteResponse handleRequestVote(RequestVoteRequest req) {
    lock.lock();
    try {
      if (req.term() < persistentState.currentTerm()) {
        return new RequestVoteResponse(persistentState.currentTerm(), false);
      }
      if (req.term() > persistentState.currentTerm()) {
        stepDownToFollower(req.term(), PersistentState.NONE);
      }
      int votedFor = persistentState.votedFor();
      boolean logUpToDate = isLogUpToDate(req.lastLogTerm(), req.lastLogIndex());
      if ((votedFor == PersistentState.NONE || votedFor == req.candidateId()) && logUpToDate) {
        persistentState.save(persistentState.currentTerm(), req.candidateId());
        electionTimer.reset();
        return new RequestVoteResponse(persistentState.currentTerm(), true);
      }
      return new RequestVoteResponse(persistentState.currentTerm(), false);
    } finally {
      lock.unlock();
    }
  }

  private boolean isLogUpToDate(long candidateLastLogTerm, long candidateLastLogIndex) {
    long myLastTerm = logStore.lastTerm();
    long myLastIndex = logStore.lastIndex();
    if (candidateLastLogTerm != myLastTerm) {
      return candidateLastLogTerm > myLastTerm;
    }
    return candidateLastLogIndex >= myLastIndex;
  }

  private long firstIndexOfTerm(long term, long fromIndex) {
    long idx = fromIndex;
    while (idx > logStore.firstIndex()) {
      RaftEntry prior = logStore.get(idx - 1);
      if (prior == null || prior.term() != term) {
        break;
      }
      idx--;
    }
    return idx;
  }

  // ---- election ----

  /** Invoked by {@link ElectionTimer} on its own virtual thread. */
  private void startElection() {
    long electionTerm;
    RequestVoteRequest req;
    List<Integer> targets;
    lock.lock();
    try {
      if (role == RaftRole.LEADER) {
        return;
      }
      electionTerm = persistentState.currentTerm() + 1;
      persistentState.save(electionTerm, selfId);
      role = RaftRole.CANDIDATE;
      leaderId = PersistentState.NONE;
      votesGranted.clear();
      votesGranted.add(selfId);
      electionTimer.reset();
      req = new RequestVoteRequest(electionTerm, selfId, logStore.lastIndex(), logStore.lastTerm());
      targets = peerIds;
      log.info("Node {} starting election for term {}", selfId, electionTerm);
    } finally {
      lock.unlock();
    }

    for (int peer : targets) {
      transport
          .requestVote(peer, req)
          .whenComplete((resp, err) -> handleVoteResponse(electionTerm, peer, resp, err));
    }
  }

  private void handleVoteResponse(
      long electionTerm, int peer, RequestVoteResponse resp, Throwable err) {
    if (err != null) {
      log.debug("RequestVote to {} failed: {}", peer, err.toString());
      return;
    }
    lock.lock();
    try {
      if (resp.term() > persistentState.currentTerm()) {
        stepDownToFollower(resp.term(), PersistentState.NONE);
        return;
      }
      if (role != RaftRole.CANDIDATE || persistentState.currentTerm() != electionTerm) {
        return; // stale response from a bygone election
      }
      if (resp.voteGranted()) {
        votesGranted.add(peer);
        if (votesGranted.size() >= majoritySize()) {
          becomeLeader();
        }
      }
    } finally {
      lock.unlock();
    }
  }

  private int majoritySize() {
    return (peerIds.size() + 1) / 2 + 1;
  }

  /** Must be called with {@link #lock} held. */
  private void becomeLeader() {
    role = RaftRole.LEADER;
    leaderId = selfId;
    electionTimer.suppress();
    log.info("Node {} became leader for term {}", selfId, persistentState.currentTerm());
    java.util.function.BiConsumer<Long, Integer> listener = leaderListener;
    if (listener != null) {
      listener.accept(persistentState.currentTerm(), selfId);
    }

    nextIndex.clear();
    matchIndex.clear();
    long lastIndex = logStore.lastIndex();
    for (int peer : peerIds) {
      nextIndex.put(peer, lastIndex + 1);
      matchIndex.put(peer, 0L);
    }
    matchIndex.put(selfId, lastIndex);

    // §5.4.2: commit a no-op entry from the new term before anything from the leader can commit.
    long noopIndex = logStore.lastIndex() + 1;
    logStore.append(new RaftEntry(persistentState.currentTerm(), noopIndex, new byte[0]));
    matchIndex.put(selfId, noopIndex);
    for (int peer : peerIds) {
      nextIndex.put(peer, noopIndex);
    }

    pipelines.clear();
    for (int peer : peerIds) {
      ReplicationPipeline pipeline = new ReplicationPipeline(this, peer, config);
      pipelines.put(peer, pipeline);
      pipeline.start();
    }
    maybeAdvanceCommit();
  }

  /** Must be called with {@link #lock} held. Persists the term change and fails all proposals. */
  private void stepDownToFollower(long newTerm, int votedFor) {
    persistentState.save(newTerm, votedFor);
    if (role == RaftRole.LEADER) {
      for (ReplicationPipeline pipeline : pipelines.values()) {
        pipeline.close();
      }
      pipelines.clear();
    }
    boolean wasLeader = role == RaftRole.LEADER;
    role = RaftRole.FOLLOWER;
    leaderId = PersistentState.NONE;
    votesGranted.clear();
    if (wasLeader) {
      electionTimer.resume();
    }
    failAllPending();
  }

  private void failAllPending() {
    for (CompletableFuture<ApplyResult> future : pendingProposals.values()) {
      future.completeExceptionally(new NotLeaderException(leaderId));
    }
    pendingProposals.clear();
  }

  // ---- commit + apply ----

  /** Must be called with {@link #lock} held. */
  private void maybeAdvanceCommit() {
    if (role != RaftRole.LEADER) {
      return;
    }
    long newCommit =
        CommitAdvancer.advance(
            persistentState.currentTerm(), matchIndex.values(), logStore, commitIndex);
    if (newCommit > commitIndex) {
      commitIndex = newCommit;
      applyCommitted();
    }
  }

  /** Must be called with {@link #lock} held. */
  private void applyCommitted() {
    while (lastApplied < commitIndex) {
      lastApplied++;
      RaftEntry entry = logStore.get(lastApplied);
      ApplyResult result = stateMachine.apply(lastApplied, entry.command());
      CompletableFuture<ApplyResult> future = pendingProposals.remove(lastApplied);
      if (future != null) {
        future.complete(result);
      }
    }
  }

  // ---- replication (invoked by ReplicationPipeline virtual threads) ----

  ReplicationOutcome replicateTo(int peerId) {
    long capturedTerm;
    AppendEntriesRequest req;
    lock.lock();
    try {
      if (role != RaftRole.LEADER) {
        return ReplicationOutcome.STOPPED;
      }
      capturedTerm = persistentState.currentTerm();
      long ni = nextIndex.getOrDefault(peerId, logStore.lastIndex() + 1);
      long prevLogIndex = ni - 1;
      long prevLogTerm = prevLogIndex <= 0 ? 0 : safeTerm(prevLogIndex);
      List<RaftEntry> entries = logStore.getFrom(ni, config.maxEntriesPerAppend());
      req =
          new AppendEntriesRequest(
              capturedTerm, selfId, prevLogIndex, prevLogTerm, entries, commitIndex);
    } finally {
      lock.unlock();
    }

    AppendEntriesResponse resp;
    try {
      resp = transport.appendEntries(peerId, req).get(config.rpcTimeoutMs(), TimeUnit.MILLISECONDS);
    } catch (TimeoutException e) {
      return ReplicationOutcome.RETRY;
    } catch (ExecutionException | InterruptedException e) {
      if (e instanceof InterruptedException) {
        Thread.currentThread().interrupt();
      }
      log.debug("AppendEntries to {} failed: {}", peerId, e.toString());
      return ReplicationOutcome.RETRY;
    }

    lock.lock();
    try {
      if (role != RaftRole.LEADER || persistentState.currentTerm() != capturedTerm) {
        return ReplicationOutcome.STOPPED;
      }
      if (resp.term() > capturedTerm) {
        stepDownToFollower(resp.term(), PersistentState.NONE);
        return ReplicationOutcome.STOPPED;
      }
      if (resp.success()) {
        nextIndex.put(peerId, resp.followerLastIndex() + 1);
        matchIndex.put(peerId, resp.followerLastIndex());
        maybeAdvanceCommit();
        return req.entries().isEmpty()
            ? ReplicationOutcome.HEARTBEAT
            : ReplicationOutcome.PROGRESSED;
      } else {
        long backedOff = backOffNextIndex(resp);
        nextIndex.put(peerId, Math.max(1, backedOff));
        return ReplicationOutcome.RETRY;
      }
    } finally {
      lock.unlock();
    }
  }

  private long safeTerm(long index) {
    RaftEntry e = logStore.get(index);
    return e == null ? 0 : e.term();
  }

  /** Must be called with {@link #lock} held. */
  private long backOffNextIndex(AppendEntriesResponse resp) {
    if (resp.conflictTerm() == 0) {
      return resp.conflictIndex();
    }
    long idx = logStore.lastIndex();
    while (idx > 0) {
      RaftEntry e = logStore.get(idx);
      if (e != null && e.term() == resp.conflictTerm()) {
        return idx + 1;
      }
      idx--;
    }
    return resp.conflictIndex();
  }
}
