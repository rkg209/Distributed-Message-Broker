package io.minikafka.broker;

import io.minikafka.protocol.GroupHeartbeatReq;
import io.minikafka.protocol.GroupHeartbeatResp;
import io.minikafka.protocol.JoinGroupReq;
import io.minikafka.protocol.JoinGroupResp;
import io.minikafka.protocol.LeaveGroupReq;
import io.minikafka.protocol.LeaveGroupResp;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.LongSupplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Broker-side coordinator for dynamic consumer-group rebalancing, hosted on the static controller
 * broker only (see {@link MetadataService#isController()}). Runs a generation-based eager rebalance
 * with an ack barrier: a membership change bumps the group's generation and recomputes a {@link
 * RangeAssignor} assignment, but no member adopts its new partitions until every member has
 * acknowledged (by pausing, committing its revoked partitions, and re-heartbeating at the new
 * generation) — see {@code specs/13-dynamic-rebalancing.md} for the full design.
 *
 * <p>Membership is in-memory only: a coordinator crash or restart drops all groups, and members
 * simply re-join on their next heartbeat. Nothing is lost because committed offsets are durable in
 * {@link ConsumerGroupManager} regardless of group membership.
 *
 * <p>Fencing here is protocol-barrier-based, not server-enforced: {@code CommitOffsetReq} carries
 * no generation, so a partitioned member that keeps polling past its revocation is only stopped by
 * session-timeout eviction, not by rejection at the commit path. That is a deliberate scope
 * narrowing for a stretch spec, not an oversight.
 */
public final class GroupCoordinator implements AutoCloseable {

  private static final Logger log = LoggerFactory.getLogger(GroupCoordinator.class);
  private static final long REAPER_POLL_INTERVAL_MS = 200;

  private final MetadataService metadataService;
  private final long heartbeatIntervalMs;
  private final long sessionTimeoutMs;
  private final long rebalanceTimeoutMs;
  private final LongSupplier nanoClock;
  private final Map<String, GroupState> groups = new ConcurrentHashMap<>();

  private volatile boolean running;
  private Thread reaperThread;

  public GroupCoordinator(
      MetadataService metadataService,
      long heartbeatIntervalMs,
      long sessionTimeoutMs,
      long rebalanceTimeoutMs) {
    this(
        metadataService,
        heartbeatIntervalMs,
        sessionTimeoutMs,
        rebalanceTimeoutMs,
        System::nanoTime);
  }

  GroupCoordinator(
      MetadataService metadataService,
      long heartbeatIntervalMs,
      long sessionTimeoutMs,
      long rebalanceTimeoutMs,
      LongSupplier nanoClock) {
    this.metadataService = metadataService;
    this.heartbeatIntervalMs = heartbeatIntervalMs;
    this.sessionTimeoutMs = sessionTimeoutMs;
    this.rebalanceTimeoutMs = rebalanceTimeoutMs;
    this.nanoClock = nanoClock;
  }

  /** Starts the session reaper on its own virtual thread. */
  public synchronized void start() {
    if (running) {
      throw new IllegalStateException("Already started");
    }
    running = true;
    reaperThread = Thread.ofVirtual().name("group-coordinator-reaper").start(this::reaperLoop);
  }

  /**
   * Handles a member joining (or re-joining) {@code req.group()}, always triggering a rebalance.
   */
  public JoinGroupResp join(JoinGroupReq req) {
    GroupState state = groups.computeIfAbsent(req.group(), g -> new GroupState(g, req.topic()));
    synchronized (state) {
      String memberId = req.memberId().isBlank() ? UUID.randomUUID().toString() : req.memberId();
      long now = nanoClock.getAsLong();
      state.members.put(memberId, new GroupMember(memberId, List.of(), state.generation, now));
      triggerRebalance(state);
      log.info(
          "Group {} member {} joined; generation={} memberCount={}",
          req.group(),
          memberId,
          state.generation,
          state.members.size());
      return new JoinGroupResp(
          req.correlationId(), memberId, state.generation, heartbeatIntervalMs, sessionTimeoutMs);
    }
  }

  /**
   * Handles a liveness/ack heartbeat. Returns {@link GroupHeartbeatResp#UNKNOWN_MEMBER} for an
   * unrecognized group or member id (e.g. after a coordinator restart), {@link
   * GroupHeartbeatResp#REBALANCE_IN_PROGRESS} with the member's still-owned (to-be-revoked)
   * assignment while it hasn't yet acked the current generation, {@link
   * GroupHeartbeatResp#AWAITING_SYNC} once it has acked but other members haven't, and {@link
   * GroupHeartbeatResp#OK} once the barrier has cleared.
   */
  public GroupHeartbeatResp heartbeat(GroupHeartbeatReq req) {
    GroupState state = groups.get(req.group());
    if (state == null) {
      return new GroupHeartbeatResp(
          req.correlationId(), GroupHeartbeatResp.UNKNOWN_MEMBER, 0, List.of());
    }
    synchronized (state) {
      GroupMember member = state.members.get(req.memberId());
      if (member == null) {
        return new GroupHeartbeatResp(
            req.correlationId(), GroupHeartbeatResp.UNKNOWN_MEMBER, state.generation, List.of());
      }
      member.lastHeartbeatNanos = nanoClock.getAsLong();

      if (member.ackedGeneration < state.generation) {
        if (req.generation() == state.generation) {
          member.ackedGeneration = state.generation;
          member.assignment = state.pendingAssignments.getOrDefault(member.memberId, List.of());
          if (allAcked(state)) {
            state.phase = GroupState.Phase.STABLE;
            log.info("Group {} reached STABLE at generation {}", state.group, state.generation);
            return new GroupHeartbeatResp(
                req.correlationId(), GroupHeartbeatResp.OK, state.generation, member.assignment);
          }
          return new GroupHeartbeatResp(
              req.correlationId(),
              GroupHeartbeatResp.AWAITING_SYNC,
              state.generation,
              member.assignment);
        }
        return new GroupHeartbeatResp(
            req.correlationId(),
            GroupHeartbeatResp.REBALANCE_IN_PROGRESS,
            state.generation,
            member.assignment);
      }

      byte status =
          state.phase == GroupState.Phase.STABLE
              ? GroupHeartbeatResp.OK
              : GroupHeartbeatResp.AWAITING_SYNC;
      return new GroupHeartbeatResp(
          req.correlationId(), status, state.generation, member.assignment);
    }
  }

  /** Handles a voluntary departure, triggering a rebalance among the remaining members. */
  public LeaveGroupResp leave(LeaveGroupReq req) {
    GroupState state = groups.get(req.group());
    if (state == null) {
      return new LeaveGroupResp(req.correlationId(), false);
    }
    synchronized (state) {
      GroupMember removed = state.members.remove(req.memberId());
      if (removed == null) {
        return new LeaveGroupResp(req.correlationId(), false);
      }
      log.info(
          "Group {} member {} left; generation={}",
          req.group(),
          req.memberId(),
          state.generation + 1);
      triggerRebalance(state);
      return new LeaveGroupResp(req.correlationId(), true);
    }
  }

  /** Test-visible snapshot of a group's coordinator state, or {@code null} if unknown. */
  GroupState groupState(String group) {
    return groups.get(group);
  }

  private boolean allAcked(GroupState state) {
    for (GroupMember member : state.members.values()) {
      if (member.ackedGeneration != state.generation) {
        return false;
      }
    }
    return true;
  }

  /** Must be called with {@code state} held as the lock. Bumps the generation and reassigns. */
  private void triggerRebalance(GroupState state) {
    state.generation++;
    state.phase = GroupState.Phase.REBALANCING;
    state.rebalanceStartNanos = nanoClock.getAsLong();

    List<String> memberIds = new ArrayList<>(state.members.keySet());
    int partitionCount = metadataService.partitionCountFor(state.topic);
    Map<String, List<Integer>> pending = new java.util.HashMap<>();
    for (int i = 0; i < memberIds.size(); i++) {
      pending.put(memberIds.get(i), RangeAssignor.assign(i, memberIds.size(), partitionCount));
    }
    state.pendingAssignments = pending;
  }

  private void reaperLoop() {
    while (running) {
      try {
        Thread.sleep(REAPER_POLL_INTERVAL_MS);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        return;
      }
      if (!running) {
        return;
      }
      for (GroupState state : groups.values()) {
        reapOnce(state);
      }
    }
  }

  /**
   * Evicts members silent past {@code sessionTimeoutMs} and, if still stuck in {@code REBALANCING}
   * past {@code rebalanceTimeoutMs}, evicts every member that hasn't yet acked the current
   * generation — bounding how long a stuck member can block the barrier. Package-private so tests
   * can drive it directly against a fake clock without waiting on the reaper thread.
   */
  void reapOnce(GroupState state) {
    synchronized (state) {
      long now = nanoClock.getAsLong();
      boolean changed = false;
      List<String> sessionExpired = new ArrayList<>();
      for (GroupMember member : state.members.values()) {
        long elapsedMs = (now - member.lastHeartbeatNanos) / 1_000_000L;
        if (elapsedMs >= sessionTimeoutMs) {
          sessionExpired.add(member.memberId);
        }
      }
      for (String memberId : sessionExpired) {
        state.members.remove(memberId);
        changed = true;
        log.info("Group {} member {} evicted: session timeout", state.group, memberId);
      }

      if (state.phase == GroupState.Phase.REBALANCING) {
        long rebalanceElapsedMs = (now - state.rebalanceStartNanos) / 1_000_000L;
        if (rebalanceElapsedMs >= rebalanceTimeoutMs) {
          List<String> stuck = new ArrayList<>();
          for (GroupMember member : state.members.values()) {
            if (member.ackedGeneration < state.generation) {
              stuck.add(member.memberId);
            }
          }
          for (String memberId : stuck) {
            state.members.remove(memberId);
            changed = true;
            log.info(
                "Group {} member {} evicted: rebalance timeout at generation {}",
                state.group,
                memberId,
                state.generation);
          }
        }
      }

      if (changed) {
        triggerRebalance(state);
      }
    }
  }

  @Override
  public synchronized void close() {
    running = false;
    if (reaperThread != null) {
      reaperThread.interrupt();
      try {
        reaperThread.join(2000);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    }
  }
}
