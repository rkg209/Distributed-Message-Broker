package io.minikafka.broker;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.minikafka.protocol.BrokerInfo;
import io.minikafka.protocol.GroupHeartbeatReq;
import io.minikafka.protocol.GroupHeartbeatResp;
import io.minikafka.protocol.JoinGroupReq;
import io.minikafka.protocol.JoinGroupResp;
import io.minikafka.protocol.LeaveGroupReq;
import io.minikafka.protocol.LeaveGroupResp;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;

/** Unit tests for {@link GroupCoordinator} driven entirely by a fake nanosecond clock. */
class GroupCoordinatorTest {

  private static final String GROUP = "g1";
  private static final String TOPIC = "orders";
  private static final long HEARTBEAT_INTERVAL_MS = 1_000;
  private static final long SESSION_TIMEOUT_MS = 10_000;
  private static final long REBALANCE_TIMEOUT_MS = 5_000;

  private final AtomicLong clockNanos = new AtomicLong(0);
  private final MetadataService metadataService =
      new MetadataService(new BrokerInfo(1, "localhost", 0), TopicConfig.parse(TOPIC + ":4", 1));
  private final GroupCoordinator coordinator =
      new GroupCoordinator(
          metadataService,
          HEARTBEAT_INTERVAL_MS,
          SESSION_TIMEOUT_MS,
          REBALANCE_TIMEOUT_MS,
          clockNanos::get);

  private void advance(long ms) {
    clockNanos.addAndGet(ms * 1_000_000L);
  }

  private JoinGroupResp join(String memberId) {
    return coordinator.join(new JoinGroupReq(0L, GROUP, memberId, TOPIC));
  }

  private GroupHeartbeatResp heartbeat(String memberId, int generation) {
    return coordinator.heartbeat(new GroupHeartbeatReq(0L, GROUP, memberId, generation));
  }

  @Test
  void joinBumpsGenerationAndEntersRebalancing() {
    JoinGroupResp resp = join("");
    assertEquals(1, resp.generation());
    assertEquals(GroupState.Phase.REBALANCING, coordinator.groupState(GROUP).phase);
  }

  @Test
  void generationIsMonotonicAcrossJoins() {
    JoinGroupResp a = join("A");
    heartbeat("A", a.generation());
    JoinGroupResp b = join("B");
    assertTrue(b.generation() > a.generation());
  }

  @Test
  void barrierHoldsAtAwaitingSyncUntilLastAck() {
    JoinGroupResp a = join("A");
    heartbeat("A", a.generation()); // A acks, alone -> immediately STABLE
    JoinGroupResp b = join("B"); // bumps generation again, A must re-ack
    int gen = b.generation();

    GroupHeartbeatResp bResp = heartbeat("B", gen);
    assertEquals(GroupHeartbeatResp.AWAITING_SYNC, bResp.status());

    GroupHeartbeatResp aResp = heartbeat("A", gen);
    assertEquals(GroupHeartbeatResp.OK, aResp.status());

    // B's next heartbeat now also observes OK, since the barrier cleared.
    GroupHeartbeatResp bResp2 = heartbeat("B", gen);
    assertEquals(GroupHeartbeatResp.OK, bResp2.status());
  }

  @Test
  void staleGenerationHeartbeatReturnsRebalanceInProgressWithOldAssignment() {
    JoinGroupResp a = join("A");
    heartbeat("A", a.generation());
    join("B");

    GroupHeartbeatResp resp = heartbeat("A", a.generation()); // stale: A still on old generation
    assertEquals(GroupHeartbeatResp.REBALANCE_IN_PROGRESS, resp.status());
  }

  @Test
  void unknownMemberIdReturnsUnknownMember() {
    join("A");
    GroupHeartbeatResp resp = heartbeat("ghost", 1);
    assertEquals(GroupHeartbeatResp.UNKNOWN_MEMBER, resp.status());
  }

  @Test
  void unknownGroupReturnsUnknownMember() {
    GroupHeartbeatResp resp =
        coordinator.heartbeat(new GroupHeartbeatReq(0L, "no-such-group", "x", 1));
    assertEquals(GroupHeartbeatResp.UNKNOWN_MEMBER, resp.status());
  }

  @Test
  void leaveTriggersRebalanceAmongRemainingMembers() {
    JoinGroupResp a = join("A");
    heartbeat("A", a.generation());
    JoinGroupResp b = join("B");
    heartbeat("B", b.generation());
    heartbeat("A", b.generation());

    LeaveGroupResp leftResp = coordinator.leave(new LeaveGroupReq(0L, GROUP, "B"));
    assertTrue(leftResp.ok());
    assertEquals(GroupState.Phase.REBALANCING, coordinator.groupState(GROUP).phase);

    GroupHeartbeatResp aResp = heartbeat("A", coordinator.groupState(GROUP).generation);
    assertEquals(GroupHeartbeatResp.OK, aResp.status());
    assertEquals(allFourPartitions(), aResp.assignment());
  }

  @Test
  void leaveOfUnknownMemberReturnsNotOk() {
    join("A");
    LeaveGroupResp resp = coordinator.leave(new LeaveGroupReq(0L, GROUP, "ghost"));
    assertFalse(resp.ok());
  }

  @Test
  void sessionTimeoutEvictsSilentMember() {
    JoinGroupResp a = join("A");
    heartbeat("A", a.generation());
    join("B"); // A must re-ack, but never does

    advance(SESSION_TIMEOUT_MS + 1);
    coordinator.reapOnce(coordinator.groupState(GROUP));

    assertFalse(coordinator.groupState(GROUP).members.containsKey("A"));
  }

  @Test
  void rebalanceTimeoutEvictsNonAckingMember() {
    JoinGroupResp a = join("A");
    heartbeat("A", a.generation());
    JoinGroupResp b = join("B");
    heartbeat("B", b.generation()); // B acks promptly; A never does

    advance(REBALANCE_TIMEOUT_MS + 1);
    coordinator.reapOnce(coordinator.groupState(GROUP));

    assertFalse(coordinator.groupState(GROUP).members.containsKey("A"));
    assertTrue(coordinator.groupState(GROUP).members.containsKey("B"));
  }

  private static List<Integer> allFourPartitions() {
    return List.of(0, 1, 2, 3);
  }
}
