package io.minikafka.client;

import io.minikafka.protocol.ErrorResp;
import io.minikafka.protocol.GroupHeartbeatReq;
import io.minikafka.protocol.GroupHeartbeatResp;
import io.minikafka.protocol.JoinGroupReq;
import io.minikafka.protocol.JoinGroupResp;
import io.minikafka.protocol.LeaveGroupReq;
import io.minikafka.protocol.Message;
import io.minikafka.protocol.PollResp;
import io.minikafka.protocol.ProtocolException;
import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * A dynamically-rebalanced consumer group member: joins {@code group} on the broker's {@code
 * GroupCoordinator}, owns one {@link ConsumerClient} per assigned partition, and transparently
 * hands partitions over on membership change.
 *
 * <p>The wire protocol is strictly request/response, so there is no server-push rebalance
 * notification — it is piggybacked on the heartbeat response instead, and this class drives that
 * heartbeat from {@link #poll()} rather than a background thread. {@code client/src/main} has zero
 * background threads today, and {@link BrokerConnection} is not thread-safe while {@link
 * ClusterClient} hands the same pooled instance to concurrent callers, so a heartbeat thread
 * sharing a connection with the polling thread would corrupt correlation matching. The tradeoff: an
 * application that stops calling {@link #poll()} stops heartbeating and is evicted for session
 * timeout — precisely Kafka's {@code max.poll.interval.ms} semantic.
 */
public final class GroupConsumer implements AutoCloseable {

  private final ClusterClient clusterClient;
  private final String topic;
  private final String group;
  private final int maxRetries;
  private final long retryBackoffMs;
  private final CoordinatorCall coordinatorCall;
  private final Map<Integer, ConsumerClient> consumers = new LinkedHashMap<>();

  private String memberId;
  private int generation;
  private long heartbeatIntervalMs;
  private long sessionTimeoutMs;
  private List<Integer> assignment = List.of();
  private long lastHeartbeatNanos;
  private int pollIndex;

  /** A group consumer over {@code cluster} with default retry tunables. */
  public GroupConsumer(ClusterClient cluster, String topic, String group) {
    this(
        cluster,
        topic,
        group,
        ProducerClient.DEFAULT_MAX_RETRIES,
        ProducerClient.DEFAULT_RETRY_BACKOFF_MS);
  }

  public GroupConsumer(
      ClusterClient cluster, String topic, String group, int maxRetries, long retryBackoffMs) {
    this.clusterClient = cluster;
    this.topic = topic;
    this.group = group;
    this.maxRetries = maxRetries;
    this.retryBackoffMs = retryBackoffMs;
    this.coordinatorCall = new CoordinatorCall(cluster, maxRetries, retryBackoffMs);
  }

  /**
   * Heartbeats (joining first if necessary) and, if due, drains one poll's worth of records from
   * the next owned partition in round-robin order. Returns an empty list if this member currently
   * owns no partitions (e.g. mid-rebalance).
   */
  public List<PollResp.Record> poll() throws IOException {
    if (memberId == null) {
      join();
      syncHeartbeat();
    } else if (System.nanoTime() - lastHeartbeatNanos >= heartbeatIntervalMs * 1_000_000L) {
      syncHeartbeat();
    }
    if (assignment.isEmpty()) {
      return List.of();
    }
    int partition = assignment.get(pollIndex % assignment.size());
    pollIndex++;
    return consumers.get(partition).poll();
  }

  /** Durably commits the current offset of every currently-owned partition. */
  public void commitOffsets() throws IOException {
    for (ConsumerClient consumer : consumers.values()) {
      consumer.commitOffset();
    }
  }

  /** This member's currently-owned partitions. */
  public List<Integer> assignment() {
    return assignment;
  }

  /** This member's current group generation. */
  public int generation() {
    return generation;
  }

  /** This member's coordinator-assigned id, or {@code null} before the first {@link #poll()}. */
  public String memberId() {
    return memberId;
  }

  /** Commits outstanding offsets, then leaves the group. */
  @Override
  public void close() throws IOException {
    if (memberId == null) {
      return;
    }
    commitOffsets();
    coordinatorCall.send(
        conn -> conn.request(new LeaveGroupReq(conn.nextCorrelationId(), group, memberId)));
  }

  private void join() throws IOException {
    Message response =
        coordinatorCall.send(
            conn -> conn.request(new JoinGroupReq(conn.nextCorrelationId(), group, "", topic)));
    JoinGroupResp resp = expect(response, JoinGroupResp.class, "Join group failed");
    memberId = resp.memberId();
    generation = resp.generation();
    heartbeatIntervalMs = resp.heartbeatIntervalMs();
    sessionTimeoutMs = resp.sessionTimeoutMs();
  }

  /**
   * Drives the heartbeat/rebalance protocol until this member is {@code OK} on the current
   * generation: on {@code REBALANCE_IN_PROGRESS} it pauses (drops its {@link ConsumerClient}s after
   * committing them), acks with the new generation, and waits out {@code AWAITING_SYNC} until every
   * member has acked or {@code sessionTimeoutMs} elapses.
   */
  private void syncHeartbeat() throws IOException {
    long deadlineNanos = System.nanoTime() + sessionTimeoutMs * 1_000_000L;
    while (true) {
      GroupHeartbeatResp resp = sendHeartbeat();
      switch (resp.status()) {
        case GroupHeartbeatResp.OK -> {
          generation = resp.generation();
          adoptAssignment(resp.assignment());
          lastHeartbeatNanos = System.nanoTime();
          return;
        }
        case GroupHeartbeatResp.REBALANCE_IN_PROGRESS -> {
          for (int partition : resp.assignment()) {
            ConsumerClient consumer = consumers.get(partition);
            if (consumer != null) {
              consumer.commitOffset();
            }
          }
          consumers.clear();
          assignment = List.of();
          generation = resp.generation();
        }
        case GroupHeartbeatResp.AWAITING_SYNC -> {
          if (System.nanoTime() > deadlineNanos) {
            throw new ProtocolException("Timed out awaiting rebalance barrier for group " + group);
          }
          sleep();
        }
        case GroupHeartbeatResp.UNKNOWN_MEMBER -> join();
        default -> throw new ProtocolException("Unknown heartbeat status: " + resp.status());
      }
    }
  }

  private GroupHeartbeatResp sendHeartbeat() throws IOException {
    int gen = generation;
    Message response =
        coordinatorCall.send(
            conn ->
                conn.request(
                    new GroupHeartbeatReq(conn.nextCorrelationId(), group, memberId, gen)));
    return expect(response, GroupHeartbeatResp.class, "Group heartbeat failed");
  }

  private void adoptAssignment(List<Integer> newAssignment) {
    assignment = newAssignment;
    pollIndex = 0;
    for (int partition : newAssignment) {
      consumers.computeIfAbsent(partition, this::openConsumer);
    }
  }

  private ConsumerClient openConsumer(int partition) {
    try {
      return new ConsumerClient(clusterClient, topic, partition, group, maxRetries, retryBackoffMs);
    } catch (IOException e) {
      throw new java.io.UncheckedIOException(
          "Failed to open consumer for " + topic + "-" + partition + " in group " + group, e);
    }
  }

  private void sleep() throws IOException {
    try {
      Thread.sleep(heartbeatIntervalMs);
    } catch (InterruptedException ie) {
      Thread.currentThread().interrupt();
      throw new java.io.EOFException("Interrupted while awaiting rebalance barrier");
    }
  }

  private static <T extends Message> T expect(Message response, Class<T> type, String context)
      throws IOException {
    if (type.isInstance(response)) {
      return type.cast(response);
    }
    if (response instanceof ErrorResp err) {
      throw new ProtocolException(context + ": " + err.message());
    }
    throw new ProtocolException(context + ": unexpected response type " + response.type());
  }
}
