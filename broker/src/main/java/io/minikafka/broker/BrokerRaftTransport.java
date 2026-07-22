package io.minikafka.broker;

import io.minikafka.protocol.AppendEntriesReq;
import io.minikafka.protocol.AppendEntriesResp;
import io.minikafka.protocol.BrokerInfo;
import io.minikafka.protocol.ErrorResp;
import io.minikafka.protocol.Message;
import io.minikafka.protocol.RequestVoteReq;
import io.minikafka.protocol.RequestVoteResp;
import io.minikafka.raft.AppendEntriesRequest;
import io.minikafka.raft.AppendEntriesResponse;
import io.minikafka.raft.RaftTransport;
import io.minikafka.raft.RequestVoteRequest;
import io.minikafka.raft.RequestVoteResponse;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Bridges {@code raft.*Request}/{@code raft.*Response} to {@code protocol.*Req}/{@code
 * protocol.*Resp} and sends them over one {@link PeerConnection} per peer, scoped to a single
 * partition's Raft group (see class-level rationale in the Spec 07 plan: a shared per-peer socket
 * would serialize every partition's replication pipeline through {@link PeerConnection}'s
 * single-in-flight lock).
 */
final class BrokerRaftTransport implements RaftTransport, AutoCloseable {

  private final TopicPartition tp;
  private final Map<Integer, PeerConnection> connections;
  private final ExecutorService vthreads;
  private final AtomicLong correlationIds = new AtomicLong();

  BrokerRaftTransport(
      TopicPartition tp,
      Map<Integer, BrokerInfo> peers,
      long readTimeoutMs,
      long reconnectBackoffMs,
      ExecutorService vthreads) {
    this.tp = tp;
    this.vthreads = vthreads;
    Map<Integer, PeerConnection> conns = new ConcurrentHashMap<>();
    peers.forEach(
        (id, info) -> conns.put(id, new PeerConnection(info, readTimeoutMs, reconnectBackoffMs)));
    this.connections = conns;
  }

  @Override
  public CompletableFuture<AppendEntriesResponse> appendEntries(
      int targetBrokerId, AppendEntriesRequest request) {
    return CompletableFuture.supplyAsync(
        () -> {
          PeerConnection conn = connectionFor(targetBrokerId);
          List<AppendEntriesReq.Entry> entries =
              request.entries().stream()
                  .map(e -> new AppendEntriesReq.Entry(e.term(), e.index(), e.command()))
                  .toList();
          AppendEntriesReq req =
              new AppendEntriesReq(
                  correlationIds.incrementAndGet(),
                  tp.topic(),
                  tp.partition(),
                  request.term(),
                  request.leaderId(),
                  request.prevLogIndex(),
                  request.prevLogTerm(),
                  entries,
                  request.leaderCommit());
          Message response = sendOrThrow(conn, req);
          if (response instanceof ErrorResp err) {
            throw new RaftRpcException(
                "AppendEntries to " + targetBrokerId + " rejected: " + err.message());
          }
          AppendEntriesResp resp = (AppendEntriesResp) response;
          return new AppendEntriesResponse(
              resp.term(),
              resp.success(),
              resp.conflictIndex(),
              resp.conflictTerm(),
              resp.followerLastIndex());
        },
        vthreads);
  }

  @Override
  public CompletableFuture<RequestVoteResponse> requestVote(
      int targetBrokerId, RequestVoteRequest request) {
    return CompletableFuture.supplyAsync(
        () -> {
          PeerConnection conn = connectionFor(targetBrokerId);
          RequestVoteReq req =
              new RequestVoteReq(
                  correlationIds.incrementAndGet(),
                  tp.topic(),
                  tp.partition(),
                  request.term(),
                  request.candidateId(),
                  request.lastLogIndex(),
                  request.lastLogTerm());
          Message response = sendOrThrow(conn, req);
          if (response instanceof ErrorResp err) {
            throw new RaftRpcException(
                "RequestVote to " + targetBrokerId + " rejected: " + err.message());
          }
          RequestVoteResp resp = (RequestVoteResp) response;
          return new RequestVoteResponse(resp.term(), resp.voteGranted());
        },
        vthreads);
  }

  private Message sendOrThrow(PeerConnection conn, Message request) {
    try {
      return conn.send(request);
    } catch (IOException e) {
      throw new RaftRpcException("Raft RPC to peer failed", e);
    }
  }

  private PeerConnection connectionFor(int brokerId) {
    PeerConnection conn = connections.get(brokerId);
    if (conn == null) {
      throw new RaftRpcException("No peer connection configured for broker " + brokerId);
    }
    return conn;
  }

  @Override
  public void close() {
    connections.values().forEach(PeerConnection::close);
  }

  /**
   * Wraps a network-level failure so {@link CompletableFuture#completeExceptionally} carries it —
   * never fabricated as a {@code success=false} response, which would let a leader misread an
   * unreachable follower as a log mismatch and back {@code nextIndex} down wrongly.
   */
  static final class RaftRpcException extends RuntimeException {
    RaftRpcException(String message) {
      super(message);
    }

    RaftRpcException(String message, Throwable cause) {
      super(message, cause);
    }
  }
}
