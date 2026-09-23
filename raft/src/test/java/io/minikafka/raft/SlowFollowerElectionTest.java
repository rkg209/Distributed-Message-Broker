package io.minikafka.raft;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * A follower whose durable append outlasts the election timeout must not start an election the
 * moment it finishes handling a valid AppendEntries — it has just heard from the leader. Before the
 * fix, the election timer thread observed the (pre-disk-work) deadline expire, blocked on the node
 * lock behind the slow handler, and then started an election unconditionally once the handler
 * released it: spurious leader churn whenever fsync is slower than the election timeout.
 */
class SlowFollowerElectionTest {

  private static final long MIN_ELECTION_MS = 150;
  private static final long MAX_ELECTION_MS = 300;
  private static final long SLOW_APPEND_MS = 2 * MAX_ELECTION_MS;

  @Test
  void slowAppendFromLeaderDoesNotTriggerElection(@TempDir Path dir) throws Exception {
    RaftConfig config =
        new RaftConfig(MIN_ELECTION_MS, MAX_ELECTION_MS, 50, 100, 200, dir, "slow-follower");
    AtomicInteger voteRequests = new AtomicInteger();
    RaftTransport transport =
        new RaftTransport() {
          @Override
          public CompletableFuture<AppendEntriesResponse> appendEntries(
              int targetBrokerId, AppendEntriesRequest request) {
            return new CompletableFuture<>();
          }

          @Override
          public CompletableFuture<RequestVoteResponse> requestVote(
              int targetBrokerId, RequestVoteRequest request) {
            voteRequests.incrementAndGet();
            return new CompletableFuture<>();
          }
        };
    RaftNode follower =
        new RaftNode(
            1,
            List.of(2, 3),
            config,
            new SlowAppendLogStore(SLOW_APPEND_MS),
            PersistentState.load(dir),
            transport,
            new CountingStateMachine(),
            System::nanoTime);
    follower.start();
    try {
      AppendEntriesResponse resp =
          follower.handleAppendEntries(
              new AppendEntriesRequest(
                  1, 2, 0, 0, List.of(new RaftEntry(1, 1, "x".getBytes())), 0));
      assertEquals(true, resp.success());

      // Well inside the minimum election timeout measured from the end of the handler: a correct
      // follower is still a follower of term 1; the buggy one has already become a candidate.
      Thread.sleep(MIN_ELECTION_MS / 3);
      assertEquals(1, follower.currentTerm());
      assertEquals(RaftRole.FOLLOWER, follower.role());
      assertEquals(0, voteRequests.get());
    } finally {
      follower.close();
    }
  }

  /** In-memory log whose {@code append} blocks like an fsync on a slow disk. */
  private static final class SlowAppendLogStore implements RaftLogStore {
    private final InMemoryRaftLogStore delegate = new InMemoryRaftLogStore();
    private final long delayMs;

    SlowAppendLogStore(long delayMs) {
      this.delayMs = delayMs;
    }

    @Override
    public void append(RaftEntry entry) {
      try {
        Thread.sleep(delayMs);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new IllegalStateException(e);
      }
      delegate.append(entry);
    }

    @Override
    public RaftEntry get(long index) {
      return delegate.get(index);
    }

    @Override
    public List<RaftEntry> getFrom(long index, int maxEntries) {
      return delegate.getFrom(index, maxEntries);
    }

    @Override
    public long lastIndex() {
      return delegate.lastIndex();
    }

    @Override
    public long lastTerm() {
      return delegate.lastTerm();
    }

    @Override
    public long firstIndex() {
      return delegate.firstIndex();
    }

    @Override
    public void truncateFrom(long fromIndex) {
      delegate.truncateFrom(fromIndex);
    }

    @Override
    public void close() {
      delegate.close();
    }
  }
}
