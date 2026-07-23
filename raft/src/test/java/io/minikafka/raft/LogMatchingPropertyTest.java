package io.minikafka.raft;

import static org.junit.jupiter.api.Assertions.fail;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Safety property 2 (Log Matching, Raft §5.3): whenever two logs contain an entry with the same
 * (term, index), the logs are identical in every entry preceding it. Drives a randomized sequence
 * of leader-term changes, appends, and (partial) replication directly through {@link
 * RaftNode#handleAppendEntries} against two independent follower log stores, then checks the
 * property holds.
 */
class LogMatchingPropertyTest {

  private static final RaftTransport UNUSED_TRANSPORT =
      new RaftTransport() {
        @Override
        public CompletableFuture<AppendEntriesResponse> appendEntries(
            int targetBrokerId, AppendEntriesRequest request) {
          throw new UnsupportedOperationException();
        }

        @Override
        public CompletableFuture<RequestVoteResponse> requestVote(
            int targetBrokerId, RequestVoteRequest request) {
          throw new UnsupportedOperationException();
        }
      };

  @Test
  void logMatchingPropertyHoldsUnderRandomizedReplication(@TempDir Path dir) {
    InMemoryRaftLogStore logA = new InMemoryRaftLogStore();
    InMemoryRaftLogStore logB = new InMemoryRaftLogStore();
    RaftNode nodeA = bareFollower(1, logA, dir.resolve("a"));
    RaftNode nodeB = bareFollower(2, logB, dir.resolve("b"));

    List<RaftEntry> leaderLog = new ArrayList<>();
    Map<RaftNode, Long> nextIndex = new HashMap<>();
    nextIndex.put(nodeA, 1L);
    nextIndex.put(nodeB, 1L);
    long leaderTerm = 1;
    Random random = new Random(123456789L);

    for (int iter = 0; iter < 300; iter++) {
      int action = random.nextInt(3);
      if (action != 1 || leaderLog.isEmpty()) {
        leaderLog.add(new RaftEntry(leaderTerm, leaderLog.size() + 1L, ("v" + iter).getBytes()));
      } else {
        int keep = random.nextInt(leaderLog.size() + 1);
        leaderLog.subList(keep, leaderLog.size()).clear();
        leaderTerm++;
        leaderLog.add(
            new RaftEntry(leaderTerm, leaderLog.size() + 1L, ("t" + leaderTerm).getBytes()));
        // A newly elected leader always starts followers optimistically at its own lastIndex+1
        // (see becomeLeader) — it has no knowledge of follower state until the first reply.
        for (RaftNode follower : nextIndex.keySet()) {
          nextIndex.put(follower, leaderLog.size() + 1L);
        }
      }
      for (RaftNode follower : List.of(nodeA, nodeB)) {
        if (random.nextBoolean()) {
          replicateOnce(follower, leaderLog, nextIndex, leaderTerm);
        }
      }
    }
    for (int i = 0; i < 50; i++) {
      for (RaftNode follower : List.of(nodeA, nodeB)) {
        replicateOnce(follower, leaderLog, nextIndex, leaderTerm);
      }
    }

    assertLogMatching(logA, logB);
  }

  private static RaftNode bareFollower(int id, RaftLogStore logStore, Path stateDir) {
    RaftConfig config = new RaftConfig(100, 200, 20, 100, 150, stateDir, "group-" + id);
    PersistentState persistentState = PersistentState.load(stateDir);
    return new RaftNode(
        id,
        List.of(),
        config,
        logStore,
        persistentState,
        UNUSED_TRANSPORT,
        (i, c) -> ApplyResult.ok(c),
        System::nanoTime);
  }

  private static void replicateOnce(
      RaftNode follower,
      List<RaftEntry> leaderLog,
      Map<RaftNode, Long> nextIndex,
      long leaderTerm) {
    long ni = nextIndex.get(follower);
    long prevIndex = ni - 1;
    long prevTerm = prevIndex <= 0 ? 0 : leaderLog.get((int) prevIndex - 1).term();
    List<RaftEntry> batch = new ArrayList<>();
    for (long i = ni; i <= leaderLog.size() && batch.size() < 10; i++) {
      batch.add(leaderLog.get((int) i - 1));
    }
    AppendEntriesRequest req =
        new AppendEntriesRequest(leaderTerm, 999, prevIndex, prevTerm, batch, 0);
    AppendEntriesResponse resp = follower.handleAppendEntries(req);
    if (resp.success()) {
      nextIndex.put(follower, resp.followerLastIndex() + 1);
    } else {
      nextIndex.put(follower, Math.max(1, ni - 1));
    }
  }

  private static void assertLogMatching(RaftLogStore logA, RaftLogStore logB) {
    long minLast = Math.min(logA.lastIndex(), logB.lastIndex());
    for (long i = 1; i <= minLast; i++) {
      RaftEntry a = logA.get(i);
      RaftEntry b = logB.get(i);
      if (a == null || b == null || a.term() != b.term()) {
        continue;
      }
      for (long j = 1; j < i; j++) {
        RaftEntry earlierA = logA.get(j);
        RaftEntry earlierB = logB.get(j);
        if (!earlierA.equals(earlierB)) {
          fail(
              "logs agree at index "
                  + i
                  + " (term "
                  + a.term()
                  + ") but diverge at earlier index "
                  + j
                  + ": "
                  + earlierA
                  + " vs "
                  + earlierB);
        }
      }
    }
  }
}
