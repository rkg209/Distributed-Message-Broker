package io.minikafka.raft;

import java.util.Collection;
import java.util.List;

/**
 * Computes the new {@code commitIndex} for a leader, per Raft §5.4.2: the largest {@code N >
 * commitIndex} such that a majority of {@code matchIndex} values (including the leader's own last
 * log index) are {@code >= N}, AND {@code log[N].term == currentTerm}. That second clause is what
 * prevents the Figure-8 scenario — a leader must never commit an entry replicated from a previous
 * term purely by majority match; it must wait until an entry from its own term also commits, which
 * transitively commits every earlier entry.
 */
final class CommitAdvancer {

  private CommitAdvancer() {}

  static long advance(
      long currentTerm,
      Collection<Long> matchIndexes,
      RaftLogStore logStore,
      long currentCommitIndex) {
    List<Long> sorted = matchIndexes.stream().sorted().toList();
    int majoritySize = sorted.size() / 2 + 1;
    long best = currentCommitIndex;
    for (long candidate = logStore.lastIndex(); candidate > currentCommitIndex; candidate--) {
      long finalCandidate = candidate;
      long countAtLeast = sorted.stream().filter(m -> m >= finalCandidate).count();
      if (countAtLeast < majoritySize) {
        continue;
      }
      RaftEntry entry = logStore.get(candidate);
      if (entry != null && entry.term() == currentTerm) {
        best = candidate;
        break;
      }
    }
    return best;
  }
}
