package io.minikafka.protocol;

/**
 * Broker → Broker: Raft RequestVote RPC, routed to a specific partition's Raft group by {@code
 * (topic, partition)}.
 */
public record RequestVoteReq(
    long correlationId,
    String topic,
    int partition,
    long term,
    int candidateId,
    long lastLogIndex,
    long lastLogTerm)
    implements Message {

  public RequestVoteReq {
    if (topic == null) {
      throw new IllegalArgumentException("topic must not be null");
    }
  }

  @Override
  public MessageType type() {
    return MessageType.REQUEST_VOTE_REQ;
  }
}
