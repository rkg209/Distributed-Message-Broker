package io.minikafka.protocol;

/** Broker → Broker: Raft RequestVote RPC. */
public record RequestVoteReq(long correlationId, long term, int candidateId) implements Message {

  @Override
  public MessageType type() {
    return MessageType.REQUEST_VOTE_REQ;
  }
}
