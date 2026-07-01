package io.minikafka.protocol;

/** Broker → Broker: Raft RequestVote reply. */
public record RequestVoteResp(long correlationId, long term, boolean voteGranted)
    implements Message {

  @Override
  public MessageType type() {
    return MessageType.REQUEST_VOTE_RESP;
  }
}
