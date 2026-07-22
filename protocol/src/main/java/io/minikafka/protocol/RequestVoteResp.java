package io.minikafka.protocol;

/** Broker → Broker: Raft RequestVote reply. */
public record RequestVoteResp(
    long correlationId, String topic, int partition, long term, boolean voteGranted)
    implements Message {

  public RequestVoteResp {
    if (topic == null) {
      throw new IllegalArgumentException("topic must not be null");
    }
  }

  @Override
  public MessageType type() {
    return MessageType.REQUEST_VOTE_RESP;
  }
}
