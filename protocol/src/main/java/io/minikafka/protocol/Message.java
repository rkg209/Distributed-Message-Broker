package io.minikafka.protocol;

/**
 * A typed, decoded protocol message. Each permitted record maps one-to-one to a {@link
 * MessageType}.
 *
 * <p>For Spec 01 the field sets are intentionally minimal (a correlation id plus a few
 * representative fields); later specs enrich the request/response bodies additively without
 * changing the framing. Every message carries a {@code correlationId} so a client can match a
 * response to the request that produced it.
 */
public sealed interface Message
    permits PublishReq,
        PublishResp,
        PollReq,
        PollResp,
        CommitOffsetReq,
        CommitOffsetResp,
        MetadataReq,
        MetadataResp,
        AppendEntriesReq,
        AppendEntriesResp,
        RequestVoteReq,
        RequestVoteResp,
        HeartbeatReq,
        HeartbeatResp,
        ErrorResp {

  /** The wire type of this message. */
  MessageType type();

  /** Client-assigned id echoed back on the matching response. */
  long correlationId();
}
