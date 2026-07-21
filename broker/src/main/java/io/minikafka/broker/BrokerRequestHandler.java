package io.minikafka.broker;

import io.minikafka.log.AppendResult;
import io.minikafka.log.LogRecord;
import io.minikafka.log.OffsetOutOfRangeException;
import io.minikafka.protocol.BrokerInfo;
import io.minikafka.protocol.ErrorResp;
import io.minikafka.protocol.Message;
import io.minikafka.protocol.MetadataReq;
import io.minikafka.protocol.MetadataResp;
import io.minikafka.protocol.PollReq;
import io.minikafka.protocol.PollResp;
import io.minikafka.protocol.PublishReq;
import io.minikafka.protocol.PublishResp;
import java.util.List;

/**
 * Real request handler backing the Spec 02 publish/poll/metadata slice. Replaces {@link
 * StubRequestHandler}.
 */
public final class BrokerRequestHandler implements RequestHandler {

  private final BrokerInfo self;
  private final PartitionManager partitionManager;
  private final int maxPollBytes;

  public BrokerRequestHandler(
      BrokerInfo self, PartitionManager partitionManager, int maxPollBytes) {
    this.self = self;
    this.partitionManager = partitionManager;
    this.maxPollBytes = maxPollBytes;
  }

  @Override
  public Message handle(Message request) {
    return switch (request) {
      case PublishReq req -> handlePublish(req);
      case PollReq req -> handlePoll(req);
      case MetadataReq req -> new MetadataResp(req.correlationId(), List.of(self));
      default ->
          new ErrorResp(
              request.correlationId(),
              ErrorResp.CODE_UNSUPPORTED,
              "Request type not supported: " + request.type());
    };
  }

  private Message handlePublish(PublishReq req) {
    AppendResult result = partitionManager.publish(req.topic(), req.partition(), req.payload());
    return new PublishResp(req.correlationId(), result.offset());
  }

  private Message handlePoll(PollReq req) {
    List<LogRecord> records;
    try {
      records = partitionManager.poll(req.topic(), req.partition(), req.offset(), maxPollBytes);
    } catch (OffsetOutOfRangeException e) {
      return new ErrorResp(req.correlationId(), ErrorResp.CODE_OFFSET_OUT_OF_RANGE, e.getMessage());
    }
    List<PollResp.Record> batch =
        records.stream().map(r -> new PollResp.Record(r.offset(), r.value())).toList();
    return new PollResp(req.correlationId(), batch);
  }
}
