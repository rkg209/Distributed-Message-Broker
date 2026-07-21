package io.minikafka.broker;

import io.minikafka.log.AppendResult;
import io.minikafka.log.LogRecord;
import io.minikafka.log.OffsetOutOfRangeException;
import io.minikafka.protocol.CommitOffsetReq;
import io.minikafka.protocol.CommitOffsetResp;
import io.minikafka.protocol.ErrorResp;
import io.minikafka.protocol.FetchOffsetReq;
import io.minikafka.protocol.FetchOffsetResp;
import io.minikafka.protocol.HeartbeatReq;
import io.minikafka.protocol.HeartbeatResp;
import io.minikafka.protocol.Message;
import io.minikafka.protocol.MetadataReq;
import io.minikafka.protocol.MetadataResp;
import io.minikafka.protocol.PollReq;
import io.minikafka.protocol.PollResp;
import io.minikafka.protocol.PublishReq;
import io.minikafka.protocol.PublishResp;
import java.util.List;

/**
 * Real request handler backing the Spec 02–04 publish/poll/metadata/consumer-group slice. Replaces
 * {@link StubRequestHandler}.
 */
public final class BrokerRequestHandler implements RequestHandler {

  private final PartitionManager partitionManager;
  private final MetadataService metadataService;
  private final ConsumerGroupManager consumerGroupManager;
  private final int maxPollBytes;

  public BrokerRequestHandler(
      MetadataService metadataService,
      PartitionManager partitionManager,
      ConsumerGroupManager consumerGroupManager,
      int maxPollBytes) {
    this.metadataService = metadataService;
    this.partitionManager = partitionManager;
    this.consumerGroupManager = consumerGroupManager;
    this.maxPollBytes = maxPollBytes;
  }

  @Override
  public Message handle(Message request) {
    return switch (request) {
      case PublishReq req -> handlePublish(req);
      case PollReq req -> handlePoll(req);
      case MetadataReq req ->
          new MetadataResp(
              req.correlationId(),
              metadataService.clusterBrokers(),
              metadataService.describeTopics());
      case CommitOffsetReq req -> handleCommitOffset(req);
      case FetchOffsetReq req -> handleFetchOffset(req);
      case HeartbeatReq req -> new HeartbeatResp(req.correlationId(), req.term());
      default ->
          new ErrorResp(
              request.correlationId(),
              ErrorResp.CODE_UNSUPPORTED,
              "Request type not supported: " + request.type());
    };
  }

  private Message handlePublish(PublishReq req) {
    AppendResult result;
    try {
      result = partitionManager.publish(req.topic(), req.partition(), req.key(), req.payload());
    } catch (UnknownPartitionException e) {
      return new ErrorResp(req.correlationId(), ErrorResp.CODE_UNKNOWN_PARTITION, e.getMessage());
    }
    return new PublishResp(req.correlationId(), result.offset());
  }

  private Message handlePoll(PollReq req) {
    List<LogRecord> records;
    try {
      records = partitionManager.poll(req.topic(), req.partition(), req.offset(), maxPollBytes);
    } catch (UnknownPartitionException e) {
      return new ErrorResp(req.correlationId(), ErrorResp.CODE_UNKNOWN_PARTITION, e.getMessage());
    } catch (OffsetOutOfRangeException e) {
      return new ErrorResp(req.correlationId(), ErrorResp.CODE_OFFSET_OUT_OF_RANGE, e.getMessage());
    }
    List<PollResp.Record> batch =
        records.stream().map(r -> new PollResp.Record(r.offset(), r.value())).toList();
    return new PollResp(req.correlationId(), batch);
  }

  private Message handleCommitOffset(CommitOffsetReq req) {
    consumerGroupManager.commit(req.group(), req.topic(), req.partition(), req.offset());
    return new CommitOffsetResp(req.correlationId(), true);
  }

  private Message handleFetchOffset(FetchOffsetReq req) {
    long offset = consumerGroupManager.fetch(req.group(), req.topic(), req.partition());
    return new FetchOffsetResp(req.correlationId(), offset);
  }
}
