package io.minikafka.broker;

import io.minikafka.log.LogConfig;
import io.minikafka.protocol.BrokerInfo;
import io.minikafka.protocol.ProtocolConfig;
import io.minikafka.raft.RaftConfig;
import java.nio.file.Path;

/**
 * Builds a single-broker {@link BrokerConfig} for tests that predate Spec 07 and don't care about
 * replication or Raft tuning — just a real {@link PartitionManager} with a degenerate, self-only
 * Raft group per partition. Uses compressed election/heartbeat timings so single-node leader
 * election doesn't slow every test down.
 */
final class TestBrokerConfig {

  private TestBrokerConfig() {}

  static BrokerConfig singleBroker(Path logDir, Path offsetDir, TopicConfig topicConfig) {
    BrokerInfo self = new BrokerInfo(1, "localhost", 0);
    return new BrokerConfig(
        self.brokerId(),
        self.host(),
        self.port(),
        ProtocolConfig.DEFAULT_MAX_FRAME_BYTES,
        1024 * 1024,
        logDir.toString(),
        LogConfig.DEFAULT_FSYNC_POLICY,
        LogConfig.DEFAULT_FSYNC_INTERVAL_MS,
        LogConfig.DEFAULT_MAX_SEGMENT_BYTES,
        LogConfig.DEFAULT_INDEX_INTERVAL_BYTES,
        LogConfig.UNLIMITED,
        LogConfig.UNLIMITED,
        topicConfig,
        offsetDir.toString(),
        ClusterConfig.singleBroker(self),
        500,
        2000,
        500,
        60,
        120,
        20,
        200,
        RaftConfig.DEFAULT_MAX_ENTRIES_PER_APPEND,
        5000,
        3000);
  }
}
