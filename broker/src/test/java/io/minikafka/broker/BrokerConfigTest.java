package io.minikafka.broker;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.minikafka.log.FsyncPolicy;
import io.minikafka.log.LogConfig;
import io.minikafka.protocol.ProtocolConfig;
import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;

class BrokerConfigTest {

  private static Map<String, String> baseEnv() {
    Map<String, String> env = new HashMap<>();
    env.put("BROKER_ID", "1");
    env.put("BROKER_HOST", "localhost");
    env.put("BROKER_PORT", "9092");
    env.put("BROKER_LOG_DIR", "/tmp/mini-kafka-test");
    return env;
  }

  @Test
  void parsesAllFieldsFromEnv() {
    Map<String, String> env = baseEnv();

    BrokerConfig config = BrokerConfig.fromEnv(env::get);

    assertEquals(1, config.brokerId());
    assertEquals("localhost", config.brokerHost());
    assertEquals(9092, config.brokerPort());
    assertEquals("/tmp/mini-kafka-test", config.logDir());
  }

  @Test
  void defaultsMaxFrameBytesWhenUnset() {
    BrokerConfig config = BrokerConfig.fromEnv(baseEnv()::get);

    assertEquals(ProtocolConfig.DEFAULT_MAX_FRAME_BYTES, config.maxFrameBytes());
  }

  @Test
  void parsesMaxFrameBytesWhenSet() {
    Map<String, String> env = baseEnv();
    env.put("BROKER_MAX_FRAME_BYTES", "1048576");

    BrokerConfig config = BrokerConfig.fromEnv(env::get);

    assertEquals(1048576, config.maxFrameBytes());
  }

  @Test
  void defaultsMaxPollBytesWhenUnset() {
    BrokerConfig config = BrokerConfig.fromEnv(baseEnv()::get);

    assertEquals(1024 * 1024, config.maxPollBytes());
  }

  @Test
  void parsesMaxPollBytesWhenSet() {
    Map<String, String> env = baseEnv();
    env.put("BROKER_MAX_POLL_BYTES", "2048");

    BrokerConfig config = BrokerConfig.fromEnv(env::get);

    assertEquals(2048, config.maxPollBytes());
  }

  @Test
  void throwsWhenRequiredVariableIsMissing() {
    Map<String, String> env = Map.of("BROKER_HOST", "localhost", "BROKER_PORT", "9092");

    assertThrows(IllegalStateException.class, () -> BrokerConfig.fromEnv(env::get));
  }

  @Test
  void throwsWhenLogDirIsMissing() {
    Map<String, String> env = baseEnv();
    env.remove("BROKER_LOG_DIR");

    assertThrows(IllegalStateException.class, () -> BrokerConfig.fromEnv(env::get));
  }

  @Test
  void throwsWhenPortIsNotAnInteger() {
    Map<String, String> env = baseEnv();
    env.put("BROKER_PORT", "not-a-number");

    assertThrows(IllegalStateException.class, () -> BrokerConfig.fromEnv(env::get));
  }

  @Test
  void defaultsLogSettingsWhenUnset() {
    BrokerConfig config = BrokerConfig.fromEnv(baseEnv()::get);

    assertEquals(LogConfig.DEFAULT_FSYNC_POLICY, config.fsyncPolicy());
    assertEquals(LogConfig.DEFAULT_FSYNC_INTERVAL_MS, config.fsyncIntervalMs());
    assertEquals(LogConfig.DEFAULT_MAX_SEGMENT_BYTES, config.segmentBytes());
    assertEquals(LogConfig.DEFAULT_INDEX_INTERVAL_BYTES, config.indexIntervalBytes());
    assertEquals(LogConfig.UNLIMITED, config.retentionBytes());
    assertEquals(LogConfig.UNLIMITED, config.retentionMs());
  }

  @Test
  void parsesLogSettingsWhenSet() {
    Map<String, String> env = baseEnv();
    env.put("BROKER_FSYNC_POLICY", "PERIODIC");
    env.put("BROKER_FSYNC_INTERVAL_MS", "500");
    env.put("BROKER_SEGMENT_BYTES", "2048");
    env.put("BROKER_INDEX_INTERVAL_BYTES", "128");
    env.put("BROKER_RETENTION_BYTES", "1000000");
    env.put("BROKER_RETENTION_MS", "60000");

    BrokerConfig config = BrokerConfig.fromEnv(env::get);

    assertEquals(FsyncPolicy.PERIODIC, config.fsyncPolicy());
    assertEquals(500, config.fsyncIntervalMs());
    assertEquals(2048, config.segmentBytes());
    assertEquals(128, config.indexIntervalBytes());
    assertEquals(1000000, config.retentionBytes());
    assertEquals(60000, config.retentionMs());
  }

  @Test
  void throwsWhenFsyncPolicyIsInvalid() {
    Map<String, String> env = baseEnv();
    env.put("BROKER_FSYNC_POLICY", "NOT_A_POLICY");

    assertThrows(IllegalStateException.class, () -> BrokerConfig.fromEnv(env::get));
  }

  @Test
  void logConfigForBuildsPerPartitionDirectory() {
    BrokerConfig config = BrokerConfig.fromEnv(baseEnv()::get);

    LogConfig logConfig = config.logConfigFor(new TopicPartition("orders", 0));

    assertEquals(java.nio.file.Path.of("/tmp/mini-kafka-test", "orders-0"), logConfig.dir());
  }

  @Test
  void defaultsTopicConfigWhenUnset() {
    BrokerConfig config = BrokerConfig.fromEnv(baseEnv()::get);

    assertEquals(1, config.topicConfig().partitionCountFor("anything"));
  }

  @Test
  void parsesBrokerTopicsAndDefaultPartitions() {
    Map<String, String> env = baseEnv();
    env.put("BROKER_TOPICS", "orders:4,events:8");
    env.put("BROKER_DEFAULT_PARTITIONS", "2");

    BrokerConfig config = BrokerConfig.fromEnv(env::get);

    assertEquals(4, config.topicConfig().partitionCountFor("orders"));
    assertEquals(8, config.topicConfig().partitionCountFor("events"));
    assertEquals(2, config.topicConfig().partitionCountFor("unlisted"));
  }

  @Test
  void throwsOnMalformedBrokerTopics() {
    Map<String, String> env = baseEnv();
    env.put("BROKER_TOPICS", "orders-4");

    assertThrows(IllegalStateException.class, () -> BrokerConfig.fromEnv(env::get));
  }

  @Test
  void defaultsOffsetDirUnderLogDir() {
    BrokerConfig config = BrokerConfig.fromEnv(baseEnv()::get);

    assertEquals(
        java.nio.file.Path.of("/tmp/mini-kafka-test", "__offsets"), config.offsetDirPath());
  }

  @Test
  void parsesOffsetDirWhenSet() {
    Map<String, String> env = baseEnv();
    env.put("BROKER_OFFSET_DIR", "/tmp/custom-offsets");

    BrokerConfig config = BrokerConfig.fromEnv(env::get);

    assertEquals(java.nio.file.Path.of("/tmp/custom-offsets"), config.offsetDirPath());
  }

  @Test
  void absentBrokerListDegradesToSingleBrokerCluster() {
    BrokerConfig config = BrokerConfig.fromEnv(baseEnv()::get);

    assertEquals(1, config.clusterConfig().brokers().size());
    assertEquals(1, config.clusterConfig().controllerId());
    assertTrue(config.clusterConfig().isController(1));
  }

  @Test
  void parsesClusterConfigFromEnv() {
    Map<String, String> env = baseEnv();
    env.put("BROKER_TOPICS", "orders:2");
    env.put("BROKER_LIST", "1@broker-1:9092,2@broker-2:9092,3@broker-3:9092");
    env.put("PARTITION_ASSIGNMENTS", "orders:0=1,2,3");
    env.put("REPLICATION_FACTOR", "3");
    env.put("CONTROLLER_ID", "2");

    BrokerConfig config = BrokerConfig.fromEnv(env::get);

    assertEquals(3, config.clusterConfig().brokers().size());
    assertEquals(2, config.clusterConfig().controllerId());
    assertEquals(3, config.clusterConfig().replicationFactor());
    assertTrue(config.clusterConfig().assignmentFor(new TopicPartition("orders", 0)).isPresent());
  }

  @Test
  void defaultsHeartbeatTimingsWhenUnset() {
    BrokerConfig config = BrokerConfig.fromEnv(baseEnv()::get);

    assertEquals(500, config.heartbeatIntervalMs());
    assertEquals(2000, config.heartbeatTimeoutMs());
    assertEquals(500, config.peerReconnectBackoffMs());
  }

  @Test
  void parsesHeartbeatTimingsWhenSet() {
    Map<String, String> env = baseEnv();
    env.put("BROKER_HEARTBEAT_INTERVAL_MS", "250");
    env.put("BROKER_HEARTBEAT_TIMEOUT_MS", "1000");
    env.put("BROKER_PEER_RECONNECT_BACKOFF_MS", "100");

    BrokerConfig config = BrokerConfig.fromEnv(env::get);

    assertEquals(250, config.heartbeatIntervalMs());
    assertEquals(1000, config.heartbeatTimeoutMs());
    assertEquals(100, config.peerReconnectBackoffMs());
  }
}
