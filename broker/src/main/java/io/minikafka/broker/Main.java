package io.minikafka.broker;

import io.minikafka.log.DiskPartitionLog;
import io.minikafka.protocol.BrokerInfo;
import java.io.IOException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Entry point for running a broker process standalone (manual testing, Docker). Reads {@link
 * BrokerConfig} from the environment, starts a {@link ConnectionAcceptor} with the {@link
 * BrokerRequestHandler} over a durable, disk-backed log, and blocks until interrupted.
 */
public final class Main {

  private static final Logger log = LoggerFactory.getLogger(Main.class);

  public static void main(String[] args) throws IOException, InterruptedException {
    BrokerConfig config = BrokerConfig.fromEnv();
    BrokerInfo self = new BrokerInfo(config.brokerId(), config.brokerHost(), config.brokerPort());
    ClusterConfig clusterConfig = config.clusterConfig();

    MetadataService metadataService =
        new MetadataService(self, config.topicConfig(), clusterConfig);
    TopicRegistry topicRegistry =
        new TopicRegistry(tp -> new DiskPartitionLog(config.logConfigFor(tp)));
    PartitionManager partitionManager = new PartitionManager(topicRegistry, metadataService);
    ConsumerGroupManager consumerGroupManager = new ConsumerGroupManager(config.offsetDirPath());
    BrokerRequestHandler handler =
        new BrokerRequestHandler(
            metadataService, partitionManager, consumerGroupManager, config.maxPollBytes());

    ConnectionAcceptor acceptor =
        new ConnectionAcceptor(config.brokerPort(), config.maxFrameBytes(), handler);
    acceptor.start();
    log.info(
        "Broker {} listening on {}:{}",
        config.brokerId(),
        config.brokerHost(),
        acceptor.boundPort());

    HeartbeatMonitor heartbeatMonitor =
        new HeartbeatMonitor(
            self,
            clusterConfig.peersOf(config.brokerId()),
            config.heartbeatIntervalMs(),
            config.heartbeatTimeoutMs(),
            config.peerReconnectBackoffMs());
    heartbeatMonitor.start();
    log.info(
        "Broker {} joined cluster of {} brokers (controller={}, self is controller: {})",
        config.brokerId(),
        clusterConfig.brokers().size(),
        clusterConfig.controllerId(),
        metadataService.isController());

    Runtime.getRuntime()
        .addShutdownHook(
            new Thread(
                () -> {
                  heartbeatMonitor.close();
                  acceptor.close();
                  topicRegistry.close();
                  consumerGroupManager.close();
                },
                "broker-shutdown"));
    Thread.currentThread().join();
  }

  private Main() {}
}
