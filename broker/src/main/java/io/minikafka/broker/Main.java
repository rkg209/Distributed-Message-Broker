package io.minikafka.broker;

import io.minikafka.protocol.BrokerInfo;
import java.io.IOException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Entry point for running a broker process standalone (manual testing, Docker). Reads {@link
 * BrokerConfig} from the environment, starts a {@link ConnectionAcceptor} with the Spec 02 {@link
 * BrokerRequestHandler}, and blocks until interrupted.
 */
public final class Main {

  private static final Logger log = LoggerFactory.getLogger(Main.class);

  public static void main(String[] args) throws IOException, InterruptedException {
    BrokerConfig config = BrokerConfig.fromEnv();
    BrokerInfo self = new BrokerInfo(config.brokerId(), config.brokerHost(), config.brokerPort());

    TopicRegistry topicRegistry = new TopicRegistry();
    PartitionManager partitionManager = new PartitionManager(topicRegistry);
    BrokerRequestHandler handler =
        new BrokerRequestHandler(self, partitionManager, config.maxPollBytes());

    ConnectionAcceptor acceptor =
        new ConnectionAcceptor(config.brokerPort(), config.maxFrameBytes(), handler);
    acceptor.start();
    log.info(
        "Broker {} listening on {}:{}",
        config.brokerId(),
        config.brokerHost(),
        acceptor.boundPort());

    Runtime.getRuntime()
        .addShutdownHook(
            new Thread(
                () -> {
                  acceptor.close();
                  topicRegistry.close();
                },
                "broker-shutdown"));
    Thread.currentThread().join();
  }

  private Main() {}
}
