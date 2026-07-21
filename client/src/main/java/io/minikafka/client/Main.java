package io.minikafka.client;

import io.minikafka.protocol.BrokerInfo;
import io.minikafka.protocol.PartitionMetadata;
import io.minikafka.protocol.PollResp;
import io.minikafka.protocol.ProtocolConfig;
import io.minikafka.protocol.TopicMetadata;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.List;

/**
 * Manual-testing CLI for a running broker. Supports metadata inspection, publishing, and consuming
 * (with optional consumer-group offset tracking) over the real wire protocol.
 *
 * <p>Usage:
 *
 * <pre>
 *   Main &lt;host&gt; &lt;port&gt; metadata
 *   Main &lt;host&gt; &lt;port&gt; produce &lt;topic&gt; [key] &lt;message...&gt;
 *   Main &lt;host&gt; &lt;port&gt; produce &lt;topic&gt; --stdin
 *   Main &lt;host&gt; &lt;port&gt; consume &lt;topic&gt; &lt;partition&gt; [--group groupId] [--from-offset N]
 * </pre>
 */
public final class Main {

  public static void main(String[] args) throws Exception {
    if (args.length < 3) {
      printUsage();
      System.exit(1);
    }
    String host = args[0];
    int port = Integer.parseInt(args[1]);
    String command = args[2];

    try (BrokerConnection conn =
        new BrokerConnection(host, port, ProtocolConfig.DEFAULT_MAX_FRAME_BYTES)) {
      switch (command) {
        case "metadata" -> runMetadata(conn);
        case "produce" -> runProduce(conn, args);
        case "consume" -> runConsume(conn, args);
        default -> {
          System.err.println("Unknown command: " + command);
          printUsage();
          System.exit(1);
        }
      }
    }
  }

  private static void runMetadata(BrokerConnection conn) throws IOException {
    MetadataClient metadataClient = new MetadataClient(conn);
    List<BrokerInfo> brokers = metadataClient.fetchMetadata();
    System.out.println("Brokers:");
    for (BrokerInfo b : brokers) {
      System.out.println("  " + b.brokerId() + " @ " + b.host() + ":" + b.port());
    }
    System.out.println("Topics:");
    for (TopicMetadata t : metadataClient.cachedTopics()) {
      System.out.println("  " + t.topic() + " (" + t.partitions().size() + " partitions)");
      for (PartitionMetadata p : t.partitions()) {
        System.out.println("    partition " + p.partitionId() + " leader=" + p.leaderId());
      }
    }
  }

  private static void runProduce(BrokerConnection conn, String[] args) throws IOException {
    if (args.length < 4) {
      System.err.println("Usage: Main <host> <port> produce <topic> [key] <message>");
      System.err.println("       Main <host> <port> produce <topic> --stdin  (one message/line)");
      System.exit(1);
      return;
    }
    String topic = args[3];
    ProducerClient producer = new ProducerClient(conn);

    if (args.length == 5 && args[4].equals("--stdin")) {
      try (BufferedReader reader =
          new BufferedReader(new InputStreamReader(System.in, StandardCharsets.UTF_8))) {
        String line;
        int count = 0;
        while ((line = reader.readLine()) != null) {
          if (line.isEmpty()) {
            continue;
          }
          ProducerClient.PublishAck ack =
              producer.publish(topic, null, line.getBytes(StandardCharsets.UTF_8));
          System.out.println(
              "published -> partition=" + ack.partition() + " offset=" + ack.offset());
          count++;
        }
        System.out.println("Published " + count + " message(s) from stdin.");
      }
      return;
    }

    byte[] key = null;
    String message;
    if (args.length >= 6) {
      key = args[4].getBytes(StandardCharsets.UTF_8);
      message = String.join(" ", java.util.Arrays.asList(args).subList(5, args.length));
    } else {
      message = args[4];
    }
    ProducerClient.PublishAck ack =
        producer.publish(topic, key, message.getBytes(StandardCharsets.UTF_8));
    System.out.println("published -> partition=" + ack.partition() + " offset=" + ack.offset());
  }

  private static void runConsume(BrokerConnection conn, String[] args) throws IOException {
    if (args.length < 5) {
      System.err.println(
          "Usage: Main <host> <port> consume <topic> <partition> [--group groupId]"
              + " [--from-offset N]");
      System.exit(1);
      return;
    }
    String topic = args[3];
    int partition = Integer.parseInt(args[4]);
    String group = null;
    long fromOffset = 0;
    boolean fromOffsetSet = false;

    for (int i = 5; i < args.length; i++) {
      switch (args[i]) {
        case "--group" -> group = args[++i];
        case "--from-offset" -> {
          fromOffset = Long.parseLong(args[++i]);
          fromOffsetSet = true;
        }
        default -> throw new IllegalArgumentException("Unknown flag: " + args[i]);
      }
    }

    ConsumerClient consumer =
        group != null
            ? new ConsumerClient(conn, topic, partition, group)
            : new ConsumerClient(conn, topic, partition, fromOffsetSet ? fromOffset : 0);

    System.out.println(
        "Consuming topic="
            + topic
            + " partition="
            + partition
            + " startOffset="
            + consumer.currentOffset()
            + (group != null ? " group=" + group : ""));
    System.out.println("Polling once; re-run to fetch the next batch. Ctrl+C to stop.");

    List<PollResp.Record> records = consumer.poll();
    if (records.isEmpty()) {
      System.out.println("No new records (caught up at offset " + consumer.currentOffset() + ").");
    } else {
      for (PollResp.Record r : records) {
        System.out.println(
            "offset=" + r.offset() + " value=" + new String(r.payload(), StandardCharsets.UTF_8));
      }
      if (group != null) {
        consumer.commitOffset();
        System.out.println("Committed offset " + consumer.currentOffset() + " for group " + group);
      } else {
        System.out.println("Next offset would be " + consumer.currentOffset());
      }
    }
  }

  private static void printUsage() {
    System.err.println("Usage:");
    System.err.println("  Main <host> <port> metadata");
    System.err.println("  Main <host> <port> produce <topic> [key] <message...>");
    System.err.println("  Main <host> <port> produce <topic> --stdin");
    System.err.println(
        "  Main <host> <port> consume <topic> <partition> [--group groupId] [--from-offset N]");
  }

  private Main() {}
}
