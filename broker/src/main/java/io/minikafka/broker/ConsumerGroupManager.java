package io.minikafka.broker;

import io.minikafka.protocol.FetchOffsetResp;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Stream;

/**
 * Durable committed-offset storage for consumer groups: one {@link OffsetStore} per group, backed
 * by an in-memory cache for fast {@code fetch}. Every existing {@code *.offsets} file under {@code
 * offsetDir} is loaded on construction so a restarted broker answers {@code FETCH_OFFSET_REQ}
 * correctly without waiting for a fresh commit.
 */
public final class ConsumerGroupManager implements AutoCloseable {

  private static final String OFFSETS_SUFFIX = ".offsets";

  private final Path offsetDir;
  private final ConcurrentHashMap<String, OffsetStore> stores = new ConcurrentHashMap<>();
  private final ConcurrentHashMap<GroupTopicPartition, Long> offsets = new ConcurrentHashMap<>();

  public ConsumerGroupManager(Path offsetDir) {
    this.offsetDir = offsetDir;
    loadExistingGroups();
  }

  /**
   * Durably commits {@code offset} for {@code (group, topic, partition)}, then updates the cache.
   */
  public void commit(String group, String topic, int partition, long offset) {
    OffsetStore store = storeFor(group);
    synchronized (store) {
      try {
        store.append(topic, partition, offset);
      } catch (IOException e) {
        throw new UncheckedIOException(
            "Failed to durably commit offset for group " + group + "/" + topic + "-" + partition,
            e);
      }
      offsets.put(new GroupTopicPartition(group, topic, partition), offset);
    }
  }

  /** Returns the last committed offset, or {@link FetchOffsetResp#NO_OFFSET} if none exists. */
  public long fetch(String group, String topic, int partition) {
    Long offset = offsets.get(new GroupTopicPartition(group, topic, partition));
    return offset == null ? FetchOffsetResp.NO_OFFSET : offset;
  }

  private void loadExistingGroups() {
    try {
      Files.createDirectories(offsetDir);
      try (Stream<Path> files = Files.list(offsetDir)) {
        files
            .filter(p -> p.getFileName().toString().endsWith(OFFSETS_SUFFIX))
            .forEach(this::loadGroupFile);
      }
    } catch (IOException e) {
      throw new UncheckedIOException("Failed to load consumer group offsets from " + offsetDir, e);
    }
  }

  private void loadGroupFile(Path path) {
    String fileName = path.getFileName().toString();
    String groupId = fileName.substring(0, fileName.length() - OFFSETS_SUFFIX.length());
    storeFor(groupId);
  }

  private OffsetStore storeFor(String group) {
    return stores.computeIfAbsent(
        group,
        g -> {
          try {
            OffsetStore store = new OffsetStore(offsetDir, g);
            store
                .recoveredOffsets()
                .forEach(
                    (tp, offset) ->
                        offsets.put(
                            new GroupTopicPartition(g, tp.topic(), tp.partition()), offset));
            return store;
          } catch (IOException e) {
            throw new UncheckedIOException("Failed to open offset store for group " + g, e);
          }
        });
  }

  @Override
  public void close() {
    stores
        .values()
        .forEach(
            store -> {
              try {
                store.close();
              } catch (IOException e) {
                throw new UncheckedIOException("Failed to close an offset store", e);
              }
            });
  }
}
