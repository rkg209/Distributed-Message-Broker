package io.minikafka.log;

/** Controls when a {@link LogSegment} forces buffered writes to durable storage. */
public enum FsyncPolicy {
  /** Fsync after every append. Strongest durability, highest latency. */
  EVERY_WRITE,

  /** Fsync on a fixed interval via a background thread, not on every append. */
  PERIODIC,

  /** Never explicitly fsync; rely on the OS page cache flushing eventually. */
  OS_MANAGED
}
