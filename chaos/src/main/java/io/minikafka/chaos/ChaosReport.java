package io.minikafka.chaos;

import io.minikafka.chaos.HistoryRecorder.History;

/** Everything a {@code ResultsWriter} needs to record one chaos run. */
public record ChaosReport(
    ChaosConfig config,
    int crashesInjected,
    int partitionsInjected,
    long messagesSent,
    long messagesAcked,
    long messagesReceived,
    long durationMs,
    History history) {}
