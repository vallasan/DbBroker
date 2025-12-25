package com.omniva.dbbroker.engine.sensors;

import lombok.Builder;
import lombok.Data;

import java.time.Duration;
import java.time.Instant;

/**
 * Monitoring data for individual DbBrokerListener
 * Contains metrics and status for a single listener thread
 */
@Builder
@Data
public class ListenerSensor {
    // Identity
    private int threadId;
    private String listenerName;
    private String queueName;

    // Status
    private boolean running;
    private Instant startTime;
    private Instant lastMessageTime;
    private Duration uptime;

    // Metrics
    private long messagesProcessed;
    private long errorsEncountered;

    // Derived metrics for individual listener
    public double getMessagesPerSecond() {
        long uptimeSeconds = uptime.getSeconds();
        return uptimeSeconds > 0 ? (double) messagesProcessed / uptimeSeconds : 0.0;
    }

    public double getErrorRate() {
        long total = messagesProcessed + errorsEncountered;
        return total > 0 ? (double) errorsEncountered / total : 0.0;
    }

    public long getTotalThroughput() {
        return messagesProcessed + errorsEncountered;
    }

    public boolean hasRecentActivity() {
        return lastMessageTime != null &&
                Duration.between(lastMessageTime, Instant.now()).toMinutes() < 5;
    }
}