package com.omniva.dbbroker.engine.sensors;

import lombok.Builder;
import lombok.Data;

import java.time.Duration;
import java.time.Instant;

/**
 * Monitoring data for DbBrokerSupervisor
 * Contains aggregated metrics and supervision status
 */
@Builder
@Data
public class SupervisorSensor {
    // Supervision Status
    private boolean supervising;
    private String queueName;
    private Instant supervisionStartTime;
    private Duration supervisionUptime;
    private boolean healthy;

    // Listener Counts
    private int configuredListeners;
    private int activeListeners;
    private int totalListeners;
    private int failedListeners;

    // Aggregated Metrics
    private long totalMessagesProcessed;
    private long totalErrorsEncountered;
    private int maxConsecutiveErrors;
    private double averageMessagesPerSecond;
    private Instant mostRecentMessageTime;

    // Supervisor-level derived metrics
    public double getOverallErrorRate() {
        long total = totalMessagesProcessed + totalErrorsEncountered;
        return total > 0 ? (double) totalErrorsEncountered / total : 0.0;
    }

    public boolean isFullyOperational() {
        return supervising && healthy && activeListeners == configuredListeners;
    }

    public boolean hasHighErrorRate() {
        return getOverallErrorRate() > 0.1; // More than 10%
    }
}