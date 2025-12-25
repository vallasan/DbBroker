package com.omniva.dbbroker.dashboard;

import com.omniva.dbbroker.engine.crankshaft.DbBrokerSupervisor;
import com.omniva.dbbroker.engine.fault.DbBrokerSupervisionException;
import com.omniva.dbbroker.engine.fault.ErrorTracker;
import com.omniva.dbbroker.engine.piston.DbBrokerListener;
import com.omniva.dbbroker.engine.sensors.ListenerSensor;
import com.omniva.dbbroker.engine.sensors.SupervisorSensor;
import com.omniva.dbbroker.engine.transmission.Transmitter;
import lombok.Getter;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Engine Control Unit (ECU) for DbBroker engine.
 * <p>
 * This component:
 * - Reads data from all engine sensors (listeners, supervisor)
 * - Processes and analyzes sensor data
 * - Generates diagnostic reports
 * - Monitors engine health and performance
 * - Provides real-time engine status
 * - Triggers alerts for engine problems
 */
public class DbBrokerECU {

    // Engine components
    private final DbBrokerSupervisor supervisor; // Supervisor (formerly crankshaft)
    private final ErrorTracker errorTracker; // Fault detection system

    // ECU internal metrics (like engine computer memory)
    private final AtomicLong totalHealthChecks = new AtomicLong(0);

    // Engine performance thresholds (like ECU calibration parameters)
    private static final double LOW_PERFORMANCE_THRESHOLD = 0.5; // messages per second

    public DbBrokerECU(DbBrokerSupervisor supervisor, ErrorTracker errorTracker) {
        this.supervisor = supervisor;
        this.errorTracker = errorTracker;
    }

    /**
     * Read all listener sensor data - like reading individual listener sensors
     */
    public List<ListenerSensor> readListenerSensors() {

        return supervisor.getActiveListeners().stream()
                .map(context -> {
                    DbBrokerListener listener = context.listener();
                    return ListenerSensor.builder()
                            .threadId(listener.getThreadId())
                            .listenerName(listener.getListenerName())
                            .queueName(listener.getQueueName())
                            .running(listener.isRunning())
                            .startTime(listener.getStartTime())
                            .lastMessageTime(listener.getLastMessageTime())
                            .uptime(listener.getUptime())
                            .messagesProcessed(listener.getTotalMessagesProcessed())
                            .errorsEncountered(listener.getTotalErrorsEncountered())
                            .build();
                })
                .toList();
    }

    /**
     * Read supervisor sensor data - like reading engine coordination and timing
     */
    public SupervisorSensor readSupervisorSensor() {
        List<ListenerSensor> listenerSensors = readListenerSensors();

        // ECU processes raw sensor data into meaningful metrics
        long totalMessages = listenerSensors.stream()
                .mapToLong(ListenerSensor::getMessagesProcessed)
                .sum();

        long totalErrors = listenerSensors.stream()
                .mapToLong(ListenerSensor::getErrorsEncountered)
                .sum();

        double avgMessagesPerSecond = listenerSensors.stream()
                .mapToDouble(ListenerSensor::getMessagesPerSecond)
                .average().orElse(0.0);

        Instant mostRecentMessage = listenerSensors.stream()
                .map(ListenerSensor::getLastMessageTime)
                .filter(Objects::nonNull)
                .max(Instant::compareTo)
                .orElse(null);

        int activeCount = supervisor.getActiveListenerCount();
        int configuredCount = supervisor.getConfiguredListenerCount();
        int failedCount = supervisor.getTotalListenerCount() - activeCount;

        return SupervisorSensor.builder()
                .supervising(supervisor.isSupervising())
                .queueName(supervisor.getCurrentQueueName())
                .supervisionStartTime(supervisor.getSupervisionStartTime())
                .supervisionUptime(supervisor.getSupervisionUptime())
                .healthy(supervisor.isHealthy())
                .configuredListeners(configuredCount)
                .activeListeners(activeCount)
                .totalListeners(supervisor.getTotalListenerCount())
                .failedListeners(failedCount)
                .totalMessagesProcessed(totalMessages)
                .totalErrorsEncountered(totalErrors)
                .averageMessagesPerSecond(avgMessagesPerSecond)
                .mostRecentMessageTime(mostRecentMessage)
                .build();
    }

    /**
     * Perform engine health check - like checking engine warning lights
     */
    public EngineHealthStatus performHealthCheck() {

        totalHealthChecks.incrementAndGet();

        SupervisorSensor supervisorSensor = readSupervisorSensor();
        List<ListenerSensor> listenerSensors = readListenerSensors();

        boolean hasRecentErrors = errorTracker.hasRecentErrors();

        // Primary health indicators
        boolean supervisorHealthy = supervisorSensor.isFullyOperational();
        boolean lowErrorRate = !supervisorSensor.hasHighErrorRate();
        boolean noRecentErrors = !hasRecentErrors;

        // Secondary health indicators
        boolean allListenersActive = listenerSensors.stream().allMatch(ListenerSensor::isRunning);
        boolean goodPerformance = supervisorSensor.getAverageMessagesPerSecond() > LOW_PERFORMANCE_THRESHOLD;
        boolean recentActivity = listenerSensors.stream().anyMatch(ListenerSensor::hasRecentActivity);

        // Overall health assessment
        boolean engineHealthy = supervisorHealthy && lowErrorRate && noRecentErrors;
        boolean enginePerforming = allListenersActive && goodPerformance && recentActivity;

        return EngineHealthStatus.builder()
                .healthy(engineHealthy)
                .performing(enginePerforming)
                .supervisorOperational(supervisorHealthy)
                .lowErrorRate(lowErrorRate)
                .noRecentErrors(noRecentErrors)
                .allListenersActive(allListenersActive)
                .goodPerformance(goodPerformance)
                .recentActivity(recentActivity)
                .activeListeners(listenerSensors.size())
                .configuredListeners(supervisorSensor.getConfiguredListeners())
                .errorRate(supervisorSensor.getOverallErrorRate())
                .messagesPerSecond(supervisorSensor.getAverageMessagesPerSecond())
                .checkTimestamp(Instant.now())
                .build();
    }

    /**
     * Get engine performance metrics - like reading engine RPM, temperature, etc.
     */
    public EnginePerformanceMetrics getPerformanceMetrics() {
        SupervisorSensor supervisorSensor = readSupervisorSensor();
        List<ListenerSensor> listenerSensors = readListenerSensors();

        // Calculate performance statistics
        double totalMessagesPerSecond = listenerSensors.stream()
                .mapToDouble(ListenerSensor::getMessagesPerSecond)
                .sum();

        double averageErrorRate = listenerSensors.stream()
                .mapToDouble(ListenerSensor::getErrorRate)
                .average()
                .orElse(0.0);

        long totalThroughput = listenerSensors.stream()
                .mapToLong(ListenerSensor::getTotalThroughput)
                .sum();

        // Calculate listener synchronization ratio directly
        int activeListeners = listenerSensors.size();
        int configuredListeners = supervisor.getConfiguredListenerCount();
        double listenerSynchronizationRatio = configuredListeners > 0 ?
                (double) activeListeners / configuredListeners : 0.0;

        return EnginePerformanceMetrics.builder()
                .totalMessagesPerSecond(totalMessagesPerSecond)
                .averageMessagesPerSecond(supervisorSensor.getAverageMessagesPerSecond())
                .totalThroughput(totalThroughput)
                .overallErrorRate(supervisorSensor.getOverallErrorRate())
                .averageErrorRate(averageErrorRate)
                .activeListeners(listenerSensors.size())
                .listenerSynchronization(listenerSynchronizationRatio)
                .engineUptime(supervisorSensor.getSupervisionUptime())
                .lastActivity(supervisorSensor.getMostRecentMessageTime())
                .build();
    }

    /**
     * Start supervision - Cold engine start
     */
    public void startSupervision() {
        try {
            if (supervisor.isSupervising()) {
                throw new IllegalStateException("Supervisor is already running");
            }

            // Get default configuration for cold start
            String defaultQueueName = supervisor.getCurrentQueueName();
            Transmitter defaultTransmitter = supervisor.getCurrentTransmitter().get();

            if (defaultQueueName == null || defaultTransmitter == null) {
                String errorMsg = "Cannot start - missing default configuration (queue name or transmitter)";
                throw new IllegalStateException(errorMsg);
            }

            supervisor.startSupervision(defaultQueueName, defaultTransmitter);

        } catch (DbBrokerSupervisionException e) {
            throw new RuntimeException("Failed to start supervision: " + e.getMessage(), e);
        }
    }

    /**
     * Stop supervision - Emergency engine shutdown
     */
    public void stopSupervision() {
        if (supervisor.isSupervising()) {
            supervisor.stopSupervision(false);
        }
    }

    /**
     * Restart supervision - Full engine restart
     */
    public void restartSupervision() {
        try {
            if (supervisor.isSupervising()) {
                supervisor.restartSupervision();
            } else {

                String queueName = supervisor.getCurrentQueueName();
                Transmitter transmitter = supervisor.getCurrentTransmitter().get();

                if (queueName == null || transmitter == null) {
                    String errorMsg = "Cannot restart - no preserved state available. Use start instead.";
                    throw new IllegalStateException(errorMsg);
                }

                supervisor.startSupervision(queueName, transmitter);
            }

        } catch (DbBrokerSupervisionException e) {
            throw new RuntimeException("Failed to restart supervision: " + e.getMessage(), e);
        }
    }

    /**
     * Restart failed listeners - Selective engine repair
     */
    public void restartFailedListeners() {
        if (!supervisor.isSupervising()) {
            throw new IllegalStateException("Cannot restart listeners - supervisor not running");
        }

        // Get current status before restart
        int activeCountBefore = supervisor.getActiveListenerCount();
        int configuredCount = supervisor.getConfiguredListenerCount();
        int failedCount = configuredCount - activeCountBefore;

        if (failedCount == 0) {
            return;
        }

        supervisor.restartFailedListeners();

    }

    /**
     * Check if supervisor has preserved state for smart restart
     */
    private boolean hasPreservedState() {
        try {
            // Check if supervisor has preserved configuration
            String queueName = supervisor.getCurrentQueueName();
            boolean hasTransmitter = supervisor.getCurrentTransmitter().get() != null;

            return !supervisor.isSupervising() && queueName != null && hasTransmitter;
        } catch (Exception e) {
            return false;
        }
    }

    /**
     * Get detailed control status - For dashboard control panel
     */
    public ControlStatus getControlStatus() {
        try {
            SupervisorSensor supervisorSensor = readSupervisorSensor();

            int activeListeners = supervisorSensor.getActiveListeners();
            int configuredListeners = supervisorSensor.getConfiguredListeners();
            int failedListeners = configuredListeners - activeListeners;

            boolean supervising = supervisor.isSupervising();
            boolean hasPreservedState = hasPreservedState();

            boolean canRestart = supervising || hasPreservedState;
            boolean canRestartFailed = supervising && failedListeners > 0;

            return ControlStatus.builder()
                    .supervising(supervising)
                    .hasPreservedState(hasPreservedState)
                    .canStop(supervising)
                    .canRestart(canRestart)
                    .canRestartFailed(canRestartFailed)
                    .activeListeners(activeListeners)
                    .configuredListeners(configuredListeners)
                    .failedListeners(failedListeners)
                    .queueName(supervisor.getCurrentQueueName())
                    .supervisionUptime(supervisor.getSupervisionUptime())
                    .lastStatusCheck(Instant.now())
                    .build();

        } catch (Exception e) {
            return ControlStatus.builder()
                    .supervising(false)
                    .hasPreservedState(false)
                    .canStop(false)
                    .canRestart(false)
                    .canRestartFailed(false)
                    .lastStatusCheck(Instant.now())
                    .build();
        }
    }


    /**
     * Control Status - Dashboard control panel information
     */
    @Getter
    public static class ControlStatus {
        private final boolean supervising;
        private final boolean hasPreservedState;
        private final boolean canStop;
        private final boolean canRestart;
        private final boolean canRestartFailed;
        private final int activeListeners;
        private final int configuredListeners;
        private final int failedListeners;
        private final String queueName;
        private final Duration supervisionUptime;
        private final Instant lastStatusCheck;

        private ControlStatus(Builder builder) {
            this.supervising = builder.supervising;
            this.hasPreservedState = builder.hasPreservedState;
            this.canStop = builder.canStop;
            this.canRestart = builder.canRestart;
            this.canRestartFailed = builder.canRestartFailed;
            this.activeListeners = builder.activeListeners;
            this.configuredListeners = builder.configuredListeners;
            this.failedListeners = builder.failedListeners;
            this.queueName = builder.queueName;
            this.supervisionUptime = builder.supervisionUptime;
            this.lastStatusCheck = builder.lastStatusCheck;
        }

        public static Builder builder() {
            return new Builder();
        }

        public String getStatusSummary() {
            if (!supervising && !hasPreservedState) return "ENGINE STOPPED - COLD START REQUIRED";
            if (!supervising) return "ENGINE STOPPED - READY TO RESUME";
            if (failedListeners == 0) return "ALL SYSTEMS OPERATIONAL";
            return String.format("ENGINE RUNNING - %d LISTENERS NEED ATTENTION", failedListeners);
        }

        public static class Builder {
            private boolean supervising;
            private boolean hasPreservedState;
            private boolean canStop;
            private boolean canRestart;
            private boolean canRestartFailed;
            private int activeListeners;
            private int configuredListeners;
            private int failedListeners;
            private String queueName;
            private Duration supervisionUptime;
            private Instant lastStatusCheck;

            public Builder supervising(boolean supervising) { this.supervising = supervising; return this; }
            public Builder hasPreservedState(boolean hasPreservedState) { this.hasPreservedState = hasPreservedState; return this; }
            public Builder canStop(boolean canStop) { this.canStop = canStop; return this; }
            public Builder canRestart(boolean canRestart) { this.canRestart = canRestart; return this; }
            public Builder canRestartFailed(boolean canRestartFailed) { this.canRestartFailed = canRestartFailed; return this; }
            public Builder activeListeners(int activeListeners) { this.activeListeners = activeListeners; return this; }
            public Builder configuredListeners(int configuredListeners) { this.configuredListeners = configuredListeners; return this; }
            public Builder failedListeners(int failedListeners) { this.failedListeners = failedListeners; return this; }
            public Builder queueName(String queueName) { this.queueName = queueName; return this; }
            public Builder supervisionUptime(Duration supervisionUptime) { this.supervisionUptime = supervisionUptime; return this; }
            public Builder lastStatusCheck(Instant lastStatusCheck) { this.lastStatusCheck = lastStatusCheck; return this; }

            public ControlStatus build() {
                return new ControlStatus(this);
            }
        }
    }

    /**
     * Engine Health Status - comprehensive health assessment
     */
    public static class EngineHealthStatus {
        // Getters
        @Getter
        private final boolean healthy;
        @Getter
        private final boolean performing;
        @Getter
        private final boolean supervisorOperational;
        @Getter
        private final boolean lowErrorRate;
        @Getter
        private final boolean noRecentErrors;
        @Getter
        private final boolean allListenersActive;
        @Getter
        private final boolean goodPerformance;
        @Getter
        private final boolean recentActivity;
        @Getter
        private final int activeListeners;
        @Getter
        private final int configuredListeners;
        @Getter
        private final double errorRate;
        @Getter
        private final double messagesPerSecond;
        @Getter
        private final Instant checkTimestamp;

        private EngineHealthStatus(Builder builder) {
            this.healthy = builder.healthy;
            this.performing = builder.performing;
            this.supervisorOperational = builder.supervisorOperational;
            this.lowErrorRate = builder.lowErrorRate;
            this.noRecentErrors = builder.noRecentErrors;
            this.allListenersActive = builder.allListenersActive;
            this.goodPerformance = builder.goodPerformance;
            this.recentActivity = builder.recentActivity;
            this.activeListeners = builder.activeListeners;
            this.configuredListeners = builder.configuredListeners;
            this.errorRate = builder.errorRate;
            this.messagesPerSecond = builder.messagesPerSecond;
            this.checkTimestamp = builder.checkTimestamp;
        }

        public static Builder builder() {
            return new Builder();
        }

        public String getOverallStatus() {
            if (healthy && performing) return "OPTIMAL";
            if (healthy) return "HEALTHY";
            if (performing) return "DEGRADED";
            return "CRITICAL";
        }

        public static class Builder {
            private boolean healthy;
            private boolean performing;
            private boolean supervisorOperational;
            private boolean lowErrorRate;
            private boolean noRecentErrors;
            private boolean allListenersActive;
            private boolean goodPerformance;
            private boolean recentActivity;
            private int activeListeners;
            private int configuredListeners;
            private double errorRate;
            private double messagesPerSecond;
            private Instant checkTimestamp;

            public Builder healthy(boolean healthy) { this.healthy = healthy; return this; }
            public Builder performing(boolean performing) { this.performing = performing; return this; }
            public Builder supervisorOperational(boolean supervisorOperational) { this.supervisorOperational = supervisorOperational; return this; }
            public Builder lowErrorRate(boolean lowErrorRate) { this.lowErrorRate = lowErrorRate; return this; }
            public Builder noRecentErrors(boolean noRecentErrors) { this.noRecentErrors = noRecentErrors; return this; }
            public Builder allListenersActive(boolean allListenersActive) { this.allListenersActive = allListenersActive; return this; }
            public Builder goodPerformance(boolean goodPerformance) { this.goodPerformance = goodPerformance; return this; }
            public Builder recentActivity(boolean recentActivity) { this.recentActivity = recentActivity; return this; }
            public Builder activeListeners(int activeListeners) { this.activeListeners = activeListeners; return this; }
            public Builder configuredListeners(int configuredListeners) { this.configuredListeners = configuredListeners; return this; }
            public Builder errorRate(double errorRate) { this.errorRate = errorRate; return this; }
            public Builder messagesPerSecond(double messagesPerSecond) { this.messagesPerSecond = messagesPerSecond; return this; }
            public Builder checkTimestamp(Instant checkTimestamp) { this.checkTimestamp = checkTimestamp; return this; }

            public EngineHealthStatus build() {
                return new EngineHealthStatus(this);
            }
        }
    }

    /**
     * Engine Performance Metrics - detailed performance data
     */
    @Getter
    public static class EnginePerformanceMetrics {
        // Getters
        private final double totalMessagesPerSecond;
        private final double averageMessagesPerSecond;
        private final long totalThroughput;
        private final double overallErrorRate;
        private final double averageErrorRate;
        private final int activeListeners;
        private final double listenerSynchronization;
        private final Duration engineUptime;
        private final Instant lastActivity;

        private EnginePerformanceMetrics(Builder builder) {
            this.totalMessagesPerSecond = builder.totalMessagesPerSecond;
            this.averageMessagesPerSecond = builder.averageMessagesPerSecond;
            this.totalThroughput = builder.totalThroughput;
            this.overallErrorRate = builder.overallErrorRate;
            this.averageErrorRate = builder.averageErrorRate;
            this.activeListeners = builder.activeListeners;
            this.listenerSynchronization = builder.listenerSynchronization;
            this.engineUptime = builder.engineUptime;
            this.lastActivity = builder.lastActivity;
        }

        public static Builder builder() {
            return new Builder();
        }

        public static class Builder {
            private double totalMessagesPerSecond;
            private double averageMessagesPerSecond;
            private long totalThroughput;
            private double overallErrorRate;
            private double averageErrorRate;
            private int activeListeners;
            private double listenerSynchronization;
            private Duration engineUptime;
            private Instant lastActivity;

            public Builder totalMessagesPerSecond(double totalMessagesPerSecond) { this.totalMessagesPerSecond = totalMessagesPerSecond; return this; }
            public Builder averageMessagesPerSecond(double averageMessagesPerSecond) { this.averageMessagesPerSecond = averageMessagesPerSecond; return this; }
            public Builder totalThroughput(long totalThroughput) { this.totalThroughput = totalThroughput; return this; }
            public Builder overallErrorRate(double overallErrorRate) { this.overallErrorRate = overallErrorRate; return this; }
            public Builder averageErrorRate(double averageErrorRate) { this.averageErrorRate = averageErrorRate; return this; }
            public Builder activeListeners(int activeListeners) { this.activeListeners = activeListeners; return this; }
            public Builder listenerSynchronization(double listenerSynchronization) { this.listenerSynchronization = listenerSynchronization; return this; }
            public Builder engineUptime(Duration engineUptime) { this.engineUptime = engineUptime; return this; }
            public Builder lastActivity(Instant lastActivity) { this.lastActivity = lastActivity; return this; }

            public EnginePerformanceMetrics build() {
                return new EnginePerformanceMetrics(this);
            }
        }
    }
}