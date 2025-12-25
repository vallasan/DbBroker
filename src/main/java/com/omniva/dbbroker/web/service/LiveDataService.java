package com.omniva.dbbroker.web.service;

import com.omniva.dbbroker.dashboard.DbBrokerECU;
import com.omniva.dbbroker.engine.fault.ErrorTracker;
import com.omniva.dbbroker.engine.sensors.ListenerSensor;
import com.omniva.dbbroker.engine.sensors.SupervisorSensor;
import com.omniva.dbbroker.web.constants.DashboardConstants;
import com.omniva.dbbroker.web.dto.ErrorStatistics;
import com.omniva.dbbroker.web.util.ErrorListUtils;
import com.omniva.dbbroker.web.util.ErrorStatisticsCalculator;
import com.omniva.dbbroker.web.util.UptimeFormatter;

import java.time.Instant;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class LiveDataService {

    private final DbBrokerECU dbBrokerECU;
    private final ErrorTracker errorTracker;

    public LiveDataService(DbBrokerECU dbBrokerECU, ErrorTracker errorTracker) {
        this.dbBrokerECU = dbBrokerECU;
        this.errorTracker = errorTracker;
    }

    /**
     * Get all live/dynamic data for AJAX updates
     * This combines data from all ECU capabilities
     */
    public Map<String, Object> getLiveData() {
        Map<String, Object> liveData = new HashMap<>();

        try {
            // 1. Engine Status (from ECU ControlStatus)
            liveData.put("engineStatus", getEngineStatusData());

            // 2. Performance Metrics (from ECU PerformanceMetrics)
            liveData.put("metrics", getMetricsData());

            // 3. System Info (from ECU sensors)
            liveData.put("systemInfo", getSystemInfoData());

            // 4. Listener Data (from ECU ListenerSensors)
            liveData.put("listeners", getListenerData());

            // 5. Error Information (from ErrorTracker)
            liveData.put("errorInfo", getErrorInfoData());

            // 6. Health Status (from ECU HealthStatus)
            liveData.put("healthStatus", getHealthStatusData());

            // 7. Timestamp
            liveData.put("timestamp", Instant.now());
            liveData.put("success", true);

        } catch (Exception e) {
            liveData.put("success", false);
            liveData.put("error", "Failed to fetch live data: " + e.getMessage());
            liveData.put("timestamp", Instant.now());
        }

        return liveData;
    }

    /**
     * Clear all recent errors and return updated live data
     */
    public Map<String, Object> clearErrors() {
        try {
            // Clear the errors
            errorTracker.clearRecentErrors();

            // Get updated live data to show the cleared state
            Map<String, Object> liveData = getLiveData();

            // Add success message to the response
            liveData.put(DashboardConstants.SUCCESS_FIELD, true);
            liveData.put(DashboardConstants.MESSAGE_FIELD, DashboardConstants.ERRORS_CLEARED_SUCCESS);

            // Ensure timestamp reflects the clear operation
            liveData.put(DashboardConstants.TIMESTAMP_FIELD, Instant.now());

            return liveData;

        } catch (Exception e) {
            // If clearing fails, try to return current state with error message
            Map<String, Object> response = new HashMap<>();

            try {
                // Attempt to get current live data even if clear failed
                response = getLiveData();
            } catch (Exception getLiveDataException) {
                // If we can't get live data either, return minimal error response
                response.put(DashboardConstants.TIMESTAMP_FIELD, Instant.now());
            }

            // Add error information
            response.put(DashboardConstants.SUCCESS_FIELD, false);
            response.put(DashboardConstants.MESSAGE_FIELD, "Failed to clear errors: " + e.getMessage());
            response.put(DashboardConstants.ERROR_FIELD, e.getClass().getSimpleName());

            return response;
        }
    }

    /**
     * Engine status data (from ECU ControlStatus)
     */
    private Map<String, Object> getEngineStatusData() {
        DbBrokerECU.ControlStatus status = dbBrokerECU.getControlStatus();

        Map<String, Object> engineStatus = new HashMap<>();
        engineStatus.put("running", status.isSupervising());
        engineStatus.put("summary", status.getStatusSummary());
        engineStatus.put("hasPreservedState", status.isHasPreservedState());
        engineStatus.put("canStop", status.isCanStop());
        engineStatus.put("canRestart", status.isCanRestart());
        engineStatus.put("canRestartFailed", status.isCanRestartFailed());
        engineStatus.put("activeListeners", status.getActiveListeners());
        engineStatus.put("configuredListeners", status.getConfiguredListeners());
        engineStatus.put("failedListeners", status.getFailedListeners());
        engineStatus.put("queueName", status.getQueueName());
        engineStatus.put("lastStatusCheck", status.getLastStatusCheck());

        return engineStatus;
    }

    /**
     * Performance metrics (from ECU PerformanceMetrics)
     */
    private Map<String, Object> getMetricsData() {
        try {
            DbBrokerECU.EnginePerformanceMetrics performance = dbBrokerECU.getPerformanceMetrics();
            DbBrokerECU.ControlStatus status = dbBrokerECU.getControlStatus();

            Map<String, Object> metrics = new HashMap<>();
            metrics.put("throughput", String.format("%.2f msg/sec", performance.getTotalMessagesPerSecond()));
            metrics.put("averageThroughput", String.format("%.2f msg/sec", performance.getAverageMessagesPerSecond()));
            metrics.put("errorRate", String.format("%.2f%%", performance.getOverallErrorRate() * 100));
            metrics.put("averageErrorRate", String.format("%.2f%%", performance.getAverageErrorRate() * 100));
            metrics.put("activeListeners", performance.getActiveListeners());
            metrics.put("totalListeners", status.getConfiguredListeners());
            metrics.put("listenerSynchronization", String.format("%.1f%%", performance.getListenerSynchronization() * 100));
            metrics.put("totalThroughput", performance.getTotalThroughput());
            metrics.put("uptime", performance.getEngineUptime() != null ?
                    UptimeFormatter.formatUptime(performance.getEngineUptime()) : "00:00:00");
            metrics.put("lastActivity", performance.getLastActivity());

            return metrics;
        } catch (Exception e) {
            Map<String, Object> defaultMetrics = new HashMap<>();
            defaultMetrics.put("throughput", "0.00 msg/sec");
            defaultMetrics.put("averageThroughput", "0.00 msg/sec");
            defaultMetrics.put("errorRate", "0.00%");
            defaultMetrics.put("averageErrorRate", "0.00%");
            defaultMetrics.put("activeListeners", 0);
            defaultMetrics.put("totalListeners", 0);
            defaultMetrics.put("listenerSynchronization", "0.0%");
            defaultMetrics.put("totalThroughput", 0L);
            defaultMetrics.put("uptime", "00:00:00");
            defaultMetrics.put("lastActivity", null);
            return defaultMetrics;
        }
    }

    /**
     * System information (from ECU sensors)
     */
    private Map<String, Object> getSystemInfoData() {
        try {
            SupervisorSensor supervisorSensor = dbBrokerECU.readSupervisorSensor();

            Map<String, Object> systemInfo = new HashMap<>();
            systemInfo.put("engineRunning", supervisorSensor.isSupervising());
            systemInfo.put("queueName", supervisorSensor.getQueueName() != null ?
                    supervisorSensor.getQueueName() : "Not configured");
            systemInfo.put("healthy", supervisorSensor.isHealthy());
            systemInfo.put("fullyOperational", supervisorSensor.isFullyOperational());
            systemInfo.put("supervisionStartTime", supervisorSensor.getSupervisionStartTime());
            systemInfo.put("mostRecentMessageTime", supervisorSensor.getMostRecentMessageTime());

            return systemInfo;
        } catch (Exception e) {
            Map<String, Object> defaultSystemInfo = new HashMap<>();
            defaultSystemInfo.put("engineRunning", false);
            defaultSystemInfo.put("queueName", "Error");
            defaultSystemInfo.put("healthy", false);
            defaultSystemInfo.put("fullyOperational", false);
            defaultSystemInfo.put("supervisionStartTime", null);
            defaultSystemInfo.put("mostRecentMessageTime", null);
            return defaultSystemInfo;
        }
    }

    /**
     * Health status data (from ECU HealthStatus)
     */
    private Map<String, Object> getHealthStatusData() {
        try {
            DbBrokerECU.EngineHealthStatus health = dbBrokerECU.performHealthCheck();

            Map<String, Object> healthStatus = new HashMap<>();
            healthStatus.put("healthy", health.isHealthy());
            healthStatus.put("performing", health.isPerforming());
            healthStatus.put("overallStatus", health.getOverallStatus());
            healthStatus.put("supervisorOperational", health.isSupervisorOperational());
            healthStatus.put("lowErrorRate", health.isLowErrorRate());
            healthStatus.put("noRecentErrors", health.isNoRecentErrors());
            healthStatus.put("allListenersActive", health.isAllListenersActive());
            healthStatus.put("goodPerformance", health.isGoodPerformance());
            healthStatus.put("recentActivity", health.isRecentActivity());
            healthStatus.put("checkTimestamp", health.getCheckTimestamp());

            return healthStatus;
        } catch (Exception e) {
            Map<String, Object> defaultHealth = new HashMap<>();
            defaultHealth.put("healthy", false);
            defaultHealth.put("performing", false);
            defaultHealth.put("overallStatus", "UNKNOWN");
            defaultHealth.put("supervisorOperational", false);
            defaultHealth.put("lowErrorRate", false);
            defaultHealth.put("noRecentErrors", false);
            defaultHealth.put("allListenersActive", false);
            defaultHealth.put("goodPerformance", false);
            defaultHealth.put("recentActivity", false);
            defaultHealth.put("checkTimestamp", Instant.now());
            return defaultHealth;
        }
    }

    /**
     * Listener diagnostic data (from ECU ListenerSensors)
     */
    private List<Map<String, Object>> getListenerData() {
        try {
            List<ListenerSensor> listenerSensors = dbBrokerECU.readListenerSensors();
            List<Map<String, Object>> listeners = new ArrayList<>();

            for (ListenerSensor sensor : listenerSensors) {
                Map<String, Object> listenerData = new HashMap<>();
                listenerData.put("threadId", sensor.getThreadId());
                listenerData.put("listenerName", sensor.getListenerName());
                listenerData.put("queueName", sensor.getQueueName() != null ? sensor.getQueueName() : "Unknown");
                listenerData.put("status", determineListenerStatus(sensor));
                listenerData.put("messagesPerSecond", String.format("%.2f", sensor.getMessagesPerSecond()));
                listenerData.put("errorRate", String.format("%.1f%%", sensor.getErrorRate() * 100));
                listenerData.put("grade", calculateListenerGrade(sensor));
                listenerData.put("running", sensor.isRunning());
                listenerData.put("messagesProcessed", sensor.getMessagesProcessed());
                listenerData.put("errorsEncountered", sensor.getErrorsEncountered());
                listenerData.put("totalThroughput", sensor.getTotalThroughput());
                listenerData.put("uptime", sensor.getUptime() != null ?
                        UptimeFormatter.formatUptime(sensor.getUptime()) : "00:00:00");
                listenerData.put("hasRecentActivity", sensor.hasRecentActivity());
                listenerData.put("startTime", sensor.getStartTime());
                listenerData.put("lastMessageTime", sensor.getLastMessageTime());

                listeners.add(listenerData);
            }

            return listeners;
        } catch (Exception e) {
            return new ArrayList<>();
        }
    }

    /**
     * Determine listener status based on sensor data
     */
    private String determineListenerStatus(ListenerSensor sensor) {
        if (!sensor.isRunning()) {
            return "STOPPED";
        }

        if (sensor.getErrorRate() > 0.1) { // More than 10% error rate
            return "DEGRADED";
        }

        if (!sensor.hasRecentActivity()) {
            return "IDLE";
        }

        return "HEALTHY";
    }

    /**
     * Calculate listener performance grade
     */
    private String calculateListenerGrade(ListenerSensor sensor) {
        if (!sensor.isRunning()) {
            return "F";
        }

        double errorRate = sensor.getErrorRate();
        double messagesPerSecond = sensor.getMessagesPerSecond();

        // Grade based on error rate and throughput
        if (errorRate == 0.0 && messagesPerSecond > 1.0) {
            return "A";
        } else if (errorRate < 0.01 && messagesPerSecond > 0.5) { // Less than 1% error rate
            return "B";
        } else if (errorRate < 0.05 && messagesPerSecond > 0.1) { // Less than 5% error rate
            return "C";
        } else if (errorRate < 0.1) { // Less than 10% error rate
            return "D";
        } else {
            return "F";
        }
    }

    /**
     * Error information data (from ErrorTracker)
     */
    private Map<String, Object> getErrorInfoData() {
        try {
            List<String> recentErrors = errorTracker.getRecentErrors(DashboardConstants.RECENT_ERRORS_LIMIT);
            ErrorStatistics errorStats = ErrorStatisticsCalculator.calculateErrorStatistics(recentErrors);

            Map<String, Object> errorInfo = new HashMap<>();

            // Basic counts and flags
            errorInfo.put("totalCount", errorTracker.getRecentErrorCount());
            errorInfo.put("hasErrors", errorTracker.hasRecentErrors());
            errorInfo.put("hasHighErrorCount", errorTracker.hasHighErrorCount(DashboardConstants.ERROR_THRESHOLD));
            errorInfo.put("lastError", errorTracker.getLastError() != null ? errorTracker.getLastError() : "");

            // Keep raw error list for compatibility
            errorInfo.put("recentErrors", recentErrors != null ? recentErrors : new ArrayList<>());

            // Enhanced formatted error list
            List<Map<String, Object>> formattedErrors = new ArrayList<>();
            for (int i = 0; i < Objects.requireNonNull(recentErrors).size(); i++) {
                String error = recentErrors.get(i);
                Map<String, Object> errorEntry = new HashMap<>();

                String timestamp = extractTimestamp(error);
                String mainMessage = extractMainMessage(error);
                String exceptionInfo = extractExceptionInfo(error);

                errorEntry.put("index", i + 1);
                errorEntry.put("fullMessage", error);
                errorEntry.put("timestamp", timestamp);
                errorEntry.put("mainMessage", mainMessage);
                errorEntry.put("exceptionInfo", exceptionInfo);
                errorEntry.put("severity", determineSeverity(error));
                errorEntry.put("shortMessage", mainMessage.length() > 80 ? mainMessage.substring(0, 77) + "..." : mainMessage);
                errorEntry.put("hasException", !exceptionInfo.isEmpty());

                formattedErrors.add(errorEntry);
            }

            errorInfo.put("formattedErrors", ErrorListUtils.reverseFormattedErrorList(formattedErrors));

            // Statistics
            Map<String, Object> statistics = new HashMap<>();
            statistics.put("totalErrors", errorStats.getTotalErrors());
            statistics.put("criticalErrors", errorStats.getCriticalErrors());
            statistics.put("sqlErrors", errorStats.getSqlErrors());
            statistics.put("poisonMessages", errorStats.getPoisonMessages());
            statistics.put("generalErrors", errorStats.getGeneralErrors());
            errorInfo.put("errorStatistics", statistics);

            return errorInfo;

        } catch (Exception e) {
            // Return safe defaults
            Map<String, Object> defaultErrorInfo = new HashMap<>();
            defaultErrorInfo.put("totalCount", 0);
            defaultErrorInfo.put("hasErrors", false);
            defaultErrorInfo.put("hasHighErrorCount", false);
            defaultErrorInfo.put("lastError", "");
            defaultErrorInfo.put("recentErrors", new ArrayList<>());
            defaultErrorInfo.put("formattedErrors", new ArrayList<>());

            Map<String, Object> defaultStats = new HashMap<>();
            defaultStats.put("totalErrors", 0);
            defaultStats.put("criticalErrors", 0);
            defaultStats.put("sqlErrors", 0);
            defaultStats.put("poisonMessages", 0);
            defaultStats.put("generalErrors", 0);
            defaultErrorInfo.put("errorStatistics", defaultStats);

            return defaultErrorInfo;
        }
    }

    /**
     * **HELPER METHOD: Determine error severity**
     */
    private String determineSeverity(String error) {
        String lowerError = error.toLowerCase();

        if (lowerError.contains("critical") || lowerError.contains("fatal")) {
            return "CRITICAL";
        } else if (lowerError.contains("poison")) {
            return "POISON";
        } else if (lowerError.contains("sql")) {
            return "SQL";
        } else {
            return "ERROR";
        }
    }

    /**
     * **HELPER METHOD: Extract timestamp from error message**
     */
    private String extractTimestamp(String error) {
        if (error == null || error.isEmpty()) {
            return "";
        }

        // Match timestamp in square brackets: [YYYY-MM-DD HH:MM:SS]
        if (error.startsWith("[") && error.length() > 21) {
            int closingBracket = error.indexOf(']');
            if (closingBracket == 20) { // Position 20 for [YYYY-MM-DD HH:MM:SS]
                String timestamp = error.substring(1, closingBracket);
                // Validate it's actually a timestamp format
                if (timestamp.matches("\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2}")) {
                    return timestamp;
                }
            }
        }

        // Fallback: try to find timestamp pattern anywhere in the string
        Pattern pattern = Pattern.compile("\\[(\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2})]");
        Matcher matcher = pattern.matcher(error);
        if (matcher.find()) {
            return matcher.group(1);
        }

        // If no timestamp found, return empty string
        return "";
    }

    /**
     * **HELPER METHOD: Truncate message for display**
     */
    private String truncateMessage(String error) {
        if (error == null || error.isEmpty()) {
            return "";
        }

        String message = error;

        // Remove timestamp in square brackets: [YYYY-MM-DD HH:MM:SS]
        if (error.startsWith("[") && error.length() > 21) {
            int closingBracket = error.indexOf(']');
            if (closingBracket > 0 && closingBracket < error.length() - 1) {
                message = error.substring(closingBracket + 1);
            }
        }

        // Trim any leading whitespace
        message = message.trim();

        // Truncate if too long
        if (message.length() <= 100) {
            return message;
        }

        return message.substring(0, 97) + "...";
    }

    /**
     * HELPER METHOD: Extract just the main error message (without exception details)
     * Extracts the primary error message before the exception type
     */
    private String extractMainMessage(String error) {
        String message = truncateMessage(error);

        // If the message contains " - ExceptionType:", extract only the part before it
        int exceptionIndex = message.indexOf(" - ");
        if (exceptionIndex > 0) {
            // Check if what follows looks like an exception (contains "Exception" or "Error")
            String afterDash = message.substring(exceptionIndex + 3);
            if (afterDash.matches(".*(?:Exception|Error)(?::|\\s).*")) {
                return message.substring(0, exceptionIndex);
            }
        }

        return message;
    }

    /**
     * HELPER METHOD: Extract exception information
     * Extracts exception type and message
     */
    private String extractExceptionInfo(String error) {
        String message = truncateMessage(error);

        // Look for pattern " - ExceptionType: message"
        int exceptionIndex = message.indexOf(" - ");
        if (exceptionIndex > 0) {
            String afterDash = message.substring(exceptionIndex + 3);
            if (afterDash.matches(".*(?:Exception|Error)(?::|\\s).*")) {
                return afterDash;
            }
        }

        return "";
    }
}