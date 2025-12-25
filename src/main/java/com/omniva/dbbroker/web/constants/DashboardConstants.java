package com.omniva.dbbroker.web.constants;

public final class DashboardConstants {

    // API Endpoints
    public static final String DASHBOARD_ENDPOINT = "/dashboard";
    public static final String ERROR_ENDPOINT = "/error";
    public static final String API_ERRORS_CLEAR_ENDPOINT = "/api/errors/clear";
    public static final String API_STATUS_ENDPOINT = "/api/status";
    public static final String API_CONTROL_START_ENDPOINT = "/api/control/start";
    public static final String API_CONTROL_STOP_ENDPOINT = "/api/control/stop";
    public static final String API_CONTROL_RESTART_ENDPOINT = "/api/control/restart";
    public static final String API_CONTROL_RESTART_FAILED_ENDPOINT = "/api/control/restart-failed";
    public static final String API_CONTROL_STATUS_ENDPOINT = "/api/control/status";
    public static final String API_LIVE_DATA_ENDPOINT = "/api/dashboard/live-data";

    // Template names
    public static final String DASHBOARD_TEMPLATE = "dbbroker/dashboard";
    public static final String ERROR_TEMPLATE = "dbbroker/error";

    // Response field constants
    public static final String SUCCESS_FIELD = "success";
    public static final String MESSAGE_FIELD = "message";
    public static final String TIMESTAMP_FIELD = "timestamp";
    public static final String ERROR_FIELD = "error";
    public static final String LAST_ERROR_FIELD = "lastError";
    public static final String FAILED_COUNT_FIELD = "failedCount";

    // Status response fields
    public static final String ENGINE_HEALTHY_FIELD = "engineHealthy";
    public static final String OVERALL_STATUS_FIELD = "overallStatus";
    public static final String MESSAGES_PER_SECOND_FIELD = "messagesPerSecond";
    public static final String ERROR_RATE_FIELD = "errorRate";

    // Model attribute constants
    public static final String SERVICE_RUNNING_ATTR = "serviceRunning";
    public static final String HAS_PRESERVED_STATE_ATTR = "hasPreservedState";
    public static final String ACTIVE_LISTENERS_ATTR = "activeListeners";
    public static final String CONFIGURED_LISTENERS_ATTR = "configuredListeners";
    public static final String FAILED_LISTENERS_ATTR = "failedListeners";
    public static final String QUEUE_NAME_ATTR = "queueName";
    public static final String SYSTEM_SUMMARY_ATTR = "systemSummary";
    public static final String UPTIME_ATTR = "uptime";
    public static final String HEALTH_STATUS_ATTR = "healthStatus";
    public static final String PERFORMANCE_ATTR = "performance";
    public static final String LISTENER_DIAGNOSTICS_ATTR = "listenerDiagnostics";
    public static final String RECENT_ERRORS_ATTR = "recentErrors";
    public static final String ERROR_STATISTICS_ATTR = "errorStatistics";
    public static final String TOTAL_ERROR_COUNT_ATTR = "totalErrorCount";
    public static final String HAS_ERRORS_ATTR = "hasErrors";
    public static final String HAS_HIGH_ERROR_COUNT_ATTR = "hasHighErrorCount";
    public static final String REFRESH_TIME_ATTR = "refreshTime";

    // Default values
    public static final String DEFAULT_UPTIME = "00:00:00";
    public static final String UNKNOWN_QUEUE = "Unknown";
    public static final String ERROR_QUEUE = "Error";
    public static final String DASHBOARD_ERROR_SUMMARY = "Dashboard error occurred";
    public static final String NOT_AVAILABLE = "N/A";
    public static final int DEFAULT_ZERO = 0;
    public static final double DEFAULT_ZERO_DOUBLE = 0.0;
    public static final int ERROR_THRESHOLD = 10;
    public static final int RECENT_ERRORS_LIMIT = 20;

    // Status messages
    public static final String SUPERVISOR_ALREADY_RUNNING = "Supervisor is already running";
    public static final String SUPERVISOR_ALREADY_STOPPED = "Supervisor is already stopped";
    public static final String SUPERVISOR_NOT_RUNNING = "Cannot restart listeners - supervisor is not running";
    public static final String CANNOT_RESTART_NO_STATE = "Cannot restart - supervisor is stopped with no preserved state. Use 'Start Engine' instead.";
    public static final String NO_FAILED_LISTENERS = "No failed listeners found - all listeners are healthy";

    // Success messages
    public static final String SUPERVISOR_STARTED_SUCCESS = "Supervisor started successfully (cold start)";
    public static final String SUPERVISOR_STOPPED_SUCCESS = "Supervisor stopped successfully";
    public static final String SUPERVISOR_RESTARTED_SUCCESS = "Supervisor restarted successfully";
    public static final String SUPERVISOR_RESUMED_SUCCESS = "Supervisor resumed successfully from preserved state";
    public static final String ERRORS_CLEARED_SUCCESS = "Recent errors cleared successfully";
    public static final String RESTARTED_FAILED_LISTENERS_FORMAT = "Restarted %d failed listener(s)";

    // Error message prefixes
    public static final String FAILED_TO_START_PREFIX = "Failed to start supervisor: ";
    public static final String FAILED_TO_STOP_PREFIX = "Failed to stop supervisor: ";
    public static final String FAILED_TO_RESTART_PREFIX = "Failed to restart supervisor: ";
    public static final String FAILED_TO_RESTART_LISTENERS_PREFIX = "Failed to restart failed listeners: ";
    public static final String FAILED_TO_CLEAR_ERRORS_PREFIX = "Failed to clear errors: ";
    public static final String FAILED_TO_LOAD_DASHBOARD_PREFIX = "Failed to load dashboard: ";

    // Health status constants
    public static final String HEALTHY_STATUS = "HEALTHY";
    public static final String STOPPED_STATUS = "STOPPED";
    public static final String UNKNOWN_STATUS = "UNKNOWN";

    // Performance grades
    public static final String GRADE_A = "A";
    public static final String GRADE_B = "B";
    public static final String GRADE_C = "C";
    public static final String GRADE_D = "D";
    public static final String GRADE_F = "F";

    // Error type keywords
    public static final String CRITICAL_KEYWORD = "critical";
    public static final String FATAL_KEYWORD = "fatal";
    public static final String POISON_KEYWORD = "poison";
    public static final String SQL_KEYWORD = "sql";

    // Efficiency calculation constants
    public static final double MAX_THROUGHPUT_SCORE = 1.0;
    public static final double MAX_ERROR_RATE_FOR_RELIABILITY = 0.1;
    public static final double ACTIVE_SCORE = 1.0;
    public static final double INACTIVE_SCORE = 0.5;
    public static final double THROUGHPUT_WEIGHT = 0.4;
    public static final double RELIABILITY_WEIGHT = 0.4;
    public static final double ACTIVITY_WEIGHT = 0.2;

    // Grade thresholds
    public static final double GRADE_A_THRESHOLD = 0.9;
    public static final double GRADE_B_THRESHOLD = 0.8;
    public static final double GRADE_C_THRESHOLD = 0.7;
    public static final double GRADE_D_THRESHOLD = 0.6;
    public static final double HIGH_ERROR_RATE_THRESHOLD = 0.1;
    public static final double CRITICAL_ERROR_RATE_THRESHOLD = 0.25;

    // Time constants
    public static final int HOURS_IN_DAY = 24;
    public static final String UPTIME_FORMAT = "%02d:%02d:%02d";

    private DashboardConstants() {
        // Utility class
    }
}