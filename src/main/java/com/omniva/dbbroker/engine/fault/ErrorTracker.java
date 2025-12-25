package com.omniva.dbbroker.engine.fault;

import com.omniva.dbbroker.config.DbBrokerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * Tracks and manages error history for DbBroker monitoring
 * Thread-safe implementation for concurrent access
 */
public class ErrorTracker {

    private static final Logger log = LoggerFactory.getLogger(ErrorTracker.class);
    private static final int MAX_RECENT_ERRORS = 100;
    private static final DateTimeFormatter TIMESTAMP_FORMAT = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    // Thread-safe list for concurrent access from multiple listener threads
    private final List<String> recentErrors = new CopyOnWriteArrayList<>();

    // Configuration for error handling
    private final DbBrokerConfig config;

    public ErrorTracker(DbBrokerConfig config) {
        this.config = config;
    }

    /**
     * Add an error with timestamp
     */
    public void addError(String errorMessage) {
        addError(errorMessage, null);
    }

    /**
     * Add an error with exception details
     */
    public void addError(String errorMessage, Throwable throwable) {
        String timestamp = LocalDateTime.now().format(TIMESTAMP_FORMAT);
        String formattedError;

        if (throwable != null) {
            formattedError = String.format("[%s] %s - %s: %s",
                    timestamp, errorMessage, throwable.getClass().getSimpleName(), throwable.getMessage());
        } else {
            formattedError = String.format("[%s] %s", timestamp, errorMessage);
        }

        recentErrors.add(formattedError);

        // Keep only the most recent errors
        while (recentErrors.size() > MAX_RECENT_ERRORS) {
            recentErrors.removeFirst();
        }
    }

    /**
     * Process SQL error - track error and check for fatal conditions
     *
     * @param threadName        the listener thread name
     * @param e                 the SQL exception
     * @param consecutiveErrors current consecutive error count (for logging only)
     * @param shutdownRequested whether shutdown was requested
     * @throws DbBrokerFatalError     when a fatal SQL error occurs that requires immediate stop
     * @throws DbBrokerShutdownException when shutdown is requested during error handling
     * @throws InterruptedException    when the thread is interrupted during a retry delay
     */
    public void processSqlError(String threadName, SQLException e,
                                int consecutiveErrors, boolean shutdownRequested)
            throws DbBrokerFatalError, DbBrokerShutdownException, InterruptedException {

        // Check for the shutdown request first
        if (shutdownRequested) {
            log.info("Listener {} stopped due to shutdown request", threadName);
            throw new DbBrokerShutdownException("Shutdown requested during SQL error handling");
        }

        // Track the error for monitoring
        String errorMsg = String.format("SQL error in listener %s (SQLState: %s, ErrorCode: %d)",
                threadName, e.getSQLState(), e.getErrorCode());
        addError(errorMsg, e);

        log.error("SQL error in listener {}: {} (SQLState: {}, ErrorCode: {}) - consecutive error {}/{}",
                threadName, e.getMessage(), e.getSQLState(), e.getErrorCode(),
                consecutiveErrors, config.getMaxRetries());

        // Check for fatal errors first
        if (isFatalSqlError(e)) {
            log.error("Fatal SQL error in listener {}, stopping thread", threadName);
            throw new DbBrokerFatalError("Fatal SQL error: " + e.getMessage(), e);
        }

        // Apply retry delay for retryable errors
        applyRetryDelay(consecutiveErrors);
    }

    /**
     * Process general processing error - track error and apply retry delay
     *
     * @param threadName        the listener thread name
     * @param e                 the processing exception
     * @param consecutiveErrors current consecutive error count (for logging only)
     * @param shutdownRequested whether shutdown was requested
     * @throws DbBrokerShutdownException when shutdown is requested during error handling
     * @throws InterruptedException      when the thread is interrupted during a retry delay
     */
    public void processGeneralError(String threadName, Exception e, int consecutiveErrors,
                                    boolean shutdownRequested)
            throws DbBrokerShutdownException, InterruptedException {

        if (shutdownRequested) {
            log.info("Listener {} stopped due to shutdown request", threadName);
            throw new DbBrokerShutdownException("Shutdown requested during processing error handling");
        }

        // Track the error for monitoring
        String errorMsg = String.format("Processing error in listener %s", threadName);
        addError(errorMsg, e);

        log.error("Processing error in listener {}: {} - consecutive error {}/{}",
                threadName, e.getMessage(), consecutiveErrors, config.getMaxRetries());

        // Apply retry delay
        applyRetryDelay(consecutiveErrors);
    }

    /**
     * Handle critical system errors that should immediately stop the listener
     *
     * @param threadName the listener thread name
     * @param t          the critical throwable
     * @throws DbBrokerFatalError always - these errors are not recoverable
     */
    public void processCriticalSystemError(String threadName, Throwable t) throws DbBrokerFatalError {
        // Log the critical error
        log.error("CRITICAL SYSTEM ERROR in listener {} - {}: {} - immediate stop required",
                threadName, t.getClass().getSimpleName(), t.getMessage(), t);

        // Track for monitoring (separate from consecutive error counting)
        try {
            String errorMsg = String.format("CRITICAL SYSTEM ERROR in listener %s - immediate stop", threadName);
            addError(errorMsg, t);
        } catch (Error trackingError) {
            // Don't let error tracking failure prevent proper handling
            log.error("Failed to track critical error: {}", trackingError.getMessage());
            throw trackingError;
        }

        throw new DbBrokerFatalError(
                String.format("Critical system error in listener %s: %s", threadName, t.getMessage()), t);
    }

    /**
     * Apply retry delay with exponential backoff
     * Public method for use by listeners when they need retry delays
     */
    public void applyRetryDelay(int consecutiveErrors) throws InterruptedException {
        long baseDelay = config.getBaseRetryDelayMs();
        long delay = baseDelay;

        if (config.isUseExponentialBackoff()) {
            int backoffCount = Math.min(consecutiveErrors, 10); // Cap at 10 to prevent overflow
            delay = Math.min(baseDelay * (1L << backoffCount), config.getMaxRetryDelayMs());
        }

        Thread.sleep(delay); // Let InterruptedException bubble up naturally
    }

    /**
     * Log poison message consumption details
     */
    public void logPoisonMessageConsumption(PoisonMessageException e, String threadName, int consecutiveErrorsBeforeReset) {
        try {
            log.error("=== POISON MESSAGE CONSUMED ===");
            log.error("Thread: {}", threadName);
            log.error("Message Type: {}", e.getMessageType());
            log.error("Conversation: {}", e.getConversationHandle());
            log.error("Original Error: {}", e.getOriginalError().getMessage());
            log.error("Consecutive Errors Before Reset: {}", consecutiveErrorsBeforeReset);
            log.error("===============================");

            // Track for monitoring
            addError("Poison message consumed", e);

        } catch (Exception logError) {
            log.error("Failed to log poison message details: {}", logError.getMessage());
        }
    }

    /**
     * Detect fatal SQL errors that should stop the listener
     */
    private boolean isFatalSqlError(SQLException e) {
        int errorCode = e.getErrorCode();
        String sqlState = e.getSQLState();

        // === SERVICE BROKER SPECIFIC ERRORS ===

        // Service Broker Configuration Errors (Fatal - require admin intervention)
        if (errorCode == 9617 ||   // Service Broker is disabled in this database
                errorCode == 9618 ||   // Service Broker in database is not enabled
                errorCode == 9619 ||   // Cannot route the message because routing is not enabled
                errorCode == 9621 ||   // Service Broker message delivery is disabled
                errorCode == 9632 ||   // Service Broker dialog security is not available
                errorCode == 9633) {   // Service Broker dialog security header is not valid
            return true;
        }

        // Queue/Service Configuration Errors (Fatal - configuration issues)
        if (errorCode == 208 ||    // Invalid object name (queue doesn't exist)
                errorCode == 15581 ||  // Please create a master key in the database
                errorCode == 15597 ||  // Service does not exist
                errorCode == 15598 ||  // Queue does not exist
                errorCode == 15599) {  // Message type does not exist
            return true;
        }

        // === CONNECTION & AUTHENTICATION ===

        // Authentication Errors (Fatal - cannot connect to process messages)
        if (errorCode == 18456 ||  // Login failed for a user
                errorCode == 18470 ||  // Login failed (password expired)
                errorCode == 18487) {  // Login failed (account locked)
            return true;
        }

        // Permission Errors (Fatal - cannot access Service Broker objects)
        if (errorCode == 229 ||    // Permission denied
                errorCode == 15404 ||  // Could not obtain information about Windows NT group/user
                errorCode == 15247) {  // User does not have permission to perform this action
            return true;
        }

        // === DATABASE AVAILABILITY ===

        // Database State Errors (Fatal - database not available for Service Broker)
        if (errorCode == 911 ||    // Database does not exist
                errorCode == 924 ||    // Database is already exclusively locked
                errorCode == 927 ||    // Database is in restricted user mode
                errorCode == 942) {    // Database is being recovered
            return true;
        }

        // === NETWORK/CONNECTION ===

        // Connection Errors (Fatal - cannot maintain connection for message processing)
        if (errorCode == 2 ||      // Cannot open database/Named Pipes Provider error
                errorCode == 53 ||     // Named Pipes Provider: Could not open connection
                errorCode == 233 ||    // Connection established but then error occurred
                errorCode == 10060 ||  // Network timeout
                errorCode == 10061) {  // Connection refused
            return true;
        }

        // Check SQL State for connection/auth issues
        if (sqlState != null) {
            // Invalid Authorization
            return sqlState.startsWith("08") ||  // Connection Exception
                    sqlState.startsWith("28");
        }

        return false;
    }

    // ========================================
    // MONITORING AND UTILITY METHODS
    // ========================================

    /**
     * Get all recent errors
     */
    public List<String> getRecentErrors() {
        return new ArrayList<>(recentErrors);
    }

    /**
     * Get recent errors with limit
     */
    public List<String> getRecentErrors(int limit) {
        List<String> allErrors = getRecentErrors();
        int size = allErrors.size();
        int fromIndex = Math.max(0, size - limit);
        return allErrors.subList(fromIndex, size);
    }

    /**
     * Get count of recent errors
     */
    public int getRecentErrorCount() {
        return recentErrors.size();
    }

    /**
     * Clear all recent errors (for administrative purposes)
     */
    public void clearRecentErrors() {
        recentErrors.clear();
        log.info("Recent errors cleared by administrator");
    }

    /**
     * Check if the error count exceeds a threshold
     */
    public boolean hasHighErrorCount(int threshold) {
        return recentErrors.size() > threshold;
    }

    /**
     * Get the most recent error
     */
    public String getLastError() {
        return recentErrors.isEmpty() ? null : recentErrors.getLast();
    }


    /**
     * Check if there are any recent errors
     */
    public boolean hasRecentErrors() {
        return !recentErrors.isEmpty();
    }
}