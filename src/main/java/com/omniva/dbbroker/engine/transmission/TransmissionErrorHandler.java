package com.omniva.dbbroker.engine.transmission;

import com.omniva.dbbroker.config.DbBrokerConfig;
import com.omniva.dbbroker.messaging.model.DbBrokerMessage;
import com.omniva.dbbroker.messaging.model.TableChangeEvent;
import com.omniva.dbbroker.engine.DbBrokerEngine;
import com.omniva.dbbroker.engine.fault.DbBrokerRuntimeException;
import com.omniva.dbbroker.engine.fault.DbBrokerShutdownException;
import com.omniva.dbbroker.engine.fault.ErrorTracker;
import com.omniva.dbbroker.engine.fault.PoisonMessageException;
import com.omniva.dbbroker.engine.fuelsystem.DbBrokerConnectionManager;
import com.omniva.dbbroker.engine.valvetrain.MessageRetryState;
import com.omniva.dbbroker.engine.valvetrain.MessageRetryTracker;
import com.omniva.dbbroker.util.DateTimeUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.context.ApplicationContext;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Duration;
import java.time.Instant;
import java.util.UUID;

/**
 * Transmission Error Handler - Comprehensive error handling for all DbBroker components
 * <p>
 * Provides sophisticated error classification and response strategies for:
 * - Listener startup/ignition errors
 * - Connection-level errors
 * - Message processing errors
 * - Transmission errors from DbBrokerService
 * - Data message processing errors
 * - Event routing errors
 * - Startup/initialization errors
 * <p>
 * Error Classification:
 * - Critical JVM Errors (OutOfMemoryError, etc.) → Immediate shutdown
 * - Critical System Errors (NoClassDefFoundError, etc.) → Graceful shutdown
 * - Programming Errors (AssertionError) → Convert to retryable
 * - Poison Messages → Consume and continue
 * - Retryable Errors → Retry with backoff
 * - Unknown Errors → Conservative shutdown
 * <p>
 * Shutdown Strategy:
 * - Uses existing DbBrokerService.stop() and shutdown() methods
 * - Leverages Spring Boot's graceful shutdown capabilities
 * - Maintains proper separation of concerns
 */
public class TransmissionErrorHandler {

    private static final Logger log = LoggerFactory.getLogger(TransmissionErrorHandler.class);

    private final ErrorTracker errorTracker;
    private final MessageRetryTracker messageRetryTracker;
    private final DbBrokerConfig config;
    private final DbBrokerConnectionManager connectionManager;
    private final Transmission transmission;
    private final ApplicationContext applicationContext;

    public TransmissionErrorHandler(ErrorTracker errorTracker,
                                    MessageRetryTracker messageRetryTracker,
                                    DbBrokerConfig config,
                                    DbBrokerConnectionManager connectionManager,
                                    Transmission transmission,
                                    ApplicationContext applicationContext) {
        this.errorTracker = errorTracker;
        this.messageRetryTracker = messageRetryTracker;
        this.config = config;
        this.connectionManager = connectionManager;
        this.transmission = transmission;
        this.applicationContext = applicationContext;
    }
    private DbBrokerEngine getDbBrokerEngine() {
        return applicationContext.getBean(DbBrokerEngine.class);
    }

    // ===== IGNITION PHASE ERRORS (No Connection, No Message) =====

    /**
     * Handle SQL errors during listener startup/ignition phase
     */
    public ProcessingState handleIgnitionSqlError(String listenerName, SQLException error, boolean shutdownRequested) {
        try {
            errorTracker.processSqlError(listenerName, error, 0, shutdownRequested);
            log.error("Ignition SQL error in listener {} - stopping", listenerName);
            return ProcessingState.STOP_CRITICAL;
        } catch (DbBrokerShutdownException shutdownEx) {
            log.info("Listener {} stopped due to shutdown request", listenerName);
            return ProcessingState.STOP_GRACEFUL;
        } catch (InterruptedException ie) {
            log.info("Listener {} interrupted during SQL error recovery", listenerName);
            Thread.currentThread().interrupt();
            return ProcessingState.STOP_GRACEFUL;
        } catch (Error fatalEx) {
            log.error("Fatal ignition SQL error in listener {}: {}", listenerName, fatalEx.getMessage());
            return ProcessingState.STOP_CRITICAL;
        }
    }

    /**
     * Handle processing errors during listener startup/ignition phase
     */
    public ProcessingState handleIgnitionProcessingError(String listenerName, Exception error, boolean shutdownRequested) {
        try {
            errorTracker.processGeneralError(listenerName, error, 0, shutdownRequested);
            log.error("Ignition processing error in listener {} - stopping", listenerName);
            return ProcessingState.STOP_CRITICAL;
        } catch (DbBrokerShutdownException shutdownEx) {
            log.info("Listener {} stopped due to shutdown request", listenerName);
            return ProcessingState.STOP_GRACEFUL;
        } catch (InterruptedException ie) {
            log.info("Listener {} interrupted during processing error", listenerName);
            Thread.currentThread().interrupt();
            return ProcessingState.STOP_GRACEFUL;
        }
    }

    /**
     * Handle critical errors during the listener startup / ignition phase
     */
    public ProcessingState handleIgnitionCriticalError(String listenerName, Throwable error) {
        log.error("CRITICAL ERROR during ignition in listener {} - {}: {}",
                listenerName, error.getClass().getSimpleName(), error.getMessage(), error);

        try {
            errorTracker.processCriticalSystemError(listenerName, error);
        } catch (Error fatalEx) {
            log.error("Critical system error - listener termination required: {}", fatalEx.getMessage());
        }

        return ProcessingState.STOP_CRITICAL;
    }

    // ===== CONNECTION PHASE ERRORS (Connection Available, No Message) =====

    /**
     * Handle interruption during processing (connection available, no specific message)
     */
    public ProcessingState handleInterruption(Connection connection, String listenerName) {
        log.info("Listener {} interrupted", listenerName);
        Thread.currentThread().interrupt();
        connectionManager.safeRollback(connection, listenerName);
        return ProcessingState.STOP_GRACEFUL;
    }

    // ===== MESSAGE PROCESSING ERRORS (Connection + Message Available) =====

    /**
     * Handle SQL errors during message processing
     */
    public ProcessingState handleMessageSqlError(Connection connection, String listenerName,
                                                 SQLException error, DbBrokerMessage message, boolean shutdownRequested) {
        try {
            if (message != null) {
                return handleMessageErrorWithRetry(connection, listenerName, error, message, shutdownRequested, "SQLException");
            } else {
                // No message context - treat as connection-level error
                log.error("SQL error in listener {} with no message context: {}", listenerName, error.getMessage());
                connectionManager.safeRollback(connection, listenerName);
                return ProcessingState.CONTINUE;
            }
        } catch (PoisonMessageException poisonEx) {
            return handlePoisonMessage(connection, poisonEx, listenerName);
        } catch (DbBrokerShutdownException shutdownEx) {
            log.info("Message processing stopped due to shutdown request");
            return ProcessingState.STOP_GRACEFUL;
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            return ProcessingState.STOP_GRACEFUL;
        } catch (Exception ex) {
            log.error("Unhandled exception in listener {}: {}", listenerName, ex.getMessage());
            connectionManager.safeRollback(connection, listenerName);
            return ProcessingState.CONTINUE;
        } catch (Error fatalEx) {
            log.error("Fatal error in listener {}: {}", listenerName, fatalEx.getMessage());
            return ProcessingState.STOP_CRITICAL;
        }
    }

    /**
     * Handle processing errors during message processing
     */
    public ProcessingState handleMessageProcessingError(Connection connection, String listenerName,
                                                        Exception error, DbBrokerMessage message, boolean shutdownRequested) {
        try {
            if (message != null) {
                return handleMessageErrorWithRetry(connection, listenerName, error, message, shutdownRequested, error.getClass().getSimpleName());
            } else {
                // No message context - treat as a connection-level error
                log.error("Processing error in listener {} with no message context: {}", listenerName, error.getMessage());
                connectionManager.safeRollback(connection, listenerName);
                return ProcessingState.CONTINUE;
            }
        } catch (PoisonMessageException poisonEx) {
            return handlePoisonMessage(connection, poisonEx, listenerName);
        } catch (DbBrokerShutdownException shutdownEx) {
            log.info("Message processing stopped due to shutdown request");
            return ProcessingState.STOP_GRACEFUL;
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            return ProcessingState.STOP_GRACEFUL;
        } catch (Exception ex) {
            log.error("Unhandled exception in listener {}: {}", listenerName, ex.getMessage());
            connectionManager.safeRollback(connection, listenerName);
            return ProcessingState.CONTINUE;
        } catch (Error fatalEx) {
            log.error("Fatal error in listener {}: {}", listenerName, fatalEx.getMessage());
            return ProcessingState.STOP_CRITICAL;
        }
    }

    /**
     * Handle critical errors during message processing
     */
    public ProcessingState handleMessageCriticalError(Connection connection, String listenerName,
                                                      Throwable error, DbBrokerMessage message) {
        log.error("CRITICAL ERROR in listener {} - {}: {}",
                listenerName, error.getClass().getSimpleName(), error.getMessage(), error);

        connectionManager.safeRollback(connection, listenerName);

        if (message != null) {
            handleCriticalErrorForMessage(connection, message, error, listenerName);
        }

        try {
            errorTracker.processCriticalSystemError(listenerName, error);
        } catch (Error fatalEx) {
            log.error("Critical system error confirmed - listener termination required: {}", fatalEx.getMessage());
        }

        return ProcessingState.STOP_CRITICAL;
    }

    // ===== DBBROKERSERVICE ERROR HANDLING =====

    /**
     * Handle startup/initialization errors from DbBrokerService
     *
     * @param reason Human-readable reason for the startup failure
     * @param cause The exception that caused the startup failure
     */
    public void handleStartupError(String reason, Throwable cause) {
        log.error("STARTUP ERROR: {} - {}", reason, cause.getMessage(), cause);
        errorTracker.addError("STARTUP: " + reason, cause);

        // Classify error and determine shutdown strategy
        if (isCriticalJvmError(cause)) {
            shutdownImmediately("Startup JVM Error: " + cause.getClass().getSimpleName());
        } else if (isCriticalSystemError(cause)) {
            shutdownGracefully("Startup System Error: " + cause.getClass().getSimpleName(), 3);
        } else {
            shutdownGracefully("Startup Error: " + reason, 1);
        }
    }

    /**
     * Handle errors that occur during message transmission in DbBrokerService.transmit()
     * Provides sophisticated error classification and appropriate response strategies
     *
     * @param message The message being processed when error occurred
     * @param error The error that occurred during transmission
     * @throws PoisonMessageException if message should be consumed/discarded
     * @throws DbBrokerRuntimeException if error is transient and should be retried
     * @throws Error if error is fatal and requires application shutdown
     */
    public void handleTransmissionError(DbBrokerMessage message, Throwable error) {
        String conversationHandle = message.getConversationHandle();
        String context = "transmission for conversation: " + conversationHandle;

        // Use common classification logic
        classifyAndHandleError(message, error, context);
    }

    /**
     * Handle errors during data message processing in DbBrokerService.handleDataMessage()
     *
     * @param message The message being processed
     * @param error The error that occurred
     * @throws PoisonMessageException if message should be consumed
     * @throws DbBrokerRuntimeException if error should be retried
     * @throws Error if error is fatal
     */
    public void handleDataMessageError(DbBrokerMessage message, Throwable error) {
        String conversationHandle = message.getConversationHandle();
        String context = "data message processing for conversation: " + conversationHandle;

        // Use common classification logic
        classifyAndHandleError(message, error, context);
    }

    /**
     * Handle errors during event routing to listeners in DbBrokerService.routeEventToListener()
     *
     * @param event The table change event being routed
     * @param error The error that occurred
     * @throws PoisonMessageException if event should be discarded
     * @throws DbBrokerRuntimeException if error should be retried
     * @throws Error if error is fatal
     */
    public void handleEventRoutingError(TableChangeEvent event, Throwable error) {
        String eventId = event.getEventId();
        String context = "event routing for event: " + eventId;

        // Create a pseudo-message for error handling consistency
        DbBrokerMessage pseudoMessage = createPseudoMessage(event);
        classifyAndHandleError(pseudoMessage, error, context);
    }

    // ===== COMMON ERROR CLASSIFICATION AND HANDLING =====

    /**
     * Common error classification and handling logic used by all DbBrokerService methods
     *
     * @param message The message context (real or pseudo)
     * @param error The error to classify
     * @param context Human-readable context for logging
     */
    private void classifyAndHandleError(DbBrokerMessage message, Throwable error, String context) {

        // error classification
        if (error instanceof PoisonMessageException) {
            handleContextualPoisonMessageError((PoisonMessageException) error, context);

        } else if (error instanceof DbBrokerRuntimeException) {
            handleContextualRetryableError((DbBrokerRuntimeException) error, context);

        } else if (error instanceof Exception) {
            handleContextualUnexpectedException(message, (Exception) error, context);

        } else if (isCriticalJvmError(error)) {
            handleContextualCriticalJvmError(error, context);

        } else if (isCriticalSystemError(error)) {
            handleContextualCriticalSystemError(error, context);

        } else if (error instanceof AssertionError) {
            handleContextualAssertionError(message, (AssertionError) error, context);

        } else {
            handleContextualUnknownError(error, context);
        }
    }

    // ===== ERROR CLASSIFICATION METHODS =====

    private boolean isCriticalJvmError(Throwable error) {
        return error instanceof VirtualMachineError;
    }

    private boolean isCriticalSystemError(Throwable error) {
        return error instanceof LinkageError;
    }

    // ===== CONTEXTUAL ERROR HANDLERS =====

    private void handleContextualPoisonMessageError(PoisonMessageException error, String context) {
        log.warn("Poison message detected during {} - {}", context, error.getMessage());
        errorTracker.addError("Poison message during " + context + ": " + error.getMessage(), error);

        // Re-throw so DbBrokerListener can consume the message
        throw error;
    }

    private void handleContextualRetryableError(DbBrokerRuntimeException error, String context) {
        log.warn("Retryable error during {} - {}", context, error.getMessage());
        errorTracker.addError("Retryable error during " + context + ": " + error.getMessage(), error);

        // Re-throw so DbBrokerListener can retry
        throw error;
    }

    private void handleContextualUnexpectedException(DbBrokerMessage message, Exception error, String context) {
        String conversationHandle = message.getConversationHandle();
        log.error("Unexpected exception during {} - {}", context, error.getMessage(), error);
        errorTracker.addError("Unexpected exception during " + context + ": " + error.getMessage(), error);

        // Convert to retryable error
        throw new DbBrokerRuntimeException(
                "Unexpected error during " + context + ": " + conversationHandle, error);
    }

    private void handleContextualCriticalJvmError(Throwable error, String context) {
        log.error("CRITICAL JVM ERROR during {} - Application must shutdown immediately: {}",
                context, error.getMessage(), error);
        errorTracker.addError("CRITICAL JVM ERROR during " + context + ": " + error.getClass().getSimpleName(), error);

        // Trigger immediate shutdown
        shutdownImmediately("JVM Error during " + context + ": " + error.getClass().getSimpleName());
        throw (Error) error; // Re-throw to ensure thread dies
    }

    private void handleContextualCriticalSystemError(Throwable error, String context) {
        log.error("CRITICAL SYSTEM ERROR during {} - Application must shutdown: {}",
                context, error.getMessage(), error);
        errorTracker.addError("CRITICAL SYSTEM ERROR during " + context + ": " + error.getClass().getSimpleName(), error);

        // Trigger graceful shutdown
        shutdownGracefully("System Error during " + context + ": " + error.getClass().getSimpleName(), 3);
        throw (Error) error; // Re-throw to ensure thread dies
    }

    private void handleContextualAssertionError(DbBrokerMessage message, AssertionError error, String context) {
        String conversationHandle = message.getConversationHandle();
        log.error("ASSERTION FAILED during {} - Programming bug detected: {}",
                context, error.getMessage(), error);
        errorTracker.addError("ASSERTION ERROR during " + context + ": " + error.getMessage(), error);

        // Convert to retryable error (don't shut down for programming bugs in production)
        throw new DbBrokerRuntimeException(
                "Assertion failed during " + context + ": " + conversationHandle, error);
    }

    private void handleContextualUnknownError(Throwable error, String context) {
        log.error("UNKNOWN ERROR TYPE during {} - Conservative shutdown: {} - {}",
                context, error.getClass().getSimpleName(), error.getMessage(), error);
        errorTracker.addError("UNKNOWN ERROR during " + context + ": " + error.getClass().getSimpleName(), error);

        // Conservative approach - shut down for unknown error types
        shutdownGracefully("Unknown Error during " + context + ": " + error.getClass().getSimpleName(), 4);
        throw (Error) error;
    }

    // ===== POISON MESSAGE HANDLING =====

    /**
     * Handle poison message consumption
     */
    private ProcessingState handlePoisonMessage(Connection connection, PoisonMessageException e, String listenerName) {
        try {
            MessageRetryState tracker = messageRetryTracker.getOrCreateRetryState(e.getConversationHandle());
            int totalAttempts = tracker.get();
            String errorType = tracker.getErrorType();
            Instant firstFailure = tracker.getFirstFailure();

            Duration retryDuration = firstFailure != null ?
                    Duration.between(firstFailure, Instant.now()) : Duration.ZERO;

            errorTracker.logPoisonMessageConsumption(e, listenerName, totalAttempts);

            log.error("Listener {} consuming poison message: {} (conversation: {}) - " +
                            "failed after {} attempts over {} - error type: {} - original error: {}",
                    listenerName, e.getMessageType(), e.getConversationHandle(),
                    totalAttempts, DateTimeUtils.formatDuration(retryDuration), errorType,
                    e.getOriginalError().getMessage());

            transmission.endConversation(connection, e.getConversationHandle(),
                    "Poison message consumed after " + totalAttempts + " consecutive errors - " +
                            e.getOriginalError().getClass().getSimpleName() + ": " + e.getOriginalError().getMessage(),
                    listenerName);

            connection.commit();
            messageRetryTracker.clearRetryState(e.getConversationHandle());
            log.info("Poison message consumed successfully - conversation {} ended", e.getConversationHandle());

            return ProcessingState.CONTINUE;

        } catch (SQLException sqlEx) {
            log.error("SQL error while consuming poison message {}: {}", e.getConversationHandle(), sqlEx.getMessage());
            connectionManager.safeRollback(connection, listenerName);
            return ProcessingState.STOP_ERROR;
        } catch (Exception consumeError) {
            log.error("Failed to consume poison message {}: {}", e.getConversationHandle(), consumeError.getMessage());
            connectionManager.safeRollback(connection, listenerName);
            return ProcessingState.STOP_ERROR;
        }
    }

    /**
     * Handle critical errors for specific messages
     */
    private void handleCriticalErrorForMessage(Connection connection, DbBrokerMessage messageContext,
                                               Throwable t, String listenerName) {
        String conversationHandle = messageContext.getConversationHandle();
        MessageRetryState tracker = messageRetryTracker.getOrCreateRetryState(conversationHandle);

        int currentAttempts = tracker.get();
        if (currentAttempts >= config.getMaxRetries()) {
            log.error("Message {} already exceeded max retries - consuming as poison after critical error", conversationHandle);

            try {
                transmission.endConversation(connection, conversationHandle,
                        "Poison message after " + currentAttempts + " attempts, final critical error: " +
                                t.getClass().getSimpleName() + ": " + t.getMessage(),
                        listenerName);

                connection.commit();
                messageRetryTracker.clearRetryState(conversationHandle);
                log.info("Poison message consumed after critical errors - conversation {} ended", conversationHandle);
            } catch (Exception consumeError) {
                log.error("Failed to consume poison message after critical error: {}", consumeError.getMessage());
                connectionManager.safeRollback(connection, listenerName);
            }
            return;
        }

        currentAttempts = tracker.incrementAndGet();
        errorTracker.processCriticalSystemError(listenerName, t);
        tracker.setErrorType(t.getClass().getSimpleName());

        log.error("Critical error for message {} in listener {} - attempt {}/{}",
                conversationHandle, listenerName, currentAttempts, config.getMaxRetries());

        try {
            errorTracker.applyRetryDelay(currentAttempts);
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            log.warn("Retry delay interrupted for critical error in listener {}", listenerName);
        }

        connectionManager.safeRollback(connection, listenerName);
        log.info("Message {} returned to queue - will be retried by new listener (attempt {}/{})",
                conversationHandle, currentAttempts, config.getMaxRetries());
    }

    // ===== RETRY LOGIC =====

    /**
     * Common retry logic for message-specific errors
     */
    private ProcessingState handleMessageErrorWithRetry(Connection connection, String listenerName,
                                                        Exception error, DbBrokerMessage message,
                                                        boolean shutdownRequested, String errorType) throws Exception {
        String conversationHandle = message.getConversationHandle();
        MessageRetryState tracker = messageRetryTracker.getOrCreateRetryState(conversationHandle);

        int currentAttempts = tracker.get();
        if (currentAttempts >= config.getMaxRetries()) {
            throw new PoisonMessageException(conversationHandle, message.getMessageTypeName(), error);
        }

        currentAttempts = tracker.incrementAndGet();
        tracker.setErrorType(errorType);

        if (error instanceof SQLException sqlError) {
            errorTracker.processSqlError(listenerName, sqlError, currentAttempts, shutdownRequested);
            log.warn("Listener {} SQL error for message {} - attempt {}/{}: {} (SQLState: {}, ErrorCode: {})",
                    listenerName, conversationHandle, currentAttempts,
                    config.getMaxRetries(), error.getMessage(),
                    sqlError.getSQLState(), sqlError.getErrorCode());
        } else {
            errorTracker.processGeneralError(listenerName, error, currentAttempts, shutdownRequested);
            log.warn("Listener {} processing error for message {} - attempt {}/{}: {}",
                    listenerName, conversationHandle, currentAttempts,
                    config.getMaxRetries(), error.getMessage());
        }

        connectionManager.safeRollback(connection, listenerName);
        return ProcessingState.CONTINUE;
    }

    // ===== SHUTDOWN METHODS (Using Existing DbBrokerEngine Methods) =====

    /**
     * Trigger immediate shutdown using existing service methods
     * For critical JVM errors that require immediate termination
     */
    private void shutdownImmediately(String reason) {
        log.error("CRITICAL ERROR - IMMEDIATE SHUTDOWN: {} - Exit code: {}", reason, 2);
        DbBrokerEngine dbBrokerEngine = getDbBrokerEngine();
        try {
            if (dbBrokerEngine.isRunning()) {
                dbBrokerEngine.stop();
            }

            // Brief pause for logging, then force exit
            Thread.sleep(100);
            System.exit(2);

        } catch (InterruptedException shutdownError) {
            log.error("Error during immediate shutdown, forcing exit: {}", shutdownError.getMessage());
            System.exit(2);
        }
    }

    /**
     * Trigger graceful shutdown using existing service methods
     * For critical system errors that allow brief cleanup time
     */
    private void shutdownGracefully(String reason, int exitCode) {
        log.error("CRITICAL ERROR - GRACEFUL SHUTDOWN: {} - Exit code: {}", reason, exitCode);

        DbBrokerEngine dbBrokerEngine = getDbBrokerEngine();

        try {
            if (dbBrokerEngine.isRunning()) {
                dbBrokerEngine.stop();
            }

            // Give time for current operations to complete
            Thread.sleep(1000);

            // Trigger Spring Boot graceful shutdown
            SpringApplication.exit(applicationContext, () -> {
                log.error("Application exiting due to critical DbBroker error: {}", reason);
                return exitCode;
            });

            // Safety timeout - if graceful shutdown takes too long, force exit
            Thread.sleep(5000);
            log.error("Graceful shutdown timeout - forcing exit");
            System.exit(exitCode);

        } catch (InterruptedException shutdownError) {
            log.error("Error during graceful shutdown, forcing exit: {}", shutdownError.getMessage());
            System.exit(exitCode);
        }
    }

    /**
     * Create a pseudo-message for error handling when we only have event context
     * This allows consistent error handling even when we don't have a real message
     */
    private DbBrokerMessage createPseudoMessage(TableChangeEvent event) {
        return DbBrokerMessage.builder()
                .conversationHandle("EVENT-" + event.getEventId())
                .messageTypeName("TableChangeEvent")
                .messageBody("Table change event for " + event.getTableName())
                .messageSequenceNumber(0L)
                .messagePriority(1)
                .serviceName("TableChangeService")
                .serviceContractName("TableChangeContract")
                .conversationGroupId(UUID.randomUUID().toString())
                .messageEnqueueTime(Timestamp.from(Instant.now()))
                .receivedAt(Instant.now())
                .threadId((int) Thread.currentThread().threadId())
                .build();
    }

    /**
     * Processing state enumeration for listener flow control
     */
    public enum ProcessingState {
        /**
         * Continue listener processing normally
         */
        CONTINUE,

        /**
         * Graceful listener shutdown (planned stop)
         */
        STOP_GRACEFUL,

        /**
         * Stop due to a recoverable error (can restart)
         */
        STOP_ERROR,

        /**
         * Critical listener failure (requires immediate stop)
         */
        STOP_CRITICAL
    }
}