package com.omniva.dbbroker.engine;
import com.omniva.dbbroker.config.DbBrokerConfig;
import com.omniva.dbbroker.messaging.listener.TableListenerRegistry;
import com.omniva.dbbroker.messaging.listener.TableListenerRegistryRecord;
import com.omniva.dbbroker.messaging.model.DbBrokerMessage;
import com.omniva.dbbroker.messaging.model.DbBrokerMessageParser;
import com.omniva.dbbroker.messaging.model.TableChangeEvent;
import com.omniva.dbbroker.engine.fault.*;
import com.omniva.dbbroker.engine.crankshaft.DbBrokerSupervisor;
import com.omniva.dbbroker.engine.ignition.EnvironmentValidator;
import com.omniva.dbbroker.engine.transmission.TransmissionErrorHandler;
import com.omniva.dbbroker.engine.transmission.Transmitter;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import lombok.RequiredArgsConstructor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.SmartLifecycle;
import org.springframework.lang.NonNull;

import java.util.Arrays;

/**
 * Database Broker Engine - Main message processing coordinator
 * <p>
 * Simplified architecture with centralized error handling:
 * DbBrokerEngine (message routing and processing coordinator)
 * ├── transmit() (main entry point - implements Transmitter interface)
 * ├── handleSystemMessage() (EndDialog cleanup)
 * └── handleDataMessage() (business logic processing)
 * <p>
 * All error handling delegated to TransmissionErrorHandler for:
 * - Error classification
 * - Appropriate shutdown strategies
 * - Consistent error tracking and logging
 * <p>
 * TableListenerRegistry (business logic routing)
 * └── Custom table listeners (onInsert, onUpdate, onDelete)
 * <p>
 * DbBrokerSupervisor (manages multiple listener threads)
 * ├── DbBrokerListener-1 through DbBrokerListener-N
 * └── Each listener processes Service Broker messages via transmit()
 */
@RequiredArgsConstructor
public class DbBrokerEngine implements SmartLifecycle, Transmitter {

    private static final Logger log = LoggerFactory.getLogger(DbBrokerEngine.class);

    private final DbBrokerConfig config;
    private final DbBrokerSupervisor supervisor;
    private final DbBrokerMessageParser messageParser;
    private final TableListenerRegistry listenerRegistry;
    private final ErrorTracker errorTracker;
    private final EnvironmentValidator environmentValidator;
    private final TransmissionErrorHandler transmissionErrorHandler;

    // SPRING LIFECYCLE: Track running state (Spring manages the rest)
    private volatile boolean running = false;

    // ===== UTILITY METHODS =====

    /**
     * Get formatted thread information for logging purposes
     */
    private String getThreadInfo() {
        Thread thread = Thread.currentThread();
        return String.format("Thread [%s:%d]", thread.getName(), thread.threadId());
    }

    /**
     * Consolidated validation logic for data message processing
     */
    private boolean isValidForProcessing(TableListenerRegistryRecord registration,
                                         TableChangeEvent event) {
        if (registration == null) {
            log.warn("No listener registered for table: {}", event.getTableName());
            return false;
        }

        if (!registration.isEnabled()) {
            log.warn("Listener for table {} is disabled", event.getTableName());
            return false;
        }

        if (!registration.supportsEvent(event.getChangeType().name())) {
            log.warn("Listener for table {} doesn't support {} events",
                    event.getTableName(), event.getChangeType());
            return false;
        }

        return true;
    }

    // ===== SPRING LIFECYCLE METHODS =====

    /**
     * Initialize the Database Broker Service and log table listener configuration
     */
    @PostConstruct
    public void initialize() {
        log.info("Starting Database Broker Service");

        try {
            int registeredCount = listenerRegistry.getListenerCount();

            if (registeredCount == 0) {
                log.warn("No table listeners registered");
                return;
            }

            log.info("=== TABLE LISTENER CONFIGURATION ===");
            listenerRegistry.getAllListeners().forEach((table, registration) ->
                    log.info("Table: {} | Listener: {} | Events: {} | RecordType: {} | Enabled: {}",
                            table,
                            registration.beanName(),
                            Arrays.toString(registration.config().events()),
                            registration.config().recordType().getSimpleName(),
                            registration.isEnabled()));
            log.info("====================================");

        } catch (Exception e) {
            transmissionErrorHandler.handleStartupError("Failed to initialize Database Broker Service", e);
        }
    }

    /**
     * Start the Database Broker Service message processing
     */
    @Override
    public void start() {
        if (running) {
            log.warn("DbBrokerService is already running");
            return;
        }

        if (!environmentValidator.validateEnvironment()) {
            log.error("DbBrokerService environment validation failed - not starting message processing");
            return;
        }

        try {
            if (listenerRegistry.getListenerCount() == 0) {
                log.warn("No table listeners registered - not starting message processing");
                return;
            }

            String queueName = config.getQueueName();
            supervisor.startSupervision(queueName, this);
            running = true;

            log.info("DbBrokerService started processing messages from queue: {} with {} listener threads",
                    queueName, supervisor.getConfiguredListenerCount());

        } catch (Exception e) {
            transmissionErrorHandler.handleStartupError("Failed to start message processing", e);
        }
    }

    /**
     * Stop the Database Broker Service message processing
     */
    @Override
    public void stop() {
        if (!running) {
            log.info("DbBrokerService is not running");
            return;
        }

        try {
            log.info("Stopping DbBrokerService listening...");
            supervisor.stopSupervision();
            running = false;
            log.info("DbBrokerService stopped listening");
        } catch (Exception e) {
            errorTracker.addError("Error stopping DbBrokerService: " + e.getMessage(), e);
            log.error("Error stopping DbBrokerService: {}", e.getMessage(), e);
            throw new DbBrokerShutdownException("Failed to stop DB Broker listening", e);
        }
    }

    /**
     * Check if the Database Broker Service is currently running
     */
    @Override
    public boolean isRunning() {
        return running && supervisor.isSupervising();
    }

    /**
     * Get the startup phase for Spring lifecycle management
     */
    @Override
    public int getPhase() {
        return 1000;
    }

    /**
     * Indicates this service should start automatically
     */
    @Override
    public boolean isAutoStartup() {
        return true;
    }

    /**
     * Stop the service with callback notification
     */
    @Override
    public void stop(@NonNull Runnable callback) {
        try {
            stop();
        } finally {
            callback.run();
        }
    }

    /**
     * Shutdown hook called by Spring before bean destruction
     */
    @PreDestroy
    public void shutdown() {
        log.info("Shutting down Database Broker Service");

        try {
            if (running) {
                stop();
            }
            log.info("Database Broker Service shutdown complete");
        } catch (Exception e) {
            errorTracker.addError("Error during Database Broker Service shutdown: " + e.getMessage(), e);
            log.error("Error during Database Broker Service shutdown: {}", e.getMessage(), e);
        }
    }

    // ===== MESSAGE PROCESSING METHODS =====

    /**
     * Transmit message through the drivetrain (business logic processing)
     * This is the main entry point for message processing from DbBrokerListener
     */
    @Override
    public void transmit(DbBrokerMessage message) {
        String threadInfo = getThreadInfo();

        try {

            if (message.isSystemMessage()) {
                handleSystemMessage(message);
            } else if (message.hasDataContent()) {
                handleDataMessage(message);
            } else {
                log.warn("{} received message with no data content: {}",
                        threadInfo, message.getMessageTypeName());
            }
        } catch (Throwable t) {
            transmissionErrorHandler.handleTransmissionError(message, t);
        }
    }

    /**
     * Handle system messages from Service Broker
     * <p>
     * System messages include:
     * - EndDialog: Normal conversation termination
     * - Error: Error condition in Service Broker
     * - DialogTimer: Conversation timeout
     * <p>
     * All system messages result in conversation cleanup and are consumed
     * without further processing.
     */
    private void handleSystemMessage(DbBrokerMessage message) {
        String threadInfo = getThreadInfo();
        String messageType = message.getMessageTypeName();
        String conversationHandle = message.getConversationHandle();

        log.info("{} handling system message: {} for conversation: {}",
                threadInfo, messageType, conversationHandle);

        // All system messages result in ending the conversation with appropriate reason
        String reason = switch (messageType) {
            case "http://schemas.microsoft.com/SQL/ServiceBroker/EndDialog" -> "EndDialog cleanup";
            case "http://schemas.microsoft.com/SQL/ServiceBroker/Error" -> "Error message cleanup";
            case "http://schemas.microsoft.com/SQL/ServiceBroker/DialogTimer" -> "Timer expiry cleanup";
            default -> "Unknown system message cleanup: " + messageType;
        };

        log.info("{} consumed system message: {} for conversation: {} (reason: {})",
                threadInfo, messageType, conversationHandle, reason);

        // System messages are handled by the message processing logic in DbBrokerListener
        // No additional processing needed here - just log and return
    }

    /**
     * Handle data messages containing table change events
     * <p>
     * Processing steps:
     * 1. Parse message to generic TableChangeEvent (to get table name)
     * 2. Find registered listener for the table
     * 3. Validate if event should be processed (enabled, supported event type)
     * 4. Re-parse with specific record type from listener configuration
     * 5. Route to appropriate listener method (onInsert/onUpdate/onDelete)
     */
    private void handleDataMessage(DbBrokerMessage message) {
        long startTime = System.currentTimeMillis();

        try {

            // STEP 1: Parse to generic event first (to get table name)
            TableChangeEvent genericEvent = messageParser.parseToTableChangeEvent(message);

            // STEP 2: Find listener registration
            TableListenerRegistryRecord registration = listenerRegistry.get(genericEvent.getTableName());

            // STEP 3: Validate if we should process this event
            if (!isValidForProcessing(registration, genericEvent)) {
                return;
            }

            // STEP 4: Get recordType from annotation and re-parse with DTO
            Class<?> recordType = registration.config().recordType();
            TableChangeEvent typedEvent = messageParser.parseToTableChangeEvent(message, recordType);

            // STEP 5: Route to listener
            routeEventToListener(typedEvent, registration);

            long processingTime = System.currentTimeMillis() - startTime;
            log.info("Processed event {} for table {} in {}ms",
                    typedEvent.getEventId(), typedEvent.getTableName(), processingTime);

        } catch (Throwable t) {
            transmissionErrorHandler.handleDataMessageError(message, t);
        }
    }

    /**
     * Route table change event to the appropriate listener method
     * <p>
     * Routes based on change type:
     * - INSERT → listener.onInsert(event)
     * - UPDATE → listener.onUpdate(event)
     * - DELETE → listener.onDelete(event)
     */
    private void routeEventToListener(TableChangeEvent event, TableListenerRegistryRecord registration) {
        try {
            switch (event.getChangeType()) {
                case INSERT -> registration.listener().onInsert(event);
                case UPDATE -> registration.listener().onUpdate(event);
                case DELETE -> registration.listener().onDelete(event);
            }

        } catch (Throwable t) {
            transmissionErrorHandler.handleEventRoutingError(event, t);
        }
    }
}