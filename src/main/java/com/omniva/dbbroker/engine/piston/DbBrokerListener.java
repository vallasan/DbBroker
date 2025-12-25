package com.omniva.dbbroker.engine.piston;

import com.omniva.dbbroker.messaging.model.DbBrokerMessage;
import com.omniva.dbbroker.engine.fault.DbBrokerFatalError;
import com.omniva.dbbroker.engine.fault.DbBrokerRuntimeException;
import com.omniva.dbbroker.engine.fault.PoisonMessageException;
import com.omniva.dbbroker.engine.fuelsystem.DbBrokerConnectionManager;
import com.omniva.dbbroker.engine.sensors.ListenerSensorProbe;
import com.omniva.dbbroker.engine.transmission.ProcessingResult;
import com.omniva.dbbroker.engine.transmission.Transmission;
import com.omniva.dbbroker.engine.transmission.TransmissionErrorHandler;
import com.omniva.dbbroker.engine.transmission.Transmitter;
import lombok.Getter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Database Broker Listener - The Engine Piston
 * <p>
 * Like a piston in an engine, this component:
 * - Performs the core work cycle (intake, compression, combustion, exhaust)
 * - Runs on its own dedicated connection (cylinder)
 * - Executes infinite WAITFOR loops (piston strokes)
 * - Transfers power through the transmission to the transmitter (drivetrain)
 * - Processes messages (fuel combustion)
 * - Reports to sensor probes for ECU monitoring
 * - Coordinates with crankshaft (supervisor)
 * <p>
 * The piston receives fuel (messages) from the fuel system (connection manager),
 * ignites it through the transmission, and delivers power to the transmitter
 * for actual work execution. Metrics and diagnostics are handled by dedicated sensor probes.
 */

public class DbBrokerListener implements Runnable {

    private static final Logger log = LoggerFactory.getLogger(DbBrokerListener.class);

    // Core Engine Components
    private final DbBrokerConnectionManager connectionManager; // Fuel System
    private final Transmission transmission; // Transmission (power transfer)
    private final TransmissionErrorHandler transmissionErrorHandler; // Engine Diagnostics/ECU
    private final Transmitter transmitter; // Drivetrain (final power application)

    // Piston Operating Components
    private final AtomicReference<PreparedStatement> currentStatement; // Spark Plug (ignition)
    private final AtomicReference<Connection> currentConnection; // Cylinder (combustion chamber)

    // Listener configuration (from factory)
    @Getter private final String queueName;
    @Getter private final int threadId;
    private String listenerName;
    // Sensor probe and diagnostics (extracted concerns)
    @Getter private final ListenerSensorProbe sensorProbe; // Engine sensors

    @Getter private volatile boolean running = false;
    private volatile boolean shutdownRequested = false;

    /**
     * Piston constructor - assembles engine components for message processing
     *
     * @param queueName the fuel line (queue)
     * @param transmitter the drivetrain that applies transmitted power
     * @param threadId the piston number in the engine block
     * @param connectionManager the fuel system providing connections
     * @param transmission the power transfer system
     * @param transmissionErrorHandler the engine diagnostics/ECU system
     */
    public DbBrokerListener(String queueName,
                            Transmitter transmitter,
                            int threadId,
                            DbBrokerConnectionManager connectionManager,
                            Transmission transmission,
                            TransmissionErrorHandler transmissionErrorHandler) {

        // Factory parameters
        this.queueName = queueName;
        this.transmitter = wrapTransmitter(transmitter); //wrapping all uncaught exceptions
        this.threadId = threadId;

        // Spring-injected components
        this.connectionManager = connectionManager;
        this.transmission = transmission;
        this.transmissionErrorHandler = transmissionErrorHandler;

        // Create a sensor probe and diagnostics for this listener
        this.sensorProbe = new ListenerSensorProbe(threadId, queueName);
        this.currentConnection = new AtomicReference<>();
        this.currentStatement = new AtomicReference<>();
    }

    /**
     * Wraps the transmitter with safety systems to prevent damage from bad messages.
     * Like signal conditioning that prevents transmission damage from corrupted signals.
     */
    private Transmitter wrapTransmitter(Transmitter transmitter) {
        return message -> {
            try {
                transmitter.transmit(message);
            } catch (PoisonMessageException e) {
                throw e; //Pass through known exceptions
            } catch (DbBrokerRuntimeException e) {
                throw e;
            } catch (Exception e) { //wrap uncaught exceptions in runtime exceptions
                throw new DbBrokerRuntimeException(
                        "Business logic error processing message in listener " + threadId, e);
            } catch (Error t) { //Pass through fatal errors
                log.error("Fatal error processing message in listener {}: {}", threadId, t.getMessage(), t);
                throw t;
            }
        };
    }

    /**
     * Main listener run method - The Processing Cycle
     */
    @Override
    public void run() {
        sensorProbe.recordIgnition(); // Engine startup initiated
        running = true;
        this.listenerName = Thread.currentThread().getName();

        log.info("Starting listener {} for queue: {}", threadId, queueName);

        try {
            // IGNITION PHASE - Start the engine
            Connection newConnection = connectionManager.createConnection();
            if (newConnection == null) {
                throw new SQLException("Connection manager returned null connection");
            }
            currentConnection.set(newConnection);
            newConnection.setAutoCommit(false);
            sensorProbe.recordConnectionEstablished();

            log.info("Listener {} ignition successful - engine running on queue: {}", threadId, queueName);

        } catch (SQLException e) {
            log.error("Ignition failure - SQL error starting listener {} for queue {}: {}", threadId, queueName, e.getMessage());
            // IGNITION PHASE ERROR - Engine won't start due to fuel system issue
            transmissionErrorHandler.handleIgnitionSqlError(listenerName, e, shutdownRequested);
            throw new DbBrokerFatalError("Engine ignition failed - connection establishment error", e);

        } catch (Exception e) {
            log.error("Ignition failure - error starting listener {} for queue {}: {}", threadId, queueName, e.getMessage());
            // IGNITION PHASE ERROR - Engine won't start due to general issue
            transmissionErrorHandler.handleIgnitionProcessingError(listenerName, e, shutdownRequested);
            throw new DbBrokerFatalError("Engine ignition failed - general startup error", e);

        } catch (Error t) {
            log.error("CRITICAL ignition failure in listener {} for queue {}: {}", threadId, queueName, t.getMessage(), t);
            // IGNITION PHASE ERROR - Catastrophic engine failure during startup
            transmissionErrorHandler.handleIgnitionCriticalError(listenerName, t);
            throw t; // Don't wrap Errors
        }

        // MAIN ENGINE LOOP - Keep pistons firing
        TransmissionErrorHandler.ProcessingState finalState = executeMainLoop();

        // Handle final engine state
        switch (finalState) {
            case STOP_GRACEFUL -> log.info("Listener {} engine stopped gracefully", listenerName);
            case STOP_ERROR -> log.warn("Listener {} engine stopped due to recoverable error", listenerName);
            case STOP_CRITICAL -> log.error("Listener {} engine stopped due to critical failure", listenerName);
            default -> log.info("Listener {} engine stopped with state: {}", listenerName, finalState);
        }

        // ENGINE SHUTDOWN - Clean up all resources
        performCleanup(finalState);

        // Determine if engine needs restart
        if (shouldRestartListener(finalState)) {
            throw new DbBrokerRuntimeException(
                    "Listener " + threadId + " engine requires restart - state: " + finalState);
        }
    }

    /**
     * Main processing loop - The Message Processing Cycle
     */
    private TransmissionErrorHandler.ProcessingState executeMainLoop() {
        TransmissionErrorHandler.ProcessingState state = TransmissionErrorHandler.ProcessingState.CONTINUE;

        while (shouldContinueRunning() && state == TransmissionErrorHandler.ProcessingState.CONTINUE) {
            sensorProbe.recordCycleStart();

            try {
                sensorProbe.recordConnectionEstablished();
                state = executeProcessingCycle();
            } catch (Exception e) {
                sensorProbe.recordMisfire();
                connectionManager.safeRollback(currentConnection.get(), listenerName);
                // IGNITION PHASE ERROR - General processing error during cycle
                state = transmissionErrorHandler.handleIgnitionProcessingError(listenerName, e, shutdownRequested);
            } catch (Error t) {
                sensorProbe.recordMisfire();
                connectionManager.safeClose(currentConnection.get(), listenerName);
                // IGNITION PHASE ERROR - Critical system failure during cycle
                state = transmissionErrorHandler.handleIgnitionCriticalError(listenerName, t);
            }
        }

        return state;
    }

    /**
     * Execute processing cycle
     */
    private TransmissionErrorHandler.ProcessingState executeProcessingCycle() {
        TransmissionErrorHandler.ProcessingState state = TransmissionErrorHandler.ProcessingState.CONTINUE;

        while (shouldContinueProcessing() && state == TransmissionErrorHandler.ProcessingState.CONTINUE) {

            if (shutdownRequested) {
                return TransmissionErrorHandler.ProcessingState.STOP_GRACEFUL;
            }

            // Prepare spark plug for ignition
            TransmissionErrorHandler.ProcessingState ignitionState = prepareStatement(listenerName);
            if (ignitionState != TransmissionErrorHandler.ProcessingState.CONTINUE) {
                return ignitionState;
            }

            // Execute the main processing cycle
            state = executeOttoCycle();
        }

        // ENGINE STOPS
        return state;
    }

    private TransmissionErrorHandler.ProcessingState prepareStatement(String threadName) {
        String sql = transmission.buildWaitForQuery(queueName);
        try {
            assert currentStatement != null;
            currentStatement.set(
                    currentConnection.get()
                            .prepareStatement(sql));
            return TransmissionErrorHandler.ProcessingState.CONTINUE;
        } catch (SQLException e) {
            log.warn("Listener {} SQL statement preparation failed: {}", threadName, e.getMessage());
            // IGNITION PHASE ERROR - Spark plug failure before engine starts
            return transmissionErrorHandler.handleIgnitionSqlError(threadName, e, shutdownRequested);
        }
    }

    private TransmissionErrorHandler.ProcessingState executeOttoCycle() {
        DbBrokerMessage message = null;
        try {
            // INTAKE - Draw fuel (message) from the fuel line (queue)
            ProcessingResult result = receiveMessage();

            if (result.hasMessage()) {
                //COMPRESSION - Prepare fuel (message) for combustion
                message = result.message();

                //COMBUSTION - Ignite fuel and transfer power through transmission to drivetrain
                transmission.transfer(message, transmitter, listenerName);

                //POWER STROKE - Confirm a successful power delivery (acknowledge message)
                acknowledgeMessage(message, listenerName);

                //EXHAUST - Clear the combustion chamber for the next cycle
                message = null;
            }

            return TransmissionErrorHandler.ProcessingState.CONTINUE;

        } catch (InterruptedException e) {
            connectionManager.cleanupListenerResources(currentStatement.get(), currentConnection.get(), listenerName);
            // CONNECTION PHASE ERROR - Emergency brake applied
            return transmissionErrorHandler.handleInterruption(currentConnection.get(), listenerName);

        } catch (SQLException e) {
            sensorProbe.recordMisfire();
            log.error("SQL error in listener {}: {}", listenerName, e.getMessage());
            // MESSAGE PROCESSING ERROR - Fuel system malfunction during combustion
            return transmissionErrorHandler.handleMessageSqlError(currentConnection.get(), listenerName, e, message, shutdownRequested);

        } catch (Exception e) {
            sensorProbe.recordMisfire();
            log.error("Transient error in listener {}: {}", listenerName, e.getMessage());
            // MESSAGE PROCESSING ERROR - Engine misfires during power delivery
            return transmissionErrorHandler.handleMessageProcessingError(currentConnection.get(), listenerName, e, message, shutdownRequested);

        } catch (Error t) {
            sensorProbe.recordMisfire();
            log.error("CRITICAL failure in listener {}: {}", listenerName, t.getMessage());
            // MESSAGE PROCESSING ERROR - Catastrophic engine failure
            return transmissionErrorHandler.handleMessageCriticalError(currentConnection.get(), listenerName, t, message);

        } finally {
            currentStatement.set(null);
        }
    }

    /**
     * Process message
     */
    private ProcessingResult receiveMessage() throws Exception {
        if (shutdownRequested || Thread.currentThread().isInterrupted()) {
            return new ProcessingResult(false, null);
        }

        try (ResultSet rs = currentStatement.get().executeQuery()) {
            if (rs.next()) {
                DbBrokerMessage message = DbBrokerMessage.fromResultSet(rs, threadId);
                return new ProcessingResult(true, message);
            } else {
                log.trace("No messages available in queue {} (timeout)", queueName);
                return new ProcessingResult(false, null);
            }
        }
    }

    /**
     * Message acknowledgment to transmission
     */
    private void acknowledgeMessage(DbBrokerMessage message, String threadName) throws SQLException {
        transmission.endConversation(
                currentConnection.get(),
                message.getConversationHandle(),
                "Successfully processed",
                threadName
        );

        currentConnection.get().commit();
        sensorProbe.recordSuccessfulCombustion();

    }

    /**
     * Request shutdown with thread interruption
     */
    public void requestShutdown() {
        log.info("Shutdown requested for listener {} (queue: {})", threadId, queueName);
        shutdownRequested = true;
        sensorProbe.recordShutdownRequested();

        connectionManager.interruptListenerOperations(currentStatement.get(), currentConnection.get(), listenerName);

        log.info("Shutdown request completed for listener {}", threadId);
    }

    /**
     * Perform cleanup
     */
    private void performCleanup(TransmissionErrorHandler.ProcessingState finalState) {
        running = false;

        log.info("Performing cleanup for listener {} (final state: {})", threadId, finalState);

        connectionManager.cleanupListenerResources(currentStatement.get(), currentConnection.get(), listenerName);

        log.info("Cleanup completed for listener {} - Total runtime: {}",
                threadId, sensorProbe.getUptime());
    }

    /**
     * Determine if listener should restart after shutdown
     */
    private boolean shouldRestartListener(TransmissionErrorHandler.ProcessingState finalState) {
        return !shutdownRequested &&
                finalState != TransmissionErrorHandler.ProcessingState.STOP_GRACEFUL &&
                finalState != TransmissionErrorHandler.ProcessingState.STOP_CRITICAL;
    }

    /**
     * Check if the listener should continue running
     */
    private boolean shouldContinueRunning() {
        return running && !shutdownRequested && !Thread.currentThread().isInterrupted();
    }

    private boolean shouldContinueProcessing() {
        return shouldContinueRunning();
    }

    // === Sensor Data Access Methods ===

    /**
     * Get listener uptime from sensor probe
     */
    public Duration getUptime() {
        return sensorProbe.getUptime();
    }

    public long getTotalMessagesProcessed() {
        return sensorProbe.getMessagesProcessed();
    }

    public long getTotalErrorsEncountered() {
        return sensorProbe.getErrorsEncountered();
    }

    public Instant getLastMessageTime() {
        return sensorProbe.getLastMessageTime();
    }

    public String getListenerName() {
        return listenerName != null ? listenerName : "Not Started";
    }

    /**
     * Get start time from the sensor probe
     */
    public Instant getStartTime() {
        return sensorProbe.getStartTime();
    }

    // === Object Methods ===

    @Override
    public String toString() {
        return String.format("DbBrokerListener[threadId=%d, queue=%s, running=%s, messages=%d, errors=%d]",
                threadId, queueName, running, getTotalMessagesProcessed(), getTotalErrorsEncountered());
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        DbBrokerListener that = (DbBrokerListener) obj;
        return threadId == that.threadId &&
                queueName != null ? queueName.equals(that.queueName) : that.queueName == null;
    }

    @Override
    public int hashCode() {
        return java.util.Objects.hash(threadId, queueName);
    }
}