package com.omniva.dbbroker.engine.crankshaft;

import com.omniva.dbbroker.engine.fuelsystem.DbBrokerConnectionManager;
import com.omniva.dbbroker.engine.piston.DbBrokerListener;
import com.omniva.dbbroker.engine.transmission.Transmission;
import com.omniva.dbbroker.engine.transmission.TransmissionErrorHandler;
import com.omniva.dbbroker.engine.transmission.Transmitter;

import java.util.concurrent.atomic.AtomicInteger;

public class DbBrokerListenerLifecycleManager {

    // ONLY Spring-injected dependencies (actually used)
    private final DbBrokerConnectionManager connectionManager;
    private final Transmission transmission;
    private final TransmissionErrorHandler errorHandler;

    private final AtomicInteger nextThreadId = new AtomicInteger(1);

    /**
     * Crankshaft assembly with core engine components
     */
    public DbBrokerListenerLifecycleManager(
            DbBrokerConnectionManager connectionManager,
            Transmission transmission,
            TransmissionErrorHandler errorHandler) {

        this.connectionManager = connectionManager;
        this.transmission = transmission;
        this.errorHandler = errorHandler;
    }

    /**
     * Create a new listener instance with the next available thread ID
     * <p>
     * Like manufacturing a new piston in the engine block, this method
     * assembles all the necessary components for message processing.
     * <p>
     * @param queueName The Service Broker queue name (fuel line)
     * @param transmitter The business logic handler for messages (drivetrain)
     * @return A fully configured DbBrokerListener ready to run (assembled piston)
     */
    public DbBrokerListener createListener(
            String queueName,
            Transmitter transmitter) {

        // Validate inputs
        if (queueName == null || queueName.trim().isEmpty()) {
            throw new IllegalArgumentException("Queue name cannot be null or empty");
        }

        if (transmitter == null) {
            throw new IllegalArgumentException("Transmitter cannot be null");
        }

        // Assign the next thread ID (piston number)
        int threadId = nextThreadId.getAndIncrement();

        // Create and return a configured listener (assembled piston)
        return new DbBrokerListener(
                queueName,
                transmitter,
                threadId,
                connectionManager,
                transmission,
                errorHandler
        );
    }

    /**
     * Get the next available thread ID for listener creation
     * <p>
     * @return The next piston number in the engine block
     */
    public int getNextAvailableThreadId() {
        return nextThreadId.get();
    }
}