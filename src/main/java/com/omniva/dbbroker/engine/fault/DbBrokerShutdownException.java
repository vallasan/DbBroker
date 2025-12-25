package com.omniva.dbbroker.engine.fault;

/**
 * Exception thrown when DB Broker service fails to stop
 */
public class DbBrokerShutdownException extends DbBrokerRuntimeException {
    public DbBrokerShutdownException(String message) {
        super(message);
    }

    public DbBrokerShutdownException(String message, Throwable cause) {
        super(message, cause);
    }
}