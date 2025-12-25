package com.omniva.dbbroker.engine.fault;

/**
 * Base runtime exception for all DB Broker related errors
 */
public class DbBrokerRuntimeException extends RuntimeException {
    public DbBrokerRuntimeException(String message) {
        super(message);
    }

    public DbBrokerRuntimeException(String message, Throwable cause) {
        super(message, cause);
    }
}