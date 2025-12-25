package com.omniva.dbbroker.engine.fault;

/**
 * Exception thrown when DB Broker supervision fails
 */
public class DbBrokerSupervisionException extends DbBrokerRuntimeException {
    public DbBrokerSupervisionException(String message) {
        super(message);
    }

    public DbBrokerSupervisionException(String message, Throwable cause) {
        super(message, cause);
    }
}