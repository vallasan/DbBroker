package com.omniva.dbbroker.engine.fault;

/**
 * Error thrown when a fatal error occurs that should stop the listener thread
 */
public class DbBrokerFatalError extends Error {
    public DbBrokerFatalError(String message, Throwable cause) {
        super(message, cause);
    }

    public DbBrokerFatalError(String message) {
        super(message);
    }
}