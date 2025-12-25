package com.omniva.dbbroker.engine.fault;

public class DbBrokerMaxRetriesExceededException extends DbBrokerRuntimeException {

    public DbBrokerMaxRetriesExceededException(String message) {
        super(message);
    }
}
