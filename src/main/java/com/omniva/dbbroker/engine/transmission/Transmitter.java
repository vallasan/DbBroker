package com.omniva.dbbroker.engine.transmission;

import com.omniva.dbbroker.messaging.model.DbBrokerMessage;

/**
 * The Transmitter - Converts transmitted power into actual work.
 * Receives power from the transmission and applies it to move the vehicle forward.
 */
@FunctionalInterface
public interface Transmitter {
    void transmit(DbBrokerMessage message);
}
