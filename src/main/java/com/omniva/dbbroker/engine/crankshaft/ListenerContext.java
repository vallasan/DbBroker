package com.omniva.dbbroker.engine.crankshaft;

import com.omniva.dbbroker.engine.piston.DbBrokerListener;

import java.time.Instant;
import java.util.concurrent.Future;

/**
 * Internal class to track listener context
 */
public record ListenerContext(DbBrokerListener listener, Future<?> future, Instant startTime) {

    /**
     * Check if this listener is running
     */
    public boolean isRunning() {
        return !future.isDone() && listener.isRunning();
    }

    /**
     * Check if this listener has failed
     */
    public boolean isFailed() {
        return future.isDone() && !future.isCancelled();
    }

}
