package com.omniva.dbbroker.engine.valvetrain;

import java.time.Instant;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Thread-safe tracker for individual message retry state.
 * Uses atomic references to ensure safe concurrent access.
 */
public class MessageRetryState {
    private final AtomicInteger retryCount = new AtomicInteger(0);
    private final AtomicReference<Instant> firstFailure = new AtomicReference<>();
    private final AtomicReference<String> errorType = new AtomicReference<>();

    /**
     * Atomically increments retry count and updates failure timestamps.
     * Thread-safe for concurrent access from multiple listener threads.
     *
     * @return The new retry count after increment
     */
    public int incrementAndGet() {
        if (firstFailure.get() == null) {
            firstFailure.set(Instant.now());
        }
        return retryCount.incrementAndGet();
    }

    /**
     * Gets the current retry count.
     *
     * @return Current retry count
     */
    public int get() {
        return retryCount.get();
    }

    /**
     * Gets the error type for the last failure.
     *
     * @return Error type string, or null if not set
     */
    public String getErrorType() {
        return errorType.get();
    }

    /**
     * Sets the error type for tracking failure categories.
     *
     * @param type The error type identifier
     */
    public void setErrorType(String type) {
        errorType.set(type);
    }

    /**
     * Gets the timestamp of the first failure.
     *
     * @return Instant of first failure, or null if no failures
     */
    public Instant getFirstFailure() {
        return firstFailure.get();
    }
}
