package com.omniva.dbbroker.engine.sensors;

import lombok.Getter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Listener Sensor Probe
 * <p>
 * Collects real-time performance data from the listener:
 * - messages per second metrics
 * - error rates metrics
 * - cycle success rate metrics
 * - cycle durations
 * <p>
 * The probe continuously monitors and the ECU reads the sensor data.
 */
public class ListenerSensorProbe {
    private static final Logger log = LoggerFactory.getLogger(ListenerSensorProbe.class);

    @Getter
    private final int listenerId;
    @Getter
    private final String queueName;

    // Sensor probe readings
    @Getter
    private volatile Instant startTime;
    @Getter
    private volatile Instant lastMessageTime;
    private volatile Instant lastCycleStart;
    @Getter
    private volatile Duration lastCycleDuration;
    @Getter
    volatile Duration averageCycleDuration = Duration.ZERO;
    @Getter
    private volatile boolean running = false;
    @Getter
    private volatile boolean shutdownRequested = false;

    // Performance sensor counters
    private final AtomicLong messagesProcessed = new AtomicLong(0);
    private final AtomicLong errorsEncountered = new AtomicLong(0);
    private final AtomicLong connectionsCreated = new AtomicLong(0);
    private final AtomicLong totalCycles = new AtomicLong(0);
    private final AtomicLong successfulCycles = new AtomicLong(0);

    public ListenerSensorProbe(int listenerId, String queueName) {
        this.listenerId = listenerId;
        this.queueName = queueName;
    }

    // === Sensor Probe Events ===
    public void recordIgnition() {
        startTime = Instant.now();
        running = true;
        shutdownRequested = false;
        log.info("Listener {} sensor probe activated", listenerId);
    }

    public void recordShutdownRequested() {
        shutdownRequested = true;
        log.info("Listener {} shutdown requested", listenerId);
    }

    public void recordCycleStart() {
        lastCycleStart = Instant.now();
        totalCycles.incrementAndGet();
    }

    public void recordSuccessfulCombustion() {
        messagesProcessed.incrementAndGet();
        lastMessageTime = Instant.now();
        successfulCycles.incrementAndGet();
        updateCycleTiming();
    }

    public void recordMisfire() {
        errorsEncountered.incrementAndGet();
    }

    public void recordConnectionEstablished() {
        connectionsCreated.incrementAndGet();
    }

    // === Core Sensor Readings ===
    public Duration getUptime() {
        return startTime != null ? Duration.between(startTime, Instant.now()) : Duration.ZERO;
    }

    // === Private Methods ===
    private void updateCycleTiming() {
        if (lastCycleStart != null) {
            lastCycleDuration = Duration.between(lastCycleStart, Instant.now());

            if (averageCycleDuration.equals(Duration.ZERO)) {
                averageCycleDuration = lastCycleDuration;
            } else {
                long avgMillis = averageCycleDuration.toMillis();
                long newMillis = lastCycleDuration.toMillis();
                long smoothedMillis = (long) (avgMillis * 0.9 + newMillis * 0.1);
                averageCycleDuration = Duration.ofMillis(smoothedMillis);
            }
        }
    }

    // === Raw Sensor Data Getters ===
    public long getMessagesProcessed() { return messagesProcessed.get(); }
    public long getErrorsEncountered() { return errorsEncountered.get(); }
}