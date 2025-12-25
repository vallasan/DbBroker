package com.omniva.dbbroker.engine.crankshaft;

import org.springframework.lang.NonNull;

import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * ThreadFactory for creating DB Broker listener threads with:
 * - Meaningful thread names
 * - Non-daemon threads to prevent JVM from exiting prematurely
 * - Normal thread priority
 */
public class DbBrokerSupervisionThreadFactory implements ThreadFactory {

    private final AtomicInteger threadCounter = new AtomicInteger(0);
    private final String namePrefix;

    /**
     * Create a new ThreadFactory for DB Broker listener threads
     *
     * @param namePrefix Prefix for thread names (e.g., "DbBroker-listener")
     */
    public DbBrokerSupervisionThreadFactory(String namePrefix) {
        this.namePrefix = namePrefix;
    }

    @Override
    public Thread newThread(@NonNull Runnable r) {
        // Create thread with meaningful name
        Thread t = new Thread(r, namePrefix + "-" + threadCounter.getAndIncrement());

        // Set as non-daemon (important for proper shutdown)
        t.setDaemon(false);

        // Set normal priority
        t.setPriority(Thread.NORM_PRIORITY);

        return t;
    }

}
