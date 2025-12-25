package com.omniva.dbbroker.engine.crankshaft;

import com.omniva.dbbroker.config.DbBrokerConfig;
import com.omniva.dbbroker.engine.crankshaft.policy.DefaultRestartPolicy;
import com.omniva.dbbroker.engine.crankshaft.policy.ThreadRestartPolicy;
import com.omniva.dbbroker.engine.fault.DbBrokerSupervisionException;
import com.omniva.dbbroker.engine.fault.ErrorTracker;
import com.omniva.dbbroker.engine.piston.DbBrokerListener;
import com.omniva.dbbroker.engine.transmission.Transmitter;
import com.omniva.dbbroker.engine.valvetrain.MessageRetryTracker;
import lombok.Getter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.lang.NonNull;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Supervisor for DB Broker listener threads - The Crankshaft
 * <p>
 * This component:
 * - Creates and manages multiple pistons (DbBrokerListener instances)
 * - Monitors health of all engine cylinders (listener threads)
 * - Handles graceful engine shutdown
 * - Provides diagnostics for monitoring integration
 * - Restarts failed pistons automatically
 * <p>
 * Architecture:
 * DbBrokerSupervisor
 * └── DbBrokerSupervisionThreadPool (Engine Block)
 *     ├── DbBrokerListener-1 (Piston 1)
 *     ├── DbBrokerListener-2 (Piston 2)
 *     ├── DbBrokerListener-3 (Piston 3)
 *     ├── DbBrokerListener-4 (Piston 4)
 *     ├── DbBrokerListener-5 (Piston 5)
 */
public class DbBrokerSupervisor {
    private final DbBrokerConfig config;
    private final ErrorTracker errorTracker;
    @Getter
        private final MessageRetryTracker retryTracker; // Shared across all threads

    private final AtomicReference<DbBrokerSupervisionThreadPool> listenerExecutor = new AtomicReference<>();
    private final AtomicBoolean supervising = new AtomicBoolean(false);

    // Thread-safe list with Future tracking
    private final List<ListenerContext> activeListeners = new CopyOnWriteArrayList<>();

    private final AtomicReference<String> currentQueueName = new AtomicReference<>();
    private final AtomicReference<Instant> supervisionStartTime = new AtomicReference<>();
    @Getter private final AtomicReference<Transmitter> currentTransmitter = new AtomicReference<>();

    // DELEGATED COMPONENTS: Handle specialized logic
    private final DbBrokerListenerLifecycleManager lifecycleManager;
    private final ThreadRestartPolicy restartPolicy;
    private static final Logger log = LoggerFactory.getLogger(DbBrokerSupervisor.class);

    public DbBrokerSupervisor(
            DbBrokerConfig config,
            ErrorTracker errorTracker,
            MessageRetryTracker retryTracker,
            DbBrokerListenerLifecycleManager lifecycleManager) { // ← Inject pre-configured

        this.config = config;
        this.errorTracker = errorTracker;
        this.retryTracker = retryTracker;
        this.restartPolicy = new DefaultRestartPolicy();
        this.lifecycleManager = lifecycleManager;

        log.info("DbBrokerSupervisor created");
    }

    // Getter methods updated for AtomicReference
    public String getCurrentQueueName() {
        return currentQueueName.get();
    }

    public Instant getSupervisionStartTime() {
        return supervisionStartTime.get();
    }

    /**
     * Starts supervision of DB Broker listeners - Engine startup sequence
     * Creates N DbBrokerListener instances (pistons) from config.listener-threads
     *
     * @param queueName   The Service Broker queue to listen to (fuel line)
     * @param transmitter The drivetrain for processing messages (power application)
     */
    public void startSupervision(String queueName, Transmitter transmitter) {
        if (!supervising.compareAndSet(false, true)) {
            log.warn("Supervision already active");
            return;
        }

        log.info("Starting supervision for queue '{}' with {} listener threads",
                queueName, config.getListenerThreads());

        // Validate inputs
        if (queueName == null || queueName.trim().isEmpty()) {
            supervising.set(false);
            throw new IllegalArgumentException("Queue name cannot be null or empty");
        }

        if (transmitter == null) {
            supervising.set(false);
            throw new IllegalArgumentException("Message transmitter cannot be null");
        }

        // Thread-safe assignments
        currentQueueName.set(queueName);
        currentTransmitter.set(transmitter);
        supervisionStartTime.set(Instant.now());

        // Create DbBrokerSupervisionFactory with meaningful names and exception handling
        DbBrokerSupervisionThreadPool executor = getDbBrokerSupervisionThreadPool();

        // Thread-safe assignment
        listenerExecutor.set(executor);

        // CREATE: N DbBrokerListener instances
        try {
            for (int i = 1; i <= config.getListenerThreads(); i++) {
                DbBrokerListener listener = lifecycleManager.createListener(
                        queueName, transmitter);

                // Submit and track Future
                Future<?> future = executor.submit(listener);
                activeListeners.add(new ListenerContext(listener, future, Instant.now()));

                log.info("Started listener thread with ID: {}", i);
            }

            log.info("DbBrokerSupervisor started {} listener threads for queue: {}",
                    config.getListenerThreads(), queueName);

        } catch (Exception e) {
            String errorMsg = "Failed to start DB Broker supervision";
            errorTracker.addError(errorMsg, e);
            log.error("Failed to start supervision: {}", e.getMessage(), e);
            // Cleanup on failure
            stopSupervision();
            throw new DbBrokerSupervisionException(errorMsg, e);
        }
    }

    private @NonNull DbBrokerSupervisionThreadPool getDbBrokerSupervisionThreadPool() {
        DbBrokerSupervisionThreadFactory threadFactory = new DbBrokerSupervisionThreadFactory(
                "DbBroker-listener"
        );

        return new DbBrokerSupervisionThreadPool(
                config.getListenerThreads(),           // corePoolSize
                config.getListenerThreads(),           // maximumPoolSize
                0L,                                     // keepAliveTime (0 = threads never time out)
                TimeUnit.MILLISECONDS,                  // time unit
                new LinkedBlockingQueue<>(),            // work queue (unbounded)
                threadFactory,                          // DbBrokerListenerFactory
                this                                    // supervisor reference
        );
    }

    /**
     * Handle piston failure detected by DbBrokerSupervisionThreadPool.
     * Called from afterExecute() when a piston completes with an exception.
     *
     * @param runnable The runnable that failed (may be a FutureTask wrapper)
     * @param cause    The exception that caused the piston failure
     */
    public void handleListenerFailure(Runnable runnable, Throwable cause) {
        log.error("Listener task failed with exception: {}", cause.getMessage(), cause);

        // Find the listener context that corresponds to this runnable
        // Note: The runnable might be wrapped in a FutureTask, so we need to match by Future
        ListenerContext failedContext = findListenerContextByRunnable(runnable);

        if (failedContext == null) {
            log.warn("Could not find listener context for failed runnable");
            return;
        }

        // Remove the failed listener from the actives list
        activeListeners.remove(failedContext);

        int threadId = failedContext.listener().getThreadId();

        // Track the failure
        String errorMsg = String.format("Listener thread %d failed", threadId);
        errorTracker.addError(errorMsg, cause);

        // Check if we should restart this thread
        if (!restartPolicy.shouldRestart(cause)) {
            log.warn("Thread {} will NOT be restarted (exception type: {})",
                    threadId, cause.getClass().getSimpleName());
            return;
        }

        // Restart the failed listener
        restartFailedListener(failedContext, cause);

        int remainingThreads = getActiveListenerCount();
        log.warn("Listener thread died. Remaining active threads: {}/{}",
                remainingThreads, getConfiguredListenerCount());
        log.debug("Listener thread failure. Message Retry Tracker tracks {} messages", retryTracker.getTrackedMessageCount());
    }

    /**
     * Find the ListenerContext that corresponds to a failed runnable.
     * The runnable is typically wrapped in a FutureTask by the executor.
     */
    private ListenerContext findListenerContextByRunnable(Runnable runnable) {
        // If the runnable is a FutureTask, we can compare it to our stored Futures
        for (ListenerContext context : activeListeners) {
            if (context.future() != null && context.future().equals(runnable)) {
                return context;
            }
        }

        // If we can't find by Future, try to find by checking if the listener is not running
        for (ListenerContext context : activeListeners) {
            if (!context.listener().isRunning()) {
                log.info("Found dead listener context for thread ID: {}", context.listener().getThreadId());
                return context;
            }
        }

        return null;
    }

    /**
     * Restart a failed listener thread
     */
    private void restartFailedListener(ListenerContext deadContext, Throwable cause) {
        try {
            int threadId = deadContext.listener().getThreadId();

            log.info("Restarting failed listener thread (original ID: {}) due to: {}",
                    threadId, cause.getClass().getSimpleName());

            // Get current configuration
            String queueName = currentQueueName.get();
            Transmitter transmitter = currentTransmitter.get();
            DbBrokerSupervisionThreadPool executor = listenerExecutor.get();

            if (queueName == null || transmitter == null || executor == null || executor.isShutdown()) {
                log.warn("Cannot restart listener - supervisor is shutting down");
                return;
            }

            // Create new listener with next available thread ID
            int newThreadId = lifecycleManager.getNextAvailableThreadId();
            DbBrokerListener newListener = lifecycleManager.createListener(
                    queueName, transmitter);

            // Submit and track new Future with success callback
            Future<?> newFuture = executor.submit(newListener);

            ListenerContext newContext = new ListenerContext(newListener, newFuture, Instant.now());
            activeListeners.add(newContext);

            log.info("Successfully restarted listener thread with ID: {}", newThreadId);

        } catch (Exception restartError) {
            String errorMsg = "Failed to restart listener thread";
            errorTracker.addError(errorMsg, restartError);
            log.error("Failed to restart listener thread: {}", restartError.getMessage(), restartError);
        }
    }

    /**
     * Manually restart failed listeners (for monitoring/admin use)
     */
    public void restartFailedListeners() {
        if (!supervising.get()) {
            log.warn("Cannot restart listeners - supervision is not active");
            return;
        }

        List<ListenerContext> failedContexts = activeListeners.stream()
                .filter(context -> context.future().isDone() && !context.future().isCancelled())
                .toList();

        if (failedContexts.isEmpty()) {
            log.info("No failed listeners to restart");
            return;
        }

        log.info("Found {} failed listeners to restart", failedContexts.size());
        for (ListenerContext failedContext : failedContexts) {
            // Remove from the active list FIRST
            activeListeners.remove(failedContext);

            int oldThreadId = failedContext.listener().getThreadId();

            try {
                // Get configuration
                String queueName = currentQueueName.get();
                Transmitter transmitter = currentTransmitter.get();
                DbBrokerSupervisionThreadPool executor = listenerExecutor.get();

                if (queueName == null || transmitter == null || executor == null) {
                    log.warn("Cannot restart listener {} - missing configuration", oldThreadId);
                    continue;
                }

                // Create replacement listener
                DbBrokerListener newListener = lifecycleManager.createListener(
                        queueName,
                        transmitter
                );

                // Submit and track
                Future<?> newFuture = executor.submit(newListener);
                ListenerContext newContext = new ListenerContext(newListener, newFuture, Instant.now());
                activeListeners.add(newContext);

                log.info("Manually restarted listener (old ID: {}, new ID: {})",
                        oldThreadId, newListener.getThreadId());

            } catch (Exception e) {
                log.error("Failed to restart listener {}: {}", oldThreadId, e.getMessage(), e);
            }
        }

        log.info("Restart complete. Active listeners: {}/{}",
                getActiveListenerCount(), getConfiguredListenerCount());
    }

    /**
     * Stops supervision and all listener threads gracefully
     */
    public void stopSupervision() {
        stopSupervision(true); // Clear state by default
    }

    /**
     * Stops supervision and all listener threads gracefully
     */
    public void stopSupervision(boolean clearState) {
        if (!supervising.getAndSet(false)) {
            return;
        }

        log.info("Stopping DbBrokerSupervisor...");

        // Graceful shutdown of listeners first
        shutdownListenersGracefully();

        DbBrokerSupervisionThreadPool executor = listenerExecutor.getAndSet(null);
        if (executor != null) {
            executor.shutdown();

            try {
                // GRACEFUL: Wait for a configured timeout
                if (!executor.awaitTermination(config.getGracefulTimeoutSeconds(), TimeUnit.SECONDS)) {
                    log.warn("Graceful shutdown timeout ({}s) exceeded, forcing shutdown", config.getGracefulTimeoutSeconds());

                    // Cancel all futures before forcing shutdown
                    cancelAllListeners();

                    executor.shutdownNow();

                    // Wait a bit more for a forced shutdown
                    if (!executor.awaitTermination(10, TimeUnit.SECONDS)) {
                        log.error("Failed to shutdown DB Broker listener threads");
                    }
                }
            } catch (InterruptedException e) {
                log.warn("Interrupted while waiting for DbBrokerSupervisor shutdown");
                cancelAllListeners();
                executor.shutdownNow();
                Thread.currentThread().interrupt();
            } finally {
                // Always cleanup - thread-safe clearing
                activeListeners.clear();
                if (clearState) {
                    currentTransmitter.set(null);
                    currentQueueName.set(null);
                    supervisionStartTime.set(null);
                }

                retryTracker.clearAll();
            }
        } else {
            activeListeners.clear();
            retryTracker.clearAll();
        }

        log.info("DbBrokerSupervisor stopped");
    }

    /**
     * Gracefully shutdown listeners with enhanced thread interruption
     */
    private void shutdownListenersGracefully() {
        log.info("Requesting graceful shutdown of {} listeners", activeListeners.size());

        DbBrokerSupervisionThreadPool executor = listenerExecutor.get();

        for (ListenerContext context : activeListeners) {
            try {
                // Step 1: Request graceful shutdown (statement cancellation)
                context.listener().requestShutdown();

                if (executor != null) {
                    executor.interruptFuture(context.future());
                }
            } catch (Exception e) {
                log.warn("Error during graceful shutdown of listener {}: {}",
                        context.listener().getThreadId(), e.getMessage());
            }
        }

        log.info("Graceful shutdown requests sent to all listeners");
    }

    /**
     * Cancel all listener futures
     */
    private void cancelAllListeners() {
        log.info("Cancelling {} listener futures", activeListeners.size());

        for (ListenerContext context : activeListeners) {
            try {
                if (!context.future().isDone()) {
                    context.future().cancel(true);
                }
            } catch (Exception e) {
                log.warn("Error cancelling listener {}: {}",
                        context.listener().getThreadId(), e.getMessage());
            }
        }
    }

    /**
     * Restart supervision (for administrative operations)
     */
    public void restartSupervision() {
        log.info("Restarting supervision...");

        // Thread-safe retrieval of the current state
        String queueName = currentQueueName.get();

        stopSupervision(false);

        // Brief pause to ensure cleanup
        try {
            Thread.sleep(2000);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        if (queueName != null) {
            startSupervision(queueName, currentTransmitter.get());
        } else {
            String errorMsg = "Cannot restart supervision - missing queue name or message handler";
            errorTracker.addError(errorMsg);
            log.error(errorMsg);
            throw new DbBrokerSupervisionException(errorMsg);
        }
    }

    /**
     * Check if supervision is active
     */
    public boolean isSupervising() {
        return supervising.get();
    }

    /**
     * Get a number of active (running) listener threads
     */
    public int getActiveListenerCount() {
        return (int) activeListeners.stream()
                .filter(context -> context.listener().isRunning() && !context.future().isDone())
                .count();
    }

    /**
     * Get a configured number of listener threads
     */
    public int getConfiguredListenerCount() {
        return config.getListenerThreads();
    }

    /**
     * Get the total number of listener contexts (including failed ones)
     */
    public int getTotalListenerCount() {
        return activeListeners.size();
    }

    /**
     * Get supervision uptime
     */
    public Duration getSupervisionUptime() {
        Instant startTime = supervisionStartTime.get();
        return startTime != null ?
                Duration.between(startTime, Instant.now()) : Duration.ZERO;
    }

    /**
     * Get a list of listener contexts for monitoring
     */
    public List<ListenerContext> getActiveListeners() {
        return List.copyOf(activeListeners); // IMMUTABLE: Return defensive copy
    }

    /**
     * Check if all listeners are healthy
     */
    public boolean isHealthy() {
        DbBrokerSupervisionThreadPool executor = listenerExecutor.get();
        if (!supervising.get() || executor == null || executor.isShutdown()) {
            return false;
        }

        // Check if we have the expected number of active listeners
        int activeCount = getActiveListenerCount();
        int configuredCount = getConfiguredListenerCount();

        // The system is healthy if we have the right number of active listeners
        return activeCount == configuredCount;
    }
}