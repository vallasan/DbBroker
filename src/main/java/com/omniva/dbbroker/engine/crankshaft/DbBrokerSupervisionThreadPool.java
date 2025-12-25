package com.omniva.dbbroker.engine.crankshaft;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.lang.NonNull;

import java.util.List;
import java.util.Map;
import java.util.concurrent.*;


/**
 * ThreadPoolExecutor that handles exceptions from submit() tasks
 * by extracting them from Future objects in afterExecute().
 * Also tracks thread references for proper shutdown handling.
 */
public class DbBrokerSupervisionThreadPool extends ThreadPoolExecutor {
    private static final Logger log = LoggerFactory.getLogger(DbBrokerSupervisionThreadPool.class);
    private final DbBrokerSupervisor supervisor;

    // Track mappings for shutdown handling
    private final Map<Future<?>, Thread> futureToThreadMap = new ConcurrentHashMap<>();
    private final Map<Runnable, Thread> runnableToThreadMap = new ConcurrentHashMap<>();
    private final Map<Future<?>, Object> futureToTaskMap = new ConcurrentHashMap<>();

    public DbBrokerSupervisionThreadPool(
            int corePoolSize,
            int maximumPoolSize,
            long keepAliveTime,
            TimeUnit unit,
            BlockingQueue<Runnable> workQueue,
            ThreadFactory threadFactory,
            DbBrokerSupervisor supervisor) {
        super(corePoolSize, maximumPoolSize, keepAliveTime, unit, workQueue, threadFactory);
        this.supervisor = supervisor;
    }

    /**
     * Hook called before task execution - track thread
     */
    @Override
    protected void beforeExecute(Thread t, Runnable r) {
        super.beforeExecute(t, r);

        // Track Runnable -> Thread
        runnableToThreadMap.put(r, t);

        for (Map.Entry<Future<?>, Object> entry : futureToTaskMap.entrySet()) {
            Future<?> future = entry.getKey();
            // The actual runnable might be a FutureTask wrapping our original task
            if (future.equals(r) || (r instanceof FutureTask && r.equals(future))) {
                futureToThreadMap.put(future, t);
                break;
            }
        }
    }

    /**
     * Hook method invoked after each task execution.
     * Extracts exceptions from Future objects and notifies supervisor of failures.
     */
    @Override
    protected void afterExecute(Runnable r, Throwable t) {
        super.afterExecute(r, t);

        cleanupRunnableMapping(r);
        cleanupFutureMapping(r);

        // Check for exceptions if none was provided
        if (t == null) {
            t = extractExceptionFromFuture(r);
        }

        // Notify supervisor of any failures
        if (t != null) {
            supervisor.handleListenerFailure(r, t);
        }
    }

    private boolean isMatchingFuture(Future<?> future, Runnable r) {
        return future.equals(r) || (r instanceof FutureTask && r.equals(future));
    }

    private Throwable extractExceptionFromFuture(Runnable r) {
        if (!(r instanceof Future<?> future) || !future.isDone()) {
            return null;
        }

        try {
            future.get();
            return null;
        } catch (CancellationException ce) {
            log.warn("Task was cancelled: {}", ce.getMessage());
            return ce;
        } catch (ExecutionException ee) {
            Throwable cause = ee.getCause();
            log.error("Task failed with exception: {}",
                    cause != null ? cause.getMessage() : "Unknown", cause);
            return cause;
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            log.warn("Interrupted while checking task result: {}", ie.getMessage());
            return ie;
        }
    }


    private Future<?> findCompletedFuture(Runnable r) {
        for (Map.Entry<Future<?>, Object> entry : futureToTaskMap.entrySet()) {
            Future<?> future = entry.getKey();
            if (isMatchingFuture(future, r)) {
                return future;
            }
        }
        return null;
    }


    private void cleanupFutureMapping(Runnable r) {
        Future<?> completedFuture = findCompletedFuture(r);
        if (completedFuture != null) {
            futureToTaskMap.remove(completedFuture);
            futureToThreadMap.remove(completedFuture);
        }
    }


    private void cleanupRunnableMapping(Runnable r) {
        Thread executingThread = runnableToThreadMap.remove(r);
        if (executingThread != null) {
            log.debug("Task completed on thread: {} ({})",
                    executingThread.getName(), executingThread.threadId());
        }
    }

    /**
     * Override submitting to track Future -> Task mapping
     */
    @Override
    @NonNull
    public Future<?> submit(@NonNull Runnable task) {
        Future<?> future = super.submit(task);
        futureToTaskMap.put(future, task);
        return future;
    }

    /**
     * Override submit(Callable) with proper tracking
     */
    @Override
    @NonNull
    public <T> Future<T> submit(@NonNull Callable<T> task) {
        Future<T> future = super.submit(task);
        futureToTaskMap.put(future, task);
        return future;
    }

    /**
     * Override submit(Runnable, T) with tracking
     */
    @Override
    @NonNull
    public <T> Future<T> submit(@NonNull Runnable task, T result) {
        Future<T> future = super.submit(task, result);
        futureToTaskMap.put(future, task);
        return future;
    }


    /**
     * Get all currently executing threads
     */
    public Map<Runnable, Thread> getExecutingThreads() {
        return Map.copyOf(runnableToThreadMap);
    }

    /**
     * Get count of currently executing tasks
     */
    public int getExecutingTaskCount() {
        return runnableToThreadMap.size();
    }

    /**
     * Interrupt all executing threads (for emergency shutdown)
     */
    public void interruptAllExecutingThreads() {
        Map<Runnable, Thread> executingThreads = getExecutingThreads();
        log.info("Interrupting {} executing threads", executingThreads.size());

        for (Map.Entry<Runnable, Thread> entry : executingThreads.entrySet()) {
            Thread thread = entry.getValue();
            if (thread != null && thread.isAlive() && !thread.isInterrupted()) {
                log.debug("Interrupting thread: {} ({})", thread.getName(), thread.threadId());
                thread.interrupt();
            }
        }
    }

    /**
     * Interrupt thread executing a specific Future
     */
    public void interruptFuture(Future<?> future) {
        Thread thread = futureToThreadMap.get(future);
        if (thread != null && thread.isAlive() && !thread.isInterrupted()) {
            log.debug("Interrupting thread for future: {} ({})", thread.getName(), thread.threadId());
            thread.interrupt();
        }
    }

    /**
     * Hook method invoked when the executor has terminated.
     */
    @Override
    protected void terminated() {
        super.terminated();

        // Clean up tracking maps
        runnableToThreadMap.clear();
        futureToThreadMap.clear();
        futureToTaskMap.clear();

        log.info("DbBrokerSupervisionThreadPool has terminated");
    }

    /**
     * ShutdownNow with thread interruption
     */
    @Override
    @NonNull
    public List<Runnable> shutdownNow() {
        log.info("Force shutting down DbBrokerSupervisionThreadPool...");

        // Interrupt all executing threads before calling shutdownNow
        interruptAllExecutingThreads();

        return super.shutdownNow();
    }

    /**
     * Enhanced toString for debugging
     */
    @Override
    @NonNull
    public String toString() {
        return String.format("DbBrokerSupervisionThreadPool[" +
                        "pool=%d/%d, " +
                        "active=%d, " +
                        "queued=%d, " +
                        "completed=%d, " +
                        "executing=%d]",
                getPoolSize(),
                getMaximumPoolSize(),
                getActiveCount(),
                getQueue().size(),
                getCompletedTaskCount(),
                getExecutingTaskCount());
    }
}