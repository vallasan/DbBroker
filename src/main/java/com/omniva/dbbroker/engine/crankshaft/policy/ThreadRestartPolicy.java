package com.omniva.dbbroker.engine.crankshaft.policy;

public interface ThreadRestartPolicy {
    /**
     * Determine if a thread should be restarted based on the exception
     * @param ex The exception that caused the thread to fail
     * @return true if the thread should be restarted, false otherwise
     */
    boolean shouldRestart(Throwable ex);
}
