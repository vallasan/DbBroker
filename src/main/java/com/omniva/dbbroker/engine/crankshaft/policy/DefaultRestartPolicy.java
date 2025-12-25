package com.omniva.dbbroker.engine.crankshaft.policy;

import com.omniva.dbbroker.engine.fault.DbBrokerMaxRetriesExceededException;
import com.omniva.dbbroker.engine.fault.DbBrokerRuntimeException;
import com.omniva.dbbroker.engine.fault.DbBrokerShutdownException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DefaultRestartPolicy implements ThreadRestartPolicy {
    private static final Logger log = LoggerFactory.getLogger(DefaultRestartPolicy.class);

    @Override
    public boolean shouldRestart(Throwable ex) {
        // Don't restart for critical system errors
        if (ex instanceof Error) {
            log.info("Critical system error detected - will not restart thread");
            return false;
        }

        // Handle DbBrokerRuntimeException
        if (ex instanceof DbBrokerRuntimeException) {
            Throwable cause = ex.getCause();
            if (cause instanceof DbBrokerShutdownException) {
                log.info("Shutdown exception - will not restart");
                return false;
            }
            if (cause instanceof DbBrokerMaxRetriesExceededException) {
                log.info("Max retries exceeded - will not restart");
                return false;
            }
            return true;
        }

        // Don't restart for shutdown-related exceptions
        if (ex instanceof InterruptedException) {
            log.info("Shutdown-related exception - will not restart");
            return false;
        }

        // Default: restart for unexpected failures
        log.info("Unexpected thread failure - will restart");
        return true;
    }
}
