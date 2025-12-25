package com.omniva.dbbroker.engine.ignition;

import com.omniva.dbbroker.config.DbBrokerConfig;
import com.omniva.dbbroker.engine.crankshaft.DbBrokerSupervisor;
import com.omniva.dbbroker.engine.fault.DbBrokerFatalError;
import com.omniva.dbbroker.engine.fuelsystem.DbBrokerConnectionManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationContext;

public class EnvironmentValidator {
    private static final Logger log = LoggerFactory.getLogger(EnvironmentValidator.class);

    private final DbBrokerConfig config;
    private final DbBrokerConnectionManager connectionManager;
    private final ApplicationContext applicationContext;

    public EnvironmentValidator (DbBrokerConnectionManager connectionManager,
                                 DbBrokerConfig config,
                                 ApplicationContext applicationContext) {
        this.connectionManager = connectionManager;
        this.config = config;
        this.applicationContext = applicationContext;
    }

    /**
     * Validates the environment for a specific queue and thread.
     *
     * @return true if the environment is valid, false otherwise
     */
    public boolean validateEnvironment() {
        try {
            String queue = config.getQueueName();
            if (!connectionManager.isServiceBrokerEnabled()) {
                return false;
            }

            if (!connectionManager.isQueueEnabled(queue)) {
                return false;
            }

            if (!config.isValidQueue(queue)) {
                log.error("Invalid queue name: {}", queue);
                throw new DbBrokerFatalError("Invalid queue configuration: " + queue);
            }

            return true;
        } catch (DbBrokerFatalError fatalError) {
            // PROPAGATE: Fatal errors bubble up unchanged
            log.error("Fatal environment validation error: {}", fatalError.getMessage());
            logActiveListenerCount();
            throw fatalError;

        } catch (Exception e) {
            log.error("Environment validation failed: {}", e.getMessage(), e);
            logActiveListenerCount();
            return false;
        }
    }

    /**
     * Logs the current active listener count for monitoring.
     */
    private void logActiveListenerCount() {
        try {
            DbBrokerSupervisor supervisor = applicationContext.getBean(DbBrokerSupervisor.class);
            log.info("Active listener count: {}/{}",
                    supervisor.getActiveListenerCount(),
                    supervisor.getConfiguredListenerCount());
        } catch (Exception e) {
            log.warn("Could not retrieve supervisor for listener count logging: {}", e.getMessage());
            log.info("Active listener count: unavailable (supervisor not ready)");
        }
    }
}
