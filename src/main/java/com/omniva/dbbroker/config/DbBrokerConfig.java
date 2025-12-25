package com.omniva.dbbroker.config;

/**
 * Configuration interface for DB Broker Service
 * This interface abstracts the configuration details from the service implementation
 */
public interface DbBrokerConfig {

    String getQueueName();
    int getListenerThreads();

    // Validation methods
    boolean isValidQueue(String queueName);

    // Error handling methods
    int getMaxRetries();
    long getBaseRetryDelayMs();
    long getMaxRetryDelayMs();
    boolean isUseExponentialBackoff();

    // Shutdown methods
    int getGracefulTimeoutSeconds();
}
