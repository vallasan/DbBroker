package com.omniva.dbbroker.autoconfigure;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.NestedConfigurationProperty;

import java.util.HashMap;
import java.util.Map;

@Data
@ConfigurationProperties(prefix = "db-broker")
public class DbBrokerProperties {

    private boolean enabled = true;
    private String queueName;
    private int listenerThreads = 4;

    @NestedConfigurationProperty
    private ValidationConfig validation = new ValidationConfig();

    @NestedConfigurationProperty
    private DataSourceConfig datasource = new DataSourceConfig();

    @NestedConfigurationProperty
    private ErrorHandlingConfig errorHandling = new ErrorHandlingConfig();

    @NestedConfigurationProperty
    private ShutdownConfig shutdown = new ShutdownConfig();

    @NestedConfigurationProperty
    private WebDashboardConfig webDashboard = new WebDashboardConfig();

    public boolean isValidQueue(String queueName) {
        return queueName != null &&
                !queueName.trim().isEmpty() &&
                queueName.length() <= validation.getMaxQueueNameLength();
    }

    @Data
    public static class DataSourceConfig {
        private String url;
        private String username;
        private String password;
        private String driverClassName = "com.microsoft.sqlserver.jdbc.SQLServerDriver";

        @NestedConfigurationProperty
        private HikariProperties hikari = new HikariProperties();

        @Data
        public static class HikariProperties {
            private int maximumPoolSize = 5; // Conservative for Service Broker
            private int minimumIdle = 1; // Minimal idle
            private long connectionTimeout = 30000; // 30 seconds
            private long idleTimeout = 0; // No timeout for WAITFOR
            private long maxLifetime = 0; // No max lifetime
            private String poolName = "DbBrokerPool"; // ← ADDED THIS
            private boolean autoCommit = false; // Correct for transactions
            private long leakDetectionThreshold = 0; // Disabled for WAITFOR
            private Map<String, String> dataSourceProperties = getDefaultDataSourceProperties();

            private static Map<String, String> getDefaultDataSourceProperties() {
                Map<String, String> defaults = new HashMap<>();
                // Performance optimizations for SQL Server
                defaults.put("cachePrepStmts", "true");
                defaults.put("prepStmtCacheSize", "250");
                defaults.put("prepStmtCacheSqlLimit", "2048");
                defaults.put("useServerPrepStmts", "true");
                defaults.put("cacheResultSetMetadata", "true");
                defaults.put("cacheServerConfiguration", "true");
                defaults.put("elideSetAutoCommits", "true");
                defaults.put("maintainTimeStats", "false");
                defaults.put("selectMethod", "cursor");
                defaults.put("responseBuffering", "adaptive");
                defaults.put("lockTimeout", "0");
                defaults.put("queryTimeout", "0");
                defaults.put("socketTimeout", "0");
                defaults.put("applicationName", "DBBroker");
                defaults.put("workstationID", "DBBroker-App");
                return defaults;
            }
        }
    }

    @Data
    public static class ErrorHandlingConfig {
        private int maxRetries = 3;
        private long baseRetryDelayMs = 1000;
        private long maxRetryDelayMs = 30000;
        private boolean useExponentialBackoff = true;
    }

    @Data
    public static class ShutdownConfig {
        private int gracefulTimeoutSeconds = 30;
    }

    @Data
    public static class ValidationConfig {
        private boolean enabled = true;
        private int maxQueueNameLength = 128;
        private int maxThreadNameLength = 64;
    }

    @Data
    public static class WebDashboardConfig {
        private boolean enabled = true;
        private String basePath = "/dbbroker";
        private boolean cacheTemplates = true;
    }
}