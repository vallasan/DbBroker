package com.omniva.dbbroker.autoconfigure;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.omniva.dbbroker.config.DbBrokerConfig;
import com.omniva.dbbroker.dashboard.DbBrokerECU;
import com.omniva.dbbroker.engine.crankshaft.DbBrokerListenerLifecycleManager;
import com.omniva.dbbroker.engine.fuelsystem.DbBrokerConnectionManager;
import com.omniva.dbbroker.engine.transmission.Transmission;
import com.omniva.dbbroker.engine.valvetrain.MessageRetryTracker;
import com.omniva.dbbroker.messaging.listener.TableChangeListener;
import com.omniva.dbbroker.messaging.listener.TableListenerRegistry;
import com.omniva.dbbroker.messaging.model.DbBrokerMessageParser;
import com.omniva.dbbroker.engine.DbBrokerEngine;
import com.omniva.dbbroker.engine.crankshaft.DbBrokerSupervisor;
import com.omniva.dbbroker.engine.fault.ErrorTracker;
import com.omniva.dbbroker.engine.ignition.EnvironmentValidator;
import com.omniva.dbbroker.engine.transmission.TransmissionErrorHandler;
import com.omniva.dbbroker.web.controller.DbBrokerDashboardController;
import com.omniva.dbbroker.web.service.*;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.FilterType;
import org.springframework.context.annotation.Primary;
import org.springframework.jdbc.core.JdbcTemplate;
import org.thymeleaf.spring6.templateresolver.SpringResourceTemplateResolver;
import org.thymeleaf.templatemode.TemplateMode;

import javax.sql.DataSource;

/**
 * Auto-configuration for DB Broker Engine
 * Enabled by default, can be disabled with: db-broker.enabled=false
 */
@AutoConfiguration
@ConditionalOnClass(DbBrokerEngine.class)
@ConditionalOnProperty(prefix = "db-broker", name = "enabled", havingValue = "true", matchIfMissing = true)
@EnableConfigurationProperties(DbBrokerProperties.class)
@ComponentScan(
        includeFilters = {
                // Find @TableChangeListener implementations anywhere
                @ComponentScan.Filter(
                        type = FilterType.ANNOTATION,
                        classes = TableChangeListener.class
                )
        },
        basePackageClasses = {
                TableListenerRegistry.class,
                DbBrokerDashboardController.class
        }
)
public class DbBrokerAutoConfiguration {

    // 1. Data layer

    @Bean("dbBrokerHikariConfig")
    @ConditionalOnMissingBean(name = "dbBrokerHikariConfig")
    public HikariConfig dbBrokerHikariConfig(DbBrokerProperties properties) {
        HikariConfig hikariConfig = new HikariConfig();
        DbBrokerProperties.DataSourceConfig datasource = properties.getDatasource();
        DbBrokerProperties.DataSourceConfig.HikariProperties hikariProps = datasource.getHikari();

        // Basic connection settings
        hikariConfig.setJdbcUrl(datasource.getUrl());
        hikariConfig.setUsername(datasource.getUsername());
        hikariConfig.setPassword(datasource.getPassword());
        hikariConfig.setDriverClassName(datasource.getDriverClassName());
        hikariConfig.setConnectionInitSql("SET LOCK_TIMEOUT -1");

        // Pool settings
        hikariConfig.setMaximumPoolSize(hikariProps.getMaximumPoolSize());
        hikariConfig.setMinimumIdle(hikariProps.getMinimumIdle());
        hikariConfig.setConnectionTimeout(hikariProps.getConnectionTimeout());
        hikariConfig.setIdleTimeout(hikariProps.getIdleTimeout());
        hikariConfig.setMaxLifetime(hikariProps.getMaxLifetime());
        hikariConfig.setPoolName(hikariProps.getPoolName());
        hikariConfig.setAutoCommit(hikariProps.isAutoCommit());
        hikariConfig.setLeakDetectionThreshold(hikariProps.getLeakDetectionThreshold());

        // Connection validation
        hikariConfig.setConnectionTestQuery("SELECT 1");
        hikariConfig.setValidationTimeout(5000);

        // Data source properties
        if (hikariProps.getDataSourceProperties() != null) {
            hikariProps.getDataSourceProperties().forEach(hikariConfig::addDataSourceProperty);
        }
        return hikariConfig;
    }

    @Bean("dbBrokerDataSource")
    @ConditionalOnMissingBean(name = "dbBrokerDataSource")
    public DataSource dbBrokerDataSource(@Qualifier("dbBrokerHikariConfig") HikariConfig config) {
        return new HikariDataSource(config);
    }

    @Bean("dbBrokerJdbcTemplate")
    @ConditionalOnMissingBean(name = "dbBrokerJdbcTemplate")
    public JdbcTemplate dbBrokerJdbcTemplate(@Qualifier("dbBrokerDataSource") DataSource dataSource) {
        return new JdbcTemplate(dataSource);
    }

    // 2. Configuration

    @Bean
    @ConditionalOnMissingBean
    public DbBrokerConfig dbBrokerConfig(DbBrokerProperties properties) {
        return new DbBrokerConfigAdapter(properties);
    }

    // 3. Core infrastructure (depends on config/data)

    @Bean
    @Primary
    public ErrorTracker errorTracker(DbBrokerConfig config) {
        return new ErrorTracker(config);
    }

    @Bean
    @ConditionalOnMissingBean
    public DbBrokerConnectionManager dbBrokerConnectionManager(@Qualifier("dbBrokerDataSource") DataSource dataSource) {
        return new DbBrokerConnectionManager(dataSource);
    }

    @Bean
    @ConditionalOnMissingBean
    public MessageRetryTracker messageRetryTracker() {
        return new MessageRetryTracker();
    }

    @Bean
    @ConditionalOnMissingBean
    public Transmission transmission() {
        return new Transmission();
    }

    // 4. Processing components (depends on core infrastructure)

    @Bean
    @ConditionalOnMissingBean
    public DbBrokerMessageParser dbBrokerMessageParser(ErrorTracker errorTracker,
                                                       ObjectMapper objectMapper) {
        return new DbBrokerMessageParser(errorTracker, objectMapper);
    }

    @Bean
    @ConditionalOnMissingBean
    public TransmissionErrorHandler transmissionErrorHandler(ErrorTracker errorTracker,
                                                             MessageRetryTracker messageRetryTracker,
                                                             DbBrokerConfig config,
                                                             DbBrokerConnectionManager connectionManager,
                                                             Transmission transmission,
                                                             ApplicationContext applicationContext) {
        return new TransmissionErrorHandler(errorTracker, messageRetryTracker, config,
                connectionManager, transmission, applicationContext);
    }

    @Bean
    @ConditionalOnMissingBean
    public EnvironmentValidator environmentValidator(DbBrokerConnectionManager connectionManager,
                                                     DbBrokerConfig config,
                                                     ApplicationContext applicationContext) {
        return new EnvironmentValidator(connectionManager, config, applicationContext);
    }

    // 5. Supervisor and Engine (depends on everything)

    @Bean
    @ConditionalOnMissingBean
    public DbBrokerListenerLifecycleManager dbBrokerListenerLifecycleManager(
            DbBrokerConnectionManager connectionManager,
            Transmission transmission,
            TransmissionErrorHandler errorHandler) {
        return new DbBrokerListenerLifecycleManager(connectionManager, transmission, errorHandler);
    }

    @Bean
    @ConditionalOnMissingBean
    public DbBrokerSupervisor dbBrokerSupervisor(
            DbBrokerConfig config,
            ErrorTracker errorTracker,
            MessageRetryTracker retryTracker,
            DbBrokerListenerLifecycleManager lifecycleManager) {
        return new DbBrokerSupervisor(config, errorTracker, retryTracker, lifecycleManager);
    }

    @Bean
    @ConditionalOnMissingBean
    public DbBrokerECU dbBrokerECU(DbBrokerSupervisor supervisor, ErrorTracker errorTracker) {
        return new DbBrokerECU(supervisor, errorTracker);
    }

    @Bean
    @ConditionalOnMissingBean
    public DbBrokerEngine dbBrokerEngine(
            DbBrokerConfig config,
            DbBrokerSupervisor supervisor,
            DbBrokerMessageParser messageParser,
            TableListenerRegistry listenerRegistry,
            ErrorTracker errorTracker,
            EnvironmentValidator environmentValidator,
            TransmissionErrorHandler transmissionErrorHandler) {

        return new DbBrokerEngine(
                config,
                supervisor,
                messageParser,
                listenerRegistry,
                errorTracker,
                environmentValidator,
                transmissionErrorHandler
        );
    }

    // WEB DASHBOARD COMPONENTS

    @Bean("dbBrokerTemplateResolver")
    @ConditionalOnMissingBean(name = "dbBrokerTemplateResolver")
    @ConditionalOnClass(name = "org.thymeleaf.spring6.SpringTemplateEngine")
    @ConditionalOnProperty(prefix = "db-broker.web-dashboard", name = "enabled", havingValue = "true", matchIfMissing = true)
    public SpringResourceTemplateResolver dbBrokerTemplateResolver() {
        SpringResourceTemplateResolver resolver = new SpringResourceTemplateResolver();

        resolver.setPrefix("classpath:/templates/");
        resolver.setSuffix(".html");
        resolver.setTemplateMode(TemplateMode.HTML);
        resolver.setCharacterEncoding("UTF-8");
        resolver.setCacheable(true); // Set to false for development
        resolver.setOrder(1); // Higher priority than default resolver

        return resolver;
    }

    @Bean
    @ConditionalOnMissingBean
    @ConditionalOnClass(name = "org.springframework.web.servlet.DispatcherServlet")
    @ConditionalOnProperty(prefix = "db-broker.web-dashboard", name = "enabled", havingValue = "true", matchIfMissing = true)
    public TemplateDataMapper templateDataMapper() {
        return new TemplateDataMapper();
    }

    @Bean
    @ConditionalOnMissingBean
    @ConditionalOnClass(name = "org.springframework.web.servlet.DispatcherServlet")
    @ConditionalOnProperty(prefix = "db-broker.web-dashboard", name = "enabled", havingValue = "true", matchIfMissing = true)
    public DashboardService dashboardService(DbBrokerECU ecu,
                                             ErrorTracker errorTracker,
                                             TemplateDataMapper templateDataMapper) {
        return new DashboardService(ecu, errorTracker, templateDataMapper);
    }

    @Bean
    @ConditionalOnMissingBean
    @ConditionalOnClass(name = "org.springframework.web.servlet.DispatcherServlet")
    @ConditionalOnProperty(prefix = "db-broker.web-dashboard", name = "enabled", havingValue = "true", matchIfMissing = true)
    public ControlService controlService(DbBrokerECU ecu) {
        return new ControlService(ecu);
    }

    @Bean
    @ConditionalOnMissingBean
    @ConditionalOnClass(name = "org.springframework.web.servlet.DispatcherServlet")
    @ConditionalOnProperty(prefix = "db-broker.web-dashboard", name = "enabled", havingValue = "true", matchIfMissing = true)
    public StatusService statusService(DbBrokerECU ecu) {
        return new StatusService(ecu);
    }

    @Bean
    @ConditionalOnMissingBean
    @ConditionalOnClass(name = "org.springframework.web.servlet.DispatcherServlet")
    @ConditionalOnProperty(prefix = "db-broker.web-dashboard", name = "enabled", havingValue = "true", matchIfMissing = true)
    public LiveDataService liveDataService(DbBrokerECU ecu, ErrorTracker errorTracker) {
        return new LiveDataService(ecu, errorTracker);
    }

    /**
     * Adapter to convert properties to config interface
     */
    private record DbBrokerConfigAdapter(DbBrokerProperties properties) implements DbBrokerConfig {

        @Override
        public String getQueueName() {
            return properties.getQueueName();
        }

        @Override
        public int getListenerThreads() {
            return properties.getListenerThreads();
        }

        @Override
        public boolean isValidQueue(String queueName) {
            return properties.isValidQueue(queueName);
        }

        @Override
        public int getMaxRetries() {
            return properties.getErrorHandling().getMaxRetries();
        }

        @Override
        public long getBaseRetryDelayMs() {
            return properties.getErrorHandling().getBaseRetryDelayMs();
        }

        @Override
        public long getMaxRetryDelayMs() {
            return properties.getErrorHandling().getMaxRetryDelayMs();
        }

        @Override
        public boolean isUseExponentialBackoff() {
            return properties.getErrorHandling().isUseExponentialBackoff();
        }

        @Override
        public int getGracefulTimeoutSeconds() {
            return properties.getShutdown().getGracefulTimeoutSeconds();
        }
    }
}