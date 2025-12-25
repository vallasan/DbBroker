package com.omniva.dbbroker.web.service;

import com.omniva.dbbroker.dashboard.DbBrokerECU;
import com.omniva.dbbroker.engine.sensors.SupervisorSensor;
import com.omniva.dbbroker.web.constants.DashboardConstants;
import com.omniva.dbbroker.web.util.UptimeFormatter;

import java.time.Instant;
import java.util.Map;

public class StatusService {

    private final DbBrokerECU dbBrokerECU;

    public StatusService(DbBrokerECU dbBrokerECU) {
        this.dbBrokerECU = dbBrokerECU;
    }

    public Map<String, Object> getCurrentStatus() {
        DbBrokerECU.EngineHealthStatus health = dbBrokerECU.performHealthCheck();
        DbBrokerECU.EnginePerformanceMetrics performance = dbBrokerECU.getPerformanceMetrics();
        SupervisorSensor supervisor = dbBrokerECU.readSupervisorSensor();

        return Map.of(
                DashboardConstants.SERVICE_RUNNING_ATTR, supervisor.isSupervising(),
                DashboardConstants.ENGINE_HEALTHY_FIELD, health.isHealthy(),
                DashboardConstants.OVERALL_STATUS_FIELD, health.getOverallStatus(),
                DashboardConstants.ACTIVE_LISTENERS_ATTR, health.getActiveListeners(),
                DashboardConstants.CONFIGURED_LISTENERS_ATTR, health.getConfiguredListeners(),
                DashboardConstants.MESSAGES_PER_SECOND_FIELD, performance.getTotalMessagesPerSecond(),
                DashboardConstants.ERROR_RATE_FIELD, health.getErrorRate(),
                DashboardConstants.UPTIME_ATTR, UptimeFormatter.formatUptime(performance.getEngineUptime()),
                DashboardConstants.TIMESTAMP_FIELD, Instant.now()
        );
    }
}