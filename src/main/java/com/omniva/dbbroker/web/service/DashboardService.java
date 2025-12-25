package com.omniva.dbbroker.web.service;

import com.omniva.dbbroker.dashboard.DbBrokerECU;
import com.omniva.dbbroker.engine.fault.ErrorTracker;
import com.omniva.dbbroker.engine.sensors.ListenerSensor;
import com.omniva.dbbroker.engine.sensors.SupervisorSensor;
import com.omniva.dbbroker.web.constants.DashboardConstants;
import com.omniva.dbbroker.web.dto.*;
import com.omniva.dbbroker.web.util.ErrorListUtils;
import com.omniva.dbbroker.web.util.ErrorStatisticsCalculator;
import com.omniva.dbbroker.web.util.UptimeFormatter;
import org.springframework.ui.Model;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

public class DashboardService {

    private final DbBrokerECU dbBrokerECU;
    private final ErrorTracker errorTracker;
    private final TemplateDataMapper templateDataMapper;

    public DashboardService(DbBrokerECU dbBrokerECU, ErrorTracker errorTracker, TemplateDataMapper templateDataMapper) {
        this.dbBrokerECU = dbBrokerECU;
        this.errorTracker = errorTracker;
        this.templateDataMapper = templateDataMapper;
    }

    public void populateDashboardModel(Model model) {
        try {
            DbBrokerECU.ControlStatus status = dbBrokerECU.getControlStatus();

            addBasicStatusToModel(model, status);
            addErrorDataToModel(model);
            addSensorDataToModel(model);

            model.addAttribute(DashboardConstants.REFRESH_TIME_ATTR, Instant.now());

        } catch (Exception e) {
            model.addAttribute(DashboardConstants.ERROR_FIELD,
                    DashboardConstants.FAILED_TO_LOAD_DASHBOARD_PREFIX + e.getMessage());
            addDefaultModelAttributes(model);
        }
    }

    private void addBasicStatusToModel(Model model, DbBrokerECU.ControlStatus status) {
        model.addAttribute(DashboardConstants.SERVICE_RUNNING_ATTR, status.isSupervising());
        model.addAttribute(DashboardConstants.HAS_PRESERVED_STATE_ATTR, status.isHasPreservedState());
        model.addAttribute(DashboardConstants.ACTIVE_LISTENERS_ATTR, status.getActiveListeners());
        model.addAttribute(DashboardConstants.CONFIGURED_LISTENERS_ATTR, status.getConfiguredListeners());
        model.addAttribute(DashboardConstants.FAILED_LISTENERS_ATTR, status.getFailedListeners());
        model.addAttribute(DashboardConstants.QUEUE_NAME_ATTR, status.getQueueName());
        model.addAttribute(DashboardConstants.SYSTEM_SUMMARY_ATTR, status.getStatusSummary());
        model.addAttribute(DashboardConstants.UPTIME_ATTR, status.getSupervisionUptime() != null ?
                UptimeFormatter.formatUptime(status.getSupervisionUptime()) : DashboardConstants.DEFAULT_UPTIME);
    }

    private void addErrorDataToModel(Model model) {
        // Get errors with full logging
        List<String> recentErrors = errorTracker.getRecentErrors(DashboardConstants.RECENT_ERRORS_LIMIT);

        // Test the utility function
        List<String> reversedErrors = ErrorListUtils.reverseErrorList(recentErrors);

        // Add to model
        model.addAttribute(DashboardConstants.RECENT_ERRORS_ATTR, reversedErrors);

        recentErrors = ErrorListUtils.reverseErrorList(
                errorTracker.getRecentErrors(DashboardConstants.RECENT_ERRORS_LIMIT));
        ErrorStatistics errorStats = ErrorStatisticsCalculator.calculateErrorStatistics(recentErrors);

        model.addAttribute(DashboardConstants.RECENT_ERRORS_ATTR, recentErrors);
        model.addAttribute(DashboardConstants.ERROR_STATISTICS_ATTR, errorStats);
        model.addAttribute(DashboardConstants.TOTAL_ERROR_COUNT_ATTR, errorTracker.getRecentErrorCount());
        model.addAttribute(DashboardConstants.HAS_ERRORS_ATTR, errorTracker.hasRecentErrors());
        model.addAttribute(DashboardConstants.LAST_ERROR_FIELD, errorTracker.getLastError());
        model.addAttribute(DashboardConstants.HAS_HIGH_ERROR_COUNT_ATTR,
                errorTracker.hasHighErrorCount(DashboardConstants.ERROR_THRESHOLD));
    }

    private void addSensorDataToModel(Model model) {
        try {
            SupervisorSensor supervisorSensor = dbBrokerECU.readSupervisorSensor();
            List<ListenerSensor> listenerSensors = dbBrokerECU.readListenerSensors();

            TemplateHealthStatus healthStatus = getHealthStatus();
            TemplatePerformanceMetrics performance = getPerformanceMetrics(supervisorSensor);
            List<TemplateListenerData> listenerDiagnostics = templateDataMapper.createListenerDataForTemplate(listenerSensors);

            model.addAttribute(DashboardConstants.HEALTH_STATUS_ATTR, healthStatus);
            model.addAttribute(DashboardConstants.PERFORMANCE_ATTR, performance);
            model.addAttribute(DashboardConstants.LISTENER_DIAGNOSTICS_ATTR, listenerDiagnostics);

        } catch (Exception sensorException) {
            model.addAttribute(DashboardConstants.HEALTH_STATUS_ATTR, templateDataMapper.createDefaultHealthStatus());
            model.addAttribute(DashboardConstants.PERFORMANCE_ATTR, templateDataMapper.createDefaultPerformanceMetrics());
            model.addAttribute(DashboardConstants.LISTENER_DIAGNOSTICS_ATTR, new ArrayList<>());
        }
    }

    private TemplateHealthStatus getHealthStatus() {
        try {
            DbBrokerECU.EngineHealthStatus healthCheck = dbBrokerECU.performHealthCheck();
            return templateDataMapper.createHealthStatusForTemplate(healthCheck);
        } catch (Exception e) {
            return templateDataMapper.createDefaultHealthStatus();
        }
    }

    private TemplatePerformanceMetrics getPerformanceMetrics(SupervisorSensor supervisor) {
        try {
            DbBrokerECU.EnginePerformanceMetrics performanceMetrics = dbBrokerECU.getPerformanceMetrics();
            return templateDataMapper.createPerformanceForTemplate(performanceMetrics, supervisor);
        } catch (Exception e) {
            return templateDataMapper.createDefaultPerformanceMetrics();
        }
    }

    public void addDefaultModelAttributes(Model model) {
        model.addAttribute(DashboardConstants.SERVICE_RUNNING_ATTR, false);
        model.addAttribute(DashboardConstants.HAS_PRESERVED_STATE_ATTR, false);
        model.addAttribute(DashboardConstants.ACTIVE_LISTENERS_ATTR, DashboardConstants.DEFAULT_ZERO);
        model.addAttribute(DashboardConstants.CONFIGURED_LISTENERS_ATTR, DashboardConstants.DEFAULT_ZERO);
        model.addAttribute(DashboardConstants.FAILED_LISTENERS_ATTR, DashboardConstants.DEFAULT_ZERO);
        model.addAttribute(DashboardConstants.QUEUE_NAME_ATTR, DashboardConstants.ERROR_QUEUE);
        model.addAttribute(DashboardConstants.SYSTEM_SUMMARY_ATTR, DashboardConstants.DASHBOARD_ERROR_SUMMARY);
        model.addAttribute(DashboardConstants.UPTIME_ATTR, DashboardConstants.DEFAULT_UPTIME);
        model.addAttribute(DashboardConstants.HEALTH_STATUS_ATTR, templateDataMapper.createDefaultHealthStatus());
        model.addAttribute(DashboardConstants.PERFORMANCE_ATTR, templateDataMapper.createDefaultPerformanceMetrics());
        model.addAttribute(DashboardConstants.LISTENER_DIAGNOSTICS_ATTR, new ArrayList<>());
        model.addAttribute(DashboardConstants.RECENT_ERRORS_ATTR, new ArrayList<>());
        model.addAttribute(DashboardConstants.ERROR_STATISTICS_ATTR, new ErrorStatistics());
        model.addAttribute(DashboardConstants.TOTAL_ERROR_COUNT_ATTR, DashboardConstants.DEFAULT_ZERO);
        model.addAttribute(DashboardConstants.HAS_ERRORS_ATTR, false);
        model.addAttribute(DashboardConstants.LAST_ERROR_FIELD, null);
        model.addAttribute(DashboardConstants.HAS_HIGH_ERROR_COUNT_ATTR, false);
        model.addAttribute(DashboardConstants.REFRESH_TIME_ATTR, Instant.now());
    }
}