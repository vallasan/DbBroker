package com.omniva.dbbroker.web.service;

import com.omniva.dbbroker.dashboard.DbBrokerECU;
import com.omniva.dbbroker.engine.sensors.ListenerSensor;
import com.omniva.dbbroker.engine.sensors.SupervisorSensor;
import com.omniva.dbbroker.web.constants.DashboardConstants;
import com.omniva.dbbroker.web.dto.*;
import com.omniva.dbbroker.web.util.EfficiencyCalculator;
import com.omniva.dbbroker.web.util.UptimeFormatter;

import org.springframework.lang.NonNull;

import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

public class TemplateDataMapper {

    public TemplateHealthStatus createDefaultHealthStatus() {
        TemplateHealthStatus healthStatus = new TemplateHealthStatus();
        healthStatus.setHealthy(false);
        healthStatus.setActiveListeners(DashboardConstants.DEFAULT_ZERO);
        healthStatus.setConfiguredListeners(DashboardConstants.DEFAULT_ZERO);
        healthStatus.setErrorRate(DashboardConstants.DEFAULT_ZERO_DOUBLE);
        healthStatus.setLastHealthCheck(LocalDateTime.now());
        return healthStatus;
    }

    public TemplatePerformanceMetrics createDefaultPerformanceMetrics() {
        TemplatePerformanceMetrics performance = new TemplatePerformanceMetrics();
        performance.setTotalMessagesPerSecond(DashboardConstants.DEFAULT_ZERO_DOUBLE);
        performance.setEngineUptime(LocalTime.of(DashboardConstants.DEFAULT_ZERO,
                DashboardConstants.DEFAULT_ZERO, DashboardConstants.DEFAULT_ZERO));
        performance.setEngineStartTime(null);
        performance.setLastUpdateTime(LocalDateTime.now());
        return performance;
    }

    public TemplateHealthStatus createHealthStatusForTemplate(DbBrokerECU.EngineHealthStatus health) {
        TemplateHealthStatus status = new TemplateHealthStatus();

        if (health != null) {
            status.setHealthy(health.isHealthy());
            status.setActiveListeners(health.getActiveListeners());
            status.setConfiguredListeners(health.getConfiguredListeners());
            status.setErrorRate(health.getErrorRate());
        } else {
            status.setHealthy(false);
            status.setActiveListeners(DashboardConstants.DEFAULT_ZERO);
            status.setConfiguredListeners(DashboardConstants.DEFAULT_ZERO);
            status.setErrorRate(DashboardConstants.DEFAULT_ZERO_DOUBLE);
        }

        status.setLastHealthCheck(LocalDateTime.now());
        return status;
    }

    public TemplatePerformanceMetrics createPerformanceForTemplate(
            DbBrokerECU.EnginePerformanceMetrics performance,
            SupervisorSensor supervisor) {

        TemplatePerformanceMetrics metrics = new TemplatePerformanceMetrics();

        if (performance != null) {
            metrics.setTotalMessagesPerSecond(performance.getTotalMessagesPerSecond());
            metrics.setEngineUptime(UptimeFormatter.convertDurationToLocalTime(performance.getEngineUptime()));
        } else {
            metrics.setTotalMessagesPerSecond(DashboardConstants.DEFAULT_ZERO_DOUBLE);
            metrics.setEngineUptime(LocalTime.of(DashboardConstants.DEFAULT_ZERO,
                    DashboardConstants.DEFAULT_ZERO, DashboardConstants.DEFAULT_ZERO));
        }

        if (supervisor != null) {
            metrics.setEngineStartTime(supervisor.getSupervisionStartTime());
        } else {
            metrics.setEngineStartTime(null);
        }

        metrics.setLastUpdateTime(LocalDateTime.now());
        return metrics;
    }

    public List<TemplateListenerData> createListenerDataForTemplate(List<ListenerSensor> sensors) {
        if (sensors == null || sensors.isEmpty()) {
            return new ArrayList<>();
        }

        return sensors.stream()
                .filter(Objects::nonNull)
                .map(this::convertSensorToTemplateData)
                .collect(Collectors.toList());
    }

    private TemplateListenerData convertSensorToTemplateData(ListenerSensor sensor) {
        TemplateListenerData data = new TemplateListenerData();

        if (sensor != null) {
            TemplateSensor templateSensor = getTemplateSensor(sensor);

            data.setSensor(templateSensor);
            data.setHealthStatus(sensor.isRunning() ? DashboardConstants.HEALTHY_STATUS : DashboardConstants.STOPPED_STATUS);
            data.setEfficiencyScore(EfficiencyCalculator.calculateEfficiency(sensor));
            data.setPerformanceGrade(EfficiencyCalculator.calculateGrade(data.getEfficiencyScore()));
            data.setNeedsAttention(!sensor.isRunning() || sensor.getErrorRate() > DashboardConstants.HIGH_ERROR_RATE_THRESHOLD);
            data.setCritical(sensor.getErrorRate() > DashboardConstants.CRITICAL_ERROR_RATE_THRESHOLD);
        } else {
            TemplateSensor templateSensor = new TemplateSensor();
            templateSensor.setThreadId(DashboardConstants.DEFAULT_ZERO);
            templateSensor.setListenerName("");
            templateSensor.setQueueName(DashboardConstants.UNKNOWN_QUEUE);
            templateSensor.setMessagesPerSecond(DashboardConstants.DEFAULT_ZERO_DOUBLE);
            templateSensor.setErrorRate(DashboardConstants.DEFAULT_ZERO_DOUBLE);
            templateSensor.setLastMessageTime(null);

            data.setSensor(templateSensor);
            data.setHealthStatus(DashboardConstants.UNKNOWN_STATUS);
            data.setEfficiencyScore(DashboardConstants.DEFAULT_ZERO_DOUBLE);
            data.setPerformanceGrade(DashboardConstants.GRADE_F);
            data.setNeedsAttention(true);
            data.setCritical(false);
        }

        data.setLastStatusUpdate(LocalDateTime.now());
        return data;
    }

    private static @NonNull TemplateSensor getTemplateSensor(ListenerSensor sensor) {
        TemplateSensor templateSensor = new TemplateSensor();
        templateSensor.setThreadId(sensor.getThreadId());
        templateSensor.setListenerName(sensor.getListenerName());
        templateSensor.setQueueName(sensor.getQueueName() != null ? sensor.getQueueName() : DashboardConstants.UNKNOWN_QUEUE);
        templateSensor.setMessagesPerSecond(sensor.getMessagesPerSecond());
        templateSensor.setErrorRate(sensor.getErrorRate());
        templateSensor.setLastMessageTime(sensor.getLastMessageTime() != null ?
                LocalDateTime.ofInstant(sensor.getLastMessageTime(), java.time.ZoneId.systemDefault()) : null);
        return templateSensor;
    }
}