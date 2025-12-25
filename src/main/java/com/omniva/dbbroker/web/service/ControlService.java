package com.omniva.dbbroker.web.service;

import com.omniva.dbbroker.dashboard.DbBrokerECU;
import com.omniva.dbbroker.web.constants.DashboardConstants;
import com.omniva.dbbroker.web.dto.ControlStatusResponse;
import com.omniva.dbbroker.web.util.UptimeFormatter;

import java.time.Instant;
import java.util.Map;

public class ControlService {

    private final DbBrokerECU dbBrokerECU;

    public ControlService(DbBrokerECU dbBrokerECU) {
        this.dbBrokerECU = dbBrokerECU;
    }

    public Map<String, Object> startSupervisor() {
        DbBrokerECU.ControlStatus status = dbBrokerECU.getControlStatus();

        if (status.isSupervising()) {
            return Map.of(
                    DashboardConstants.SUCCESS_FIELD, false,
                    DashboardConstants.MESSAGE_FIELD, DashboardConstants.SUPERVISOR_ALREADY_RUNNING,
                    DashboardConstants.TIMESTAMP_FIELD, Instant.now()
            );
        }

        dbBrokerECU.startSupervision();

        return Map.of(
                DashboardConstants.SUCCESS_FIELD, true,
                DashboardConstants.MESSAGE_FIELD, DashboardConstants.SUPERVISOR_STARTED_SUCCESS,
                DashboardConstants.TIMESTAMP_FIELD, Instant.now()
        );
    }

    public Map<String, Object> stopSupervisor() {
        DbBrokerECU.ControlStatus status = dbBrokerECU.getControlStatus();

        if (!status.isSupervising()) {
            return Map.of(
                    DashboardConstants.SUCCESS_FIELD, false,
                    DashboardConstants.MESSAGE_FIELD, DashboardConstants.SUPERVISOR_ALREADY_STOPPED,
                    DashboardConstants.TIMESTAMP_FIELD, Instant.now()
            );
        }

        dbBrokerECU.stopSupervision();

        return Map.of(
                DashboardConstants.SUCCESS_FIELD, true,
                DashboardConstants.MESSAGE_FIELD, DashboardConstants.SUPERVISOR_STOPPED_SUCCESS,
                DashboardConstants.TIMESTAMP_FIELD, Instant.now()
        );
    }

    public Map<String, Object> restartSupervisor() {
        DbBrokerECU.ControlStatus status = dbBrokerECU.getControlStatus();

        if (!status.isSupervising() && !status.isHasPreservedState()) {
            return Map.of(
                    DashboardConstants.SUCCESS_FIELD, false,
                    DashboardConstants.MESSAGE_FIELD, DashboardConstants.CANNOT_RESTART_NO_STATE,
                    DashboardConstants.TIMESTAMP_FIELD, Instant.now()
            );
        }

        dbBrokerECU.restartSupervision();

        String message = status.isSupervising() ?
                DashboardConstants.SUPERVISOR_RESTARTED_SUCCESS :
                DashboardConstants.SUPERVISOR_RESUMED_SUCCESS;

        return Map.of(
                DashboardConstants.SUCCESS_FIELD, true,
                DashboardConstants.MESSAGE_FIELD, message,
                DashboardConstants.TIMESTAMP_FIELD, Instant.now()
        );
    }

    public Map<String, Object> restartFailedListeners() {
        DbBrokerECU.ControlStatus status = dbBrokerECU.getControlStatus();

        if (!status.isSupervising()) {
            return Map.of(
                    DashboardConstants.SUCCESS_FIELD, false,
                    DashboardConstants.MESSAGE_FIELD, DashboardConstants.SUPERVISOR_NOT_RUNNING,
                    DashboardConstants.TIMESTAMP_FIELD, Instant.now()
            );
        }

        if (status.getFailedListeners() == DashboardConstants.DEFAULT_ZERO) {
            return Map.of(
                    DashboardConstants.SUCCESS_FIELD, true,
                    DashboardConstants.MESSAGE_FIELD, DashboardConstants.NO_FAILED_LISTENERS,
                    DashboardConstants.ACTIVE_LISTENERS_ATTR, status.getActiveListeners(),
                    DashboardConstants.CONFIGURED_LISTENERS_ATTR, status.getConfiguredListeners(),
                    DashboardConstants.TIMESTAMP_FIELD, Instant.now()
            );
        }

        dbBrokerECU.restartFailedListeners();

        return Map.of(
                DashboardConstants.SUCCESS_FIELD, true,
                DashboardConstants.MESSAGE_FIELD, String.format(DashboardConstants.RESTARTED_FAILED_LISTENERS_FORMAT,
                        status.getFailedListeners()),
                DashboardConstants.FAILED_COUNT_FIELD, status.getFailedListeners(),
                DashboardConstants.TIMESTAMP_FIELD, Instant.now()
        );
    }

    public ControlStatusResponse getControlStatus() {
        DbBrokerECU.ControlStatus status = dbBrokerECU.getControlStatus();
        String formattedUptime = status.getSupervisionUptime() != null ?
                UptimeFormatter.formatUptime(status.getSupervisionUptime()) : DashboardConstants.DEFAULT_UPTIME;

        return new ControlStatusResponse(status, formattedUptime);
    }
}