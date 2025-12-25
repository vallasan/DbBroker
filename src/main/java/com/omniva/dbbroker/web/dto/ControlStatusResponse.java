package com.omniva.dbbroker.web.dto;

import com.omniva.dbbroker.dashboard.DbBrokerECU;
import com.omniva.dbbroker.web.constants.DashboardConstants;
import lombok.Getter;

import java.time.Instant;

@Getter
public class ControlStatusResponse {
    private final boolean supervising;
    private final boolean hasPreservedState;
    private final boolean canStop;
    private final boolean canRestart;
    private final boolean canRestartFailed;
    private final int activeListeners;
    private final int configuredListeners;
    private final int failedListeners;
    private final String queueName;
    private final String statusSummary;
    private final String uptime;
    private final Instant timestamp;

    public ControlStatusResponse(DbBrokerECU.ControlStatus status, String formattedUptime) {
        this.supervising = status.isSupervising();
        this.hasPreservedState = status.isHasPreservedState();
        this.canStop = status.isCanStop();
        this.canRestart = status.isCanRestart();
        this.canRestartFailed = status.isCanRestartFailed();
        this.activeListeners = status.getActiveListeners();
        this.configuredListeners = status.getConfiguredListeners();
        this.failedListeners = status.getFailedListeners();
        this.queueName = status.getQueueName() != null ? status.getQueueName() : DashboardConstants.NOT_AVAILABLE;
        this.statusSummary = status.getStatusSummary();
        this.uptime = formattedUptime;
        this.timestamp = Instant.now();
    }
}
