package com.omniva.dbbroker.web.dto;

import lombok.Getter;
import lombok.Setter;

public class ErrorStatistics {
    @Getter
    @Setter
    private int totalErrors;
    @Getter
    @Setter
    private int criticalErrors;
    @Getter
    @Setter
    private int sqlErrors;
    @Getter
    @Setter
    private int poisonMessages;
    @Getter
    @Setter
    private int generalErrors;

    public ErrorStatistics() {
        this.totalErrors = 0;
        this.criticalErrors = 0;
        this.sqlErrors = 0;
        this.poisonMessages = 0;
        this.generalErrors = 0;
    }
}