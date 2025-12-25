package com.omniva.dbbroker.web.util;

import com.omniva.dbbroker.web.constants.DashboardConstants;
import com.omniva.dbbroker.web.dto.ErrorStatistics;

import java.util.List;

public final class ErrorStatisticsCalculator {

    private ErrorStatisticsCalculator() {
        // Utility class
    }

    public static ErrorStatistics calculateErrorStatistics(List<String> errors) {
        ErrorStatistics stats = new ErrorStatistics();

        if (errors == null || errors.isEmpty()) {
            return stats;
        }

        stats.setTotalErrors(errors.size());

        for (String error : errors) {
            String errorLower = error.toLowerCase();

            if (errorLower.contains(DashboardConstants.CRITICAL_KEYWORD) ||
                    errorLower.contains(DashboardConstants.FATAL_KEYWORD)) {
                stats.setCriticalErrors(stats.getCriticalErrors() + 1);
            } else if (errorLower.contains(DashboardConstants.POISON_KEYWORD)) {
                stats.setPoisonMessages(stats.getPoisonMessages() + 1);
            } else if (errorLower.contains(DashboardConstants.SQL_KEYWORD)) {
                stats.setSqlErrors(stats.getSqlErrors() + 1);
            } else {
                stats.setGeneralErrors(stats.getGeneralErrors() + 1);
            }
        }

        return stats;
    }
}