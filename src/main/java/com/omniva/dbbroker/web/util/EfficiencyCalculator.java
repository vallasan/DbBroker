package com.omniva.dbbroker.web.util;

import com.omniva.dbbroker.engine.sensors.ListenerSensor;
import com.omniva.dbbroker.web.constants.DashboardConstants;

public final class EfficiencyCalculator {

    private EfficiencyCalculator() {
        // Utility class
    }

    public static double calculateEfficiency(ListenerSensor sensor) {
        if (!sensor.isRunning()) return DashboardConstants.DEFAULT_ZERO_DOUBLE;

        double throughputScore = Math.min(sensor.getMessagesPerSecond(), DashboardConstants.MAX_THROUGHPUT_SCORE);
        double reliabilityScore = Math.max(DashboardConstants.DEFAULT_ZERO_DOUBLE,
                DashboardConstants.MAX_THROUGHPUT_SCORE - (sensor.getErrorRate() / DashboardConstants.MAX_ERROR_RATE_FOR_RELIABILITY));
        double activityScore = sensor.hasRecentActivity() ? DashboardConstants.ACTIVE_SCORE : DashboardConstants.INACTIVE_SCORE;

        return (throughputScore * DashboardConstants.THROUGHPUT_WEIGHT +
                reliabilityScore * DashboardConstants.RELIABILITY_WEIGHT +
                activityScore * DashboardConstants.ACTIVITY_WEIGHT);
    }

    public static String calculateGrade(double efficiency) {
        if (efficiency >= DashboardConstants.GRADE_A_THRESHOLD) return DashboardConstants.GRADE_A;
        if (efficiency >= DashboardConstants.GRADE_B_THRESHOLD) return DashboardConstants.GRADE_B;
        if (efficiency >= DashboardConstants.GRADE_C_THRESHOLD) return DashboardConstants.GRADE_C;
        if (efficiency >= DashboardConstants.GRADE_D_THRESHOLD) return DashboardConstants.GRADE_D;
        return DashboardConstants.GRADE_F;
    }
}