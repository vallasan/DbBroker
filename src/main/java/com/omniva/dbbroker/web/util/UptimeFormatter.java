package com.omniva.dbbroker.web.util;

import com.omniva.dbbroker.web.constants.DashboardConstants;

import java.time.Duration;
import java.time.LocalTime;

public final class UptimeFormatter {

    private UptimeFormatter() {
        // Utility class
    }

    public static String formatUptime(Duration duration) {
        if (duration == null) return DashboardConstants.DEFAULT_UPTIME;

        long hours = duration.toHours();
        long minutes = duration.toMinutesPart();
        long seconds = duration.toSecondsPart();

        return String.format(DashboardConstants.UPTIME_FORMAT, hours, minutes, seconds);
    }

    public static LocalTime convertDurationToLocalTime(Duration duration) {
        if (duration == null) return LocalTime.of(DashboardConstants.DEFAULT_ZERO,
                DashboardConstants.DEFAULT_ZERO, DashboardConstants.DEFAULT_ZERO);

        long hours = duration.toHours() % DashboardConstants.HOURS_IN_DAY;
        long minutes = duration.toMinutesPart();
        long seconds = duration.toSecondsPart();

        return LocalTime.of((int)hours, (int)minutes, (int)seconds);
    }
}