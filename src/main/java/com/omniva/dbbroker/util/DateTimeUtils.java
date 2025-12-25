package com.omniva.dbbroker.util;

import java.time.Duration;

public class DateTimeUtils {
    /**
     * Format duration for display across the application
     */
    public static String formatDuration(Duration duration) {
        if (duration == null || duration.isZero()) return "0s";

        long seconds = duration.getSeconds();
        if (seconds < 60) {
            return seconds + "s";
        } else if (seconds < 3600) {
            return (seconds / 60) + "m " + (seconds % 60) + "s";
        } else if (seconds < 86400) {
            long hours = seconds / 3600;
            long minutes = (seconds % 3600) / 60;
            long secs = seconds % 60;
            return hours + "h " + minutes + "m " + secs + "s";
        } else {
            long days = seconds / 86400;
            long hours = (seconds % 86400) / 3600;
            long minutes = (seconds % 3600) / 60;
            return days + "d " + hours + "h " + minutes + "m";
        }
    }
}
