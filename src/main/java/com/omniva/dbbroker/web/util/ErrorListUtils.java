package com.omniva.dbbroker.web.util;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public final class ErrorListUtils {

    private ErrorListUtils() {
        // Utility class
    }

    public static List<String> reverseErrorList(List<String> errors) {
        if (errors == null || errors.isEmpty()) {
            return new ArrayList<>();
        }

        List<String> reversed = new ArrayList<>(errors);
        Collections.reverse(reversed);
        return reversed;
    }

    public static List<Map<String, Object>> reverseFormattedErrorList(List<Map<String, Object>> errors) {
        if (errors == null || errors.isEmpty()) {
            return new ArrayList<>();
        }

        List<Map<String, Object>> reversed = new ArrayList<>(errors);
        Collections.reverse(reversed);
        return reversed;
    }
}