package com.omniva.dbbroker.messaging.listener;

import java.util.Arrays;

public record TableListenerRegistryRecord(
        String tableName,
        TableListener listener,
        TableChangeListener config,
        String beanName
) {

    /**
     * Check if this listener is enabled
     */
    public boolean isEnabled() {
        return config.enabled();
    }

    /**
     * Check if this listener supports the given event type
     */
    public boolean supportsEvent(String eventType) {
        return Arrays.asList(config.events()).contains(eventType.toUpperCase());
    }
}