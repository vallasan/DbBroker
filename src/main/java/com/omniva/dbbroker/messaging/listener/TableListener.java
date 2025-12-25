package com.omniva.dbbroker.messaging.listener;

import com.omniva.dbbroker.messaging.model.TableChangeEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Interface for handling table change events
 * Implementations should be annotated with @TableChangeListener
 * <p>
 * TableChangeListener(
 * table = "MYTABLE",
 * recordType = MyTableRecordDTO.class,
 * events = {"INSERT", "UPDATE", "DELETE}
 * enabled = true/false
 * )
 */
public interface TableListener {

    Logger log = LoggerFactory.getLogger(TableListener.class);

    /**
     * Handle INSERT operations
     * Override this method if your listener supports INSERT events
     */
    default void onInsert(TableChangeEvent event) {
        log.error("INSERT event not handled by {}: {}", getListenerName(), event.getEventId());
    }

    /**
     * Handle UPDATE operations
     * Override this method if your listener supports UPDATE events
     */
    default void onUpdate(TableChangeEvent event) {
        log.error("UPDATE event not handled by {}: {}", getListenerName(), event.getEventId());
    }

    /**
     * Handle DELETE operations
     * Override this method if your listener supports DELETE events
     */
    default void onDelete(TableChangeEvent event) {
        log.error("DELETE event not handled by {}: {}", getListenerName(), event.getEventId());
    }

    /**
     * Called when listener is registered (optional override)
     */
    default void onRegistered(String tableName) {
        // Default: do nothing
    }

    /**
     * Called to validate listener setup (optional override)
     */
    default void validateSetup() {
        // Default: do nothing
    }

    /**
     * Get listener name for logging/debugging
     */
    default String getListenerName() {
        return this.getClass().getSimpleName();
    }
}