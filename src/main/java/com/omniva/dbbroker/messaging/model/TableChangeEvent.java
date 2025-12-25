package com.omniva.dbbroker.messaging.model;

import lombok.Builder;
import lombok.Data;

import java.time.LocalDateTime;
import java.util.Map;

@Data
@Builder
public class TableChangeEvent {
    private String eventId;
    private String tableName;
    private ChangeType changeType;
    private LocalDateTime eventTime;
    private LocalDateTime receivedTime;

    private Map<String, Object> rawRecord;     // Raw JSON data
    private Object dbRecord;                   // Parsed DTO (if successful)
    private Class<?> recordType;               // What DTO type was attempted

    private String conversationHandle;
    private String messageType;

    /**
     * Check if raw record data exists
     */
    public boolean hasRawRecord() {
        return rawRecord != null && !rawRecord.isEmpty();
    }

    /**
     * Check if parsed DTO record exists
     */
    public boolean hasRecord() {
        return dbRecord != null;
    }

    /**
     * Get parsed DTO record with type safety
     */
    @SuppressWarnings("unchecked")
    public <T> T getRecord(Class<T> expectedType) {
        if (expectedType.isInstance(dbRecord)) {
            return (T) dbRecord;
        }
        return null;
    }

    /**
     * Get parsed DTO record (generic)
     */
    @SuppressWarnings("unchecked")
    public <T> T getRecord() {
        return (T) dbRecord;
    }

    /**
     * Get raw field value if DTO parsing failed
     */
    public Object getRawFieldValue(String fieldName) {
        return hasRawRecord() ? rawRecord.get(fieldName) : null;
    }

    /**
     * Get raw field value with type casting
     */
    @SuppressWarnings("unchecked")
    public <T> T getRawFieldValue(String fieldName, Class<T> type) {
        Object value = getRawFieldValue(fieldName);
        if (type.isInstance(value)) {
            return (T) value;
        }
        return null;
    }

    /**
     * Get field value from DTO first, fallback to raw
     */
    public Object getFieldValue(String fieldName) {
        // Try DTO first, then fallback to raw
        if (hasRecord() && dbRecord instanceof Map<?, ?> recordMap) {
            Object value = recordMap.get(fieldName);
            if (value != null) {
                return value;
            }
        }
        return getRawFieldValue(fieldName);
    }

    public enum ChangeType {
        INSERT, UPDATE, DELETE
    }
}