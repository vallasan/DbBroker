package com.omniva.dbbroker.messaging.model;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.omniva.dbbroker.engine.fault.ErrorTracker;
import com.omniva.dbbroker.engine.fault.PoisonMessageException;
import lombok.RequiredArgsConstructor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.Map;

/**
 * Parse DbBrokerMessage to TableChangeEvent
 * <p>
 * Message JSON:
 * {
 * "eventId": "OSYNDEXP_9_B5719C8C-7B2A-4700-8109-98711215CC5C",
 * "tableName": "OSYNDEXP",
 * "operation": "INSERT",
 * "timestamp": "2025-12-01T15:47:03.280",
 * "record": {"Jrknr":9,"Id":92749,"Tunnus":92749}
 * }
 */
@RequiredArgsConstructor
public class DbBrokerMessageParser {
    private static final Logger log = LoggerFactory.getLogger(DbBrokerMessageParser.class);

    private final ErrorTracker errorTracker;
    private final ObjectMapper objectMapper;

    public TableChangeEvent parseToTableChangeEvent(DbBrokerMessage message, Class<?> recordType) {
        try {
            String jsonContent = message.getMessageBody();

            if (jsonContent == null || jsonContent.trim().isEmpty()) {
                throw new PoisonMessageException(message.getConversationHandle(), "Poisonous message", new IllegalArgumentException("Message body is empty"));
            }

            JsonNode jsonNode = objectMapper.readTree(jsonContent);

            // Extract required fields from JSON
            String eventId = getRequiredString(jsonNode, "eventId");
            String tableName = getRequiredString(jsonNode, "tableName");
            String operation = getRequiredString(jsonNode, "operation");
            String timestamp = jsonNode.path("timestamp").asText();

            Map<String, Object> rawRecord = extractRawRecord(jsonNode);

            Object parsedRecord = rawRecord;
            if (recordType != null && !Map.class.equals(recordType)) {
                parsedRecord = convertRawRecord(rawRecord, recordType);
                if (parsedRecord == null) {
                    log.warn("Failed to parse record to DTO type: {}, will use rawRecord", recordType.getSimpleName());
                }
            }

            return TableChangeEvent.builder()
                    .eventId(eventId)
                    .tableName(tableName.toUpperCase())
                    .changeType(TableChangeEvent.ChangeType.valueOf(operation.toUpperCase()))
                    .eventTime(parseTimestamp(timestamp))
                    .receivedTime(LocalDateTime.now())
                    .rawRecord(rawRecord)
                    .dbRecord(parsedRecord)
                    .recordType(recordType)
                    .conversationHandle(message.getConversationHandle())
                    .messageType(message.getMessageTypeName())
                    .build();

        } catch (JsonProcessingException e) {
            // Specific handling for JSON parsing errors
            String errorMsg = String.format("Malformed JSON in message %s: %s",
                    message.getMessageTypeName(), e.getOriginalMessage());
            errorTracker.addError(errorMsg, e);
            log.error("JSON parsing failed - treating as poison: {}", e.getMessage(), e);
            throw new PoisonMessageException(message.getConversationHandle(), errorMsg, e);

        } catch (IllegalArgumentException e) {
            // Specific handling for validation errors
            String errorMsg = String.format("Invalid message data in %s: %s",
                    message.getMessageTypeName(), e.getMessage());
            errorTracker.addError(errorMsg, e);
            log.error("Validation failed - treating as poison: {}", e.getMessage(), e);
            throw new PoisonMessageException(message.getConversationHandle(), errorMsg, e);

        } catch (Exception e) {
            // Catch-all for unexpected errors
            String errorMsg = String.format("Unexpected error parsing message %s: %s",
                    message.getMessageTypeName(), e.getMessage());
            errorTracker.addError(errorMsg, e);
            log.error("Unexpected parsing error - treating as poison: {}", e.getMessage(), e);
            throw new PoisonMessageException(message.getConversationHandle(), errorMsg, e);
        }
    }

    /**
     * OVERLOAD: Parse without DTO conversion (backward compatibility)
     */
    public TableChangeEvent parseToTableChangeEvent(DbBrokerMessage message) {
        return parseToTableChangeEvent(message, null);
    }

    /**
     * UTILITY: Convert rawRecord to DTO (can be used independently)
     */
    public <T> T convertRawRecord(Map<String, Object> rawRecord, Class<T> dtoClass) {
        if (rawRecord == null || rawRecord.isEmpty()) {
            return null;
        }

        if (dtoClass == null) {
            return null;
        }

        if (Map.class.equals(dtoClass)) {
            return dtoClass.cast(rawRecord);
        }

        try {
            return objectMapper.convertValue(rawRecord, dtoClass);

        } catch (Exception e) {
            log.error("Failed to convert rawRecord to DTO {}: {}",
                    dtoClass.getSimpleName(), e.getMessage());
            return null;
        }
    }

    // PRIVATE HELPER METHODS

    /**
     * Extract required string field from JSON
     */
    private String getRequiredString(JsonNode jsonNode, String fieldName) {
        JsonNode fieldNode = jsonNode.path(fieldName);
        if (fieldNode.isMissingNode() || fieldNode.asText().trim().isEmpty()) {
            throw new PoisonMessageException(fieldName + " is required but missing or empty", new IllegalArgumentException(fieldName + " is required but missing or empty"));
        }
        return fieldNode.asText().trim();
    }

    /**
     * Extract record data from JSON as Map<String, Object>
     */
    @SuppressWarnings("unchecked")
    private Map<String, Object> extractRawRecord(JsonNode jsonNode) {
        JsonNode recordNode = jsonNode.path("record");

        if (recordNode.isMissingNode() || recordNode.isNull()) {
            return new HashMap<>();
        }

        try {
            Map<String, Object> rawRecord = objectMapper.convertValue(recordNode, Map.class);
            return rawRecord != null ? rawRecord : new HashMap<>();

        } catch (Exception e) {
            log.warn("Failed to extract record data from JSON, using empty map: {}", e.getMessage());
            return new HashMap<>();
        }
    }

    /**
     * Parse timestamp string to LocalDateTime
     */
    private LocalDateTime parseTimestamp(String timestampStr) {
        if (timestampStr == null || timestampStr.trim().isEmpty()) {
            return LocalDateTime.now();
        }

        try {
            return LocalDateTime.parse(timestampStr, DateTimeFormatter.ISO_LOCAL_DATE_TIME);
        } catch (Exception e) {
            log.warn("Failed to parse timestamp '{}', using current time: {}", timestampStr, e.getMessage());
            return LocalDateTime.now();
        }
    }
}