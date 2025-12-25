package com.omniva.dbbroker.engine.transmission;

import com.omniva.dbbroker.messaging.model.DbBrokerMessage;
import org.springframework.lang.NonNull;

/**
 * Processing result with context
 */
public record ProcessingResult(boolean hasMessage, DbBrokerMessage message) {

    @NonNull
    @Override
    public String toString() {
        return String.format("ProcessingResult{messageProcessed=%s, message=%s}",
                hasMessage, message != null ? message.getConversationHandle() : "null");
    }
}
