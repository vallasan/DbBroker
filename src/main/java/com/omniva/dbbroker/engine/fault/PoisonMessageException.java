package com.omniva.dbbroker.engine.fault;

import lombok.Getter;

/**
 * Exception thrown when a message should be consumed as poison to prevent infinite reprocessing
 */
@Getter
public class PoisonMessageException extends DbBrokerRuntimeException {
    private final String conversationHandle;
    private final String messageType;
    private final Throwable originalError;

    public PoisonMessageException(String conversationHandle, String messageType, Throwable originalError) {
        super(String.format("Poison message detected - Type: %s, Conversation: %s, Original error: %s",
                messageType, conversationHandle, originalError.getMessage()), originalError);
        this.conversationHandle = conversationHandle;
        this.messageType = messageType;
        this.originalError = originalError;
    }

    public PoisonMessageException(String messageType, Throwable originalError) {
        super(String.format("Potential poison message detected - Type: %s, Original error: %s",
                messageType, originalError.getMessage()), originalError);
        this.conversationHandle = null;
        this.messageType = messageType;
        this.originalError = originalError;
    }

}