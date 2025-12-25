package com.omniva.dbbroker.engine.valvetrain;

import java.util.concurrent.ConcurrentHashMap;

/**
 * Centralized service for managing message retry state across all listener threads.
 * <p>
 * This service provides thread-safe operations for tracking retry attempts,
 * error types, and failure timestamps for each message identified by its
 * conversation handle.
 * <p>
 * THREAD SAFETY:
 * - Uses ConcurrentHashMap for thread-safe map operations
 * - MessageRetryState uses atomic operations internally
 * - computeIfAbsent ensures only one RetryState per conversation handle
 */
public class MessageRetryTracker {

    private final ConcurrentHashMap<String, MessageRetryState> stateMap =
            new ConcurrentHashMap<>();

    /**
     * Creates or retrieves the retry state for a conversation handle.
     * Use this when you need to perform multiple operations on the state.
     *
     * @param conversationHandle The unique message identifier
     * @return The RetryState (never null)
     */
    public MessageRetryState getOrCreateRetryState(String conversationHandle) {
        return stateMap.computeIfAbsent(conversationHandle,
                k -> new MessageRetryState());
    }

    /**
     * Clears the retry state for a conversation handle.
     * Call this after successful message processing.
     *
     * @param conversationHandle The unique message identifier
     */
    public void clearRetryState(String conversationHandle) {
        stateMap.remove(conversationHandle);
    }

    /**
     * Gets the total number of messages currently being tracked.
     *
     * @return The number of conversation handles with retry state
     */
    public int getTrackedMessageCount() {
        return stateMap.size();
    }

    /**
     * Clears all retry state. Use with caution.
     * Typically called during shutdown or testing.
     */
    public void clearAll() {
        stateMap.clear();
    }
}