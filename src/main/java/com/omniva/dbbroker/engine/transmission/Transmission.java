package com.omniva.dbbroker.engine.transmission;

import com.omniva.dbbroker.messaging.model.DbBrokerMessage;
import com.omniva.dbbroker.engine.fault.DbBrokerRuntimeException;
import com.omniva.dbbroker.engine.fault.PoisonMessageException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * Database Broker Transmission - Message Processing Engine
 * <p>
 * Like a transmission in a car, this component:
 * - Transfers power (messages) from the engine (database) to the wheels (business logic)
 * - Handles gear changes (different message types)
 * - Manages clutch operations (transaction boundaries)
 * - Provides smooth power delivery (reliable message processing)
 */
public class Transmission {

    private static final Logger log = LoggerFactory.getLogger(Transmission.class);

    private static final String WAITFOR_QUERY_TEMPLATE =
            "WAITFOR (RECEIVE TOP(1) " +
                    "conversation_handle, " +
                    "message_type_name, " +
                    "CAST(message_body AS NVARCHAR(MAX)) as message_body, " +
                    "message_enqueue_time, " +
                    "message_sequence_number, " +
                    "priority, " +
                    "conversation_group_id, " +
                    "service_name, " +
                    "service_contract_name " +
                    "FROM [%s])"
            ;

    /**
     * Build WAITFOR query (used by listener for direct statement management)
     */
    public String buildWaitForQuery(String queueName) {
        return String.format(WAITFOR_QUERY_TEMPLATE, queueName);
    }

    /**
     * Transfer power from engine to drivetrain.
     * Like a transmission transferring engine power to the drivetrain for actual work.
     *
     * @param message the power/message from the engine
     * @param drivetrain the drivetrain that will convert power into work
     * @param threadName the listener thread name for logging
     */
    public void transfer(DbBrokerMessage message, Transmitter drivetrain, String threadName) {
        try {

            // Transfer power to the drivetrain for actual work
            drivetrain.transmit(message);
        } catch (PoisonMessageException e) {
            log.error("Poison message detected for listener {}: {}", threadName, e.getMessage());
            throw e;
        } catch (DbBrokerRuntimeException e) {
            log.warn("Transient error processing message for listener {} - will retry: {}", threadName, e.getMessage());
            throw e;
        } catch (Exception e) {
            log.error("Message processing error for listener {}: {}", threadName, e.getMessage(), e);
            throw new DbBrokerRuntimeException(
                    "Message processing failure for listener " + threadName, e);
        } catch (Error t) {
            log.error("Critical error processing message for listener {}: {}", threadName, t.getMessage(), t);
            throw t;
        }
    }

    /**
     * End conversation (acknowledge message processing)
     */
    public void endConversation(Connection connection, String conversationHandle,
                                String reason, String threadName) throws SQLException {

        String sql = "END CONVERSATION ?";

        try (PreparedStatement stmt = connection.prepareStatement(sql)) {
            stmt.setString(1, conversationHandle);

            int rowsAffected = stmt.executeUpdate();

            if (rowsAffected > 0) {
                log.info("Conversation ended successfully for listener {} - Conversation: {} ({})",
                        threadName, conversationHandle, reason);
            } else {
                log.warn("No conversation found to end for listener {} - Conversation: {}",
                        threadName, conversationHandle);
            }

        } catch (SQLException e) {
            log.error("Error ending conversation for listener {} - Conversation: {}: {}",
                    threadName, conversationHandle, e.getMessage());
            throw e;
        }
    }
}