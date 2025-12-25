package com.omniva.dbbroker.messaging.model;

import lombok.Builder;
import lombok.Data;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;

@Data
@Builder
public class DbBrokerMessage {

    private String conversationHandle;
    private String messageTypeName;
    private String messageBody;
    private Timestamp messageEnqueueTime;
    private Long messageSequenceNumber;
    private Integer messagePriority;
    private String conversationGroupId;
    private String serviceName;
    private String serviceContractName;

    private Instant receivedAt;
    private int threadId;

    /**
     * Create BdBrokerMessage from ResultSet
     * Maps all standard Service Broker fields as-is
     */
    public static DbBrokerMessage fromResultSet(ResultSet rs, int threadId) throws SQLException {
        return DbBrokerMessage.builder()
                .conversationHandle(rs.getString("conversation_handle"))
                .messageTypeName(rs.getString("message_type_name"))
                .messageBody(rs.getString("message_body"))
                .messageEnqueueTime(rs.getTimestamp("message_enqueue_time"))
                .messageSequenceNumber(getNullableLong(rs))
                .messagePriority(getNullableInteger(rs))
                .conversationGroupId(rs.getString("conversation_group_id"))
                .serviceName(rs.getString("service_name"))
                .serviceContractName(rs.getString("service_contract_name"))
                .receivedAt(Instant.now())
                .threadId(threadId)
                .build();
    }

    // UTILITY METHODS

    private static Long getNullableLong(ResultSet rs) throws SQLException {
        long value = rs.getLong("message_sequence_number");
        return rs.wasNull() ? null : value;
    }

    private static Integer getNullableInteger(ResultSet rs) throws SQLException {
        int value = rs.getInt("priority");
        return rs.wasNull() ? null : value;
    }

    public boolean isSystemMessage() {
        return messageTypeName != null && (
                messageTypeName.equals("http://schemas.microsoft.com/SQL/ServiceBroker/EndDialog") ||
                        messageTypeName.equals("http://schemas.microsoft.com/SQL/ServiceBroker/Error") ||
                        messageTypeName.equals("http://schemas.microsoft.com/SQL/ServiceBroker/DialogTimer")
        );
    }

    public boolean hasDataContent() {
        return messageBody != null && !messageBody.trim().isEmpty();
    }

}