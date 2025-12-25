package com.omniva.dbbroker.web.dto;

import lombok.Getter;
import lombok.Setter;

import java.time.LocalDateTime;

@Setter
@Getter
public class TemplateSensor {
    private int threadId;
    private String listenerName;
    private String queueName;
    private double messagesPerSecond;
    private double errorRate;
    private LocalDateTime lastMessageTime;
}
