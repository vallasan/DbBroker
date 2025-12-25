package com.omniva.dbbroker.web.dto;

import lombok.Getter;
import lombok.Setter;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.LocalTime;

@Setter
@Getter
public class TemplatePerformanceMetrics {
    private double totalMessagesPerSecond;
    private LocalTime engineUptime;
    private Instant engineStartTime;
    private LocalDateTime lastUpdateTime;
}
