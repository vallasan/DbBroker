package com.omniva.dbbroker.web.dto;

import lombok.Getter;
import lombok.Setter;

import java.time.LocalDateTime;

@Setter
@Getter
public class TemplateHealthStatus {
    private boolean isHealthy;
    private int activeListeners;
    private int configuredListeners;
    private double errorRate;
    private LocalDateTime lastHealthCheck;
}
