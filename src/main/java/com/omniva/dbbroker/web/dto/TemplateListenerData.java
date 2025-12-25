package com.omniva.dbbroker.web.dto;

import lombok.Getter;
import lombok.Setter;

import java.time.LocalDateTime;

@Setter
@Getter
public class TemplateListenerData {
    private TemplateSensor sensor;
    private String healthStatus;
    private double efficiencyScore;
    private String performanceGrade;
    private boolean needsAttention;
    private boolean isCritical;
    private LocalDateTime lastStatusUpdate;
}
