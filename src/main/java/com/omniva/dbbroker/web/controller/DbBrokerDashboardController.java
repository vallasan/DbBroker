package com.omniva.dbbroker.web.controller;

import com.omniva.dbbroker.web.constants.DashboardConstants;
import com.omniva.dbbroker.web.dto.ControlStatusResponse;
import com.omniva.dbbroker.web.service.*;
import jakarta.servlet.http.HttpServletRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.*;

import java.time.Instant;
import java.util.Map;

@Controller
@RequestMapping("${db-broker.web-dashboard.base-path:/dbbroker}")
@ConditionalOnClass(name = "org.springframework.web.servlet.DispatcherServlet")
@ConditionalOnProperty(prefix = "db-broker.web-dashboard", name = "enabled", havingValue = "true", matchIfMissing = true)
public class DbBrokerDashboardController {

    private static final Logger logger = LoggerFactory.getLogger(DbBrokerDashboardController.class);

    private final DashboardService dashboardService;
    private final ControlService controlService;
    private final StatusService statusService;
    private final LiveDataService liveDataService;

    @Value("${db-broker.web-dashboard.base-path:/dbbroker}")
    private String basePath;

    @Value("${db-broker.web-dashboard.refresh-interval:5000}")
    private int refreshInterval;

    public DbBrokerDashboardController(DashboardService dashboardService,
                                       ControlService controlService,
                                       StatusService statusService,
                                       LiveDataService liveDataService) {
        this.dashboardService = dashboardService;
        this.controlService = controlService;
        this.statusService = statusService;
        this.liveDataService = liveDataService;
    }

    @GetMapping(DashboardConstants.DASHBOARD_ENDPOINT)
    public String dashboard(Model model, HttpServletRequest request) {
        try {
            dashboardService.populateDashboardModel(model);
            model.addAttribute("basePath", basePath);
            model.addAttribute("refreshInterval", refreshInterval);
            model.addAttribute(DashboardConstants.REFRESH_TIME_ATTR, Instant.now());

            return DashboardConstants.DASHBOARD_TEMPLATE;

        } catch (Exception e) {
            logger.error("Failed to load dashboard", e);
            return handleDashboardError(model, e, DashboardConstants.FAILED_TO_LOAD_DASHBOARD_PREFIX.replace(": ", ""));
        }
    }

    @GetMapping(DashboardConstants.ERROR_ENDPOINT)
    public String errorPage(Model model,
                            @RequestParam(required = false) String error,
                            @RequestParam(required = false) String details) {
        model.addAttribute(DashboardConstants.ERROR_FIELD, error != null ? error : "An unexpected error occurred");
        model.addAttribute("errorDetails", details);
        model.addAttribute("basePath", basePath);
        return DashboardConstants.ERROR_TEMPLATE;
    }

    // LIVE DATA
    @GetMapping(DashboardConstants.API_LIVE_DATA_ENDPOINT)
    @ResponseBody
    public ResponseEntity<Map<String, Object>> getLiveData() {
        try {
            Map<String, Object> liveData = liveDataService.getLiveData();
            return ResponseEntity.ok(liveData);
        } catch (Exception e) {
            logger.error("Failed to fetch live data", e);
            return ResponseEntity.status(500).body(Map.of(
                    DashboardConstants.SUCCESS_FIELD, false,
                    DashboardConstants.ERROR_FIELD, "Failed to fetch live data: " + e.getMessage(),
                    DashboardConstants.TIMESTAMP_FIELD, Instant.now()
            ));
        }
    }

    // STATUS - Keep for backward compatibility if needed
    @GetMapping(DashboardConstants.API_STATUS_ENDPOINT)
    @ResponseBody
    public ResponseEntity<Map<String, Object>> getStatus() {
        try {
            Map<String, Object> status = statusService.getCurrentStatus();
            return ResponseEntity.ok(status);
        } catch (Exception e) {
            logger.error("Failed to get status", e);
            return ResponseEntity.status(500).body(Map.of(
                    DashboardConstants.SERVICE_RUNNING_ATTR, false,
                    DashboardConstants.ERROR_FIELD, e.getMessage(),
                    DashboardConstants.TIMESTAMP_FIELD, Instant.now()
            ));
        }
    }

    // CONTROL STATUS
    @GetMapping(DashboardConstants.API_CONTROL_STATUS_ENDPOINT)
    @ResponseBody
    public ResponseEntity<ControlStatusResponse> getControlStatus() {
        try {
            ControlStatusResponse response = controlService.getControlStatus();
            return ResponseEntity.ok(response);
        } catch (Exception e) {
            logger.error("Failed to get control status", e);
            return ResponseEntity.status(500).body(null);
        }
    }

    @PostMapping(DashboardConstants.API_ERRORS_CLEAR_ENDPOINT)
    @ResponseBody
    public ResponseEntity<Map<String, Object>> clearErrors() {
        try {
            Map<String, Object> response = liveDataService.clearErrors();
            return ResponseEntity.ok(response);
        } catch (Exception e) {
            logger.error("Failed to clear errors", e);
            return ResponseEntity.status(500).body(Map.of(
                    DashboardConstants.SUCCESS_FIELD, false,
                    DashboardConstants.MESSAGE_FIELD, DashboardConstants.FAILED_TO_CLEAR_ERRORS_PREFIX + e.getMessage(),
                    DashboardConstants.TIMESTAMP_FIELD, Instant.now()
            ));
        }
    }

    // ENGINE CONTROL
    @PostMapping(DashboardConstants.API_CONTROL_START_ENDPOINT)
    @ResponseBody
    public ResponseEntity<Map<String, Object>> startEngine() {
        try {
            logger.info("Starting engine");
            Map<String, Object> response = controlService.startSupervisor();
            return ResponseEntity.ok(response);
        } catch (Exception e) {
            logger.error("Failed to start engine", e);
            return ResponseEntity.status(500).body(createErrorResponse(DashboardConstants.FAILED_TO_START_PREFIX, e));
        }
    }

    @PostMapping(DashboardConstants.API_CONTROL_STOP_ENDPOINT)
    @ResponseBody
    public ResponseEntity<Map<String, Object>> stopEngine() {
        try {
            logger.info("Stopping engine");
            Map<String, Object> response = controlService.stopSupervisor();
            return ResponseEntity.ok(response);
        } catch (Exception e) {
            logger.error("Failed to stop engine", e);
            return ResponseEntity.status(500).body(createErrorResponse(DashboardConstants.FAILED_TO_STOP_PREFIX, e));
        }
    }

    @PostMapping(DashboardConstants.API_CONTROL_RESTART_ENDPOINT)
    @ResponseBody
    public ResponseEntity<Map<String, Object>> restartEngine() {
        try {
            logger.info("Restarting engine");
            Map<String, Object> response = controlService.restartSupervisor();
            return ResponseEntity.ok(response);
        } catch (Exception e) {
            logger.error("Failed to restart engine", e);
            return ResponseEntity.status(500).body(createErrorResponse(DashboardConstants.FAILED_TO_RESTART_PREFIX, e));
        }
    }

    @PostMapping(DashboardConstants.API_CONTROL_RESTART_FAILED_ENDPOINT)
    @ResponseBody
    public ResponseEntity<Map<String, Object>> restartFailedListeners() {
        try {
            logger.info("Restarting failed listeners");
            Map<String, Object> response = controlService.restartFailedListeners();
            return ResponseEntity.ok(response);
        } catch (Exception e) {
            logger.error("Failed to restart failed listeners", e);
            return ResponseEntity.status(500).body(createErrorResponse(DashboardConstants.FAILED_TO_RESTART_LISTENERS_PREFIX, e));
        }
    }

    // UTILITY ENDPOINTS
    @GetMapping("/health")
    @ResponseBody
    public ResponseEntity<Map<String, Object>> health() {
        try {
            Map<String, Object> health = Map.of(
                    "status", "UP",
                    DashboardConstants.TIMESTAMP_FIELD, Instant.now(),
                    "dashboard", "available"
            );
            return ResponseEntity.ok(health);
        } catch (Exception e) {
            logger.error("Health check failed", e);
            return ResponseEntity.status(500).body(Map.of(
                    "status", "DOWN",
                    DashboardConstants.TIMESTAMP_FIELD, Instant.now(),
                    DashboardConstants.ERROR_FIELD, e.getMessage()
            ));
        }
    }

    // HELPER METHODS
    private Map<String, Object> createErrorResponse(String messagePrefix, Exception e) {
        return Map.of(
                DashboardConstants.SUCCESS_FIELD, false,
                DashboardConstants.MESSAGE_FIELD, messagePrefix + e.getMessage(),
                DashboardConstants.ERROR_FIELD, e.getClass().getSimpleName(),
                DashboardConstants.TIMESTAMP_FIELD, Instant.now()
        );
    }

    private String handleDashboardError(Model model, Exception e, String userMessage) {
        model.addAttribute(DashboardConstants.ERROR_FIELD, userMessage);
        model.addAttribute("errorDetails", e.getClass().getSimpleName() + ": " + e.getMessage());
        model.addAttribute("basePath", basePath);
        model.addAttribute(DashboardConstants.REFRESH_TIME_ATTR, Instant.now());

        try {
            model.addAttribute(DashboardConstants.SERVICE_RUNNING_ATTR, false);
        } catch (Exception ignored) {
            // Ignore if we can't get basic status
        }

        return DashboardConstants.ERROR_TEMPLATE;
    }

    @ExceptionHandler(Exception.class)
    public String handleControllerException(Exception e, Model model, HttpServletRequest request) {
        logger.error("Unhandled exception in dashboard controller for request: {}", request.getRequestURI(), e);

        String acceptHeader = request.getHeader("Accept");
        if (acceptHeader != null && acceptHeader.contains("application/json")) {
            return "redirect:" + basePath + "/api/error?message=" + e.getMessage();
        }

        return handleDashboardError(model, e, "An unexpected error occurred");
    }
}