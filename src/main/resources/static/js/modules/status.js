/**
 * Status Fragment Module
 * Handles service status display
 */
window.StatusModule = class StatusModule {
    constructor(dashboard) {
        this.dashboard = dashboard;
    }

    updateData(data) {
        if (data.engineStatus) {
            this.updateEngineStatus(data.engineStatus);
        }
        if (data.systemInfo) {
            this.updateSystemInfo(data.systemInfo);
        }
    }

    updateEngineStatus(engineStatus) {
        // Update status badge
        const statusBadge = document.querySelector('[data-element="status-badge"]');
        if (statusBadge) {
            statusBadge.className = engineStatus.running ? 'badge bg-success me-2' : 'badge bg-danger me-2';
        }

        // Update status text
        const statusText = document.querySelector('[data-element="status-text"]');
        if (statusText) {
            statusText.textContent = engineStatus.running ? 'ENGINE RUNNING' : 'ENGINE STOPPED';
        }

        // Update system summary
        const systemSummary = document.querySelector('[data-element="system-summary"]');
        if (systemSummary) {
            systemSummary.textContent = engineStatus.summary || 'System initializing...';
        }
    }

    updateSystemInfo(systemInfo) {
        // Update queue name
        const queueNameEl = document.querySelector('[data-info="queue-name"]');
        if (queueNameEl) {
            queueNameEl.textContent = systemInfo.queueName || 'Not configured';
        }

        // Update engine status
        const engineStatusEl = document.querySelector('[data-info="engine-status"]');
        if (engineStatusEl) {
            engineStatusEl.textContent = systemInfo.engineRunning ? 'Running' : 'Stopped';
            engineStatusEl.className = systemInfo.engineRunning ? 'text-success' : 'text-danger';
        }
    }
};