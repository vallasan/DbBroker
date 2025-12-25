/**
 * Controls Fragment Module
 * Handles engine control buttons
 */
window.ControlsModule = class ControlsModule {
    constructor(dashboard) {
        this.dashboard = dashboard;
        this.init();
    }

    init() {
        // Setup control button handlers
        document.addEventListener('click', (e) => {
            const action = e.target.closest('[data-action]')?.dataset.action;
            if (action) {
                e.preventDefault();
                this.handleControlAction(action, e.target.closest('[data-action]'));
            }
        });
    }

    async handleControlAction(action, button) {
        const originalText = button.innerHTML;
        const originalDisabled = button.disabled;

        try {
            // Show loading state
            button.disabled = true;
            button.innerHTML = '<i class="fas fa-spinner fa-spin"></i> Processing...';

            const endpoint = this.getControlEndpoint(action);
            const response = await fetch(window.buildApiUrl(endpoint), {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json'
                }
            });

            const result = await response.json();

            if (result.success) {
                this.dashboard.showStatusMessage(result.message || `${action} completed successfully`, 'success');
                // Refresh data to show updated state
                setTimeout(() => this.dashboard.refreshLiveData(), 1000);
            } else {
                this.dashboard.showStatusMessage(result.message || `Failed to ${action}`, 'danger');
            }

        } catch (error) {
            console.error(`Error executing ${action}:`, error);
            this.dashboard.showStatusMessage(`Error: ${error.message}`, 'danger');
        } finally {
            // Restore button state
            button.innerHTML = originalText;
            button.disabled = originalDisabled;
        }
    }

    getControlEndpoint(action) {
        const endpoints = {
            'start': '/control/start',
            'stop': '/control/stop',
            'restart': '/control/restart',
            'restart-failed': '/control/restart-failed'
        };
        return endpoints[action] || '/control/start';
    }

    updateData(data) {
        // Store the full data object
        this.currentData = data;

        // Update control button states based on live data
        if (data.engineStatus) {
            this.updateControlButtons(data.engineStatus);
        }
    }

    updateControlButtons(engineStatus) {
        const isRunning = engineStatus.running;
        const canStop = engineStatus.canStop;
        const canRestart = engineStatus.canRestart;
        const canRestartFailed = engineStatus.canRestartFailed;
        const failedListeners = engineStatus.failedListeners || 0;

        const stopBtn = document.querySelector('[data-action="stop"]');
        if (stopBtn) {
            // Only enable when engine is running AND can stop
            stopBtn.disabled = !isRunning || !canStop;
        }

        const restartBtn = document.querySelector('[data-action="restart"]');
        if (restartBtn) {
            if (isRunning) {
                // Engine running: Enable if can restart
                restartBtn.disabled = !canRestart;
            } else {
                // Engine stopped: Always enable (primary way to start)
                restartBtn.disabled = false;
            }
        }

        const restartFailedBtn = document.querySelector('[data-action="restart-failed"]');
        if (restartFailedBtn) {
            const shouldDisable = !isRunning || !canRestartFailed || failedListeners === 0;
            restartFailedBtn.disabled = shouldDisable;

            // Update the button text with current failed count
            const span = restartFailedBtn.querySelector('span');
            if (span) {
                span.textContent = `Fix Failed (${failedListeners})`;
            }

            // Update button class based on failed listeners
            if (failedListeners > 0) {
                restartFailedBtn.className = 'btn btn-warning';
            } else {
                restartFailedBtn.className = 'btn btn-info';
            }
        }

        const refreshButtons = document.querySelectorAll('[data-refresh]');
        refreshButtons.forEach(btn => {
            btn.disabled = false;
        });

        const clearErrorsBtn = document.querySelector('[data-clear-errors]');
        if (clearErrorsBtn && this.currentData && this.currentData.errorInfo) {
            clearErrorsBtn.disabled = !this.currentData.errorInfo.hasErrors;
        } else if (clearErrorsBtn) {
            clearErrorsBtn.disabled = true;
        }
    }
};