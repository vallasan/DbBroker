/**
 * Alerts Fragment Module
 * Handles status messages and error displays
 */
window.AlertsModule = class AlertsModule {
    constructor(dashboard) {
        this.dashboard = dashboard;
        this.statusContainer = document.getElementById('statusMessages');
        this.statusAlert = document.getElementById('statusAlert');
        this.statusMessage = document.getElementById('statusMessage');

        this.init();
    }

    init() {
        // Setup close button handler
        const closeBtn = this.statusAlert?.querySelector('.btn-close');
        if (closeBtn) {
            closeBtn.addEventListener('click', () => this.hideStatus());
        }
    }

    showStatus(message, type = 'info') {
        if (!this.statusContainer || !this.statusAlert || !this.statusMessage) {
            return;
        }

        // Update message
        this.statusMessage.textContent = message;

        // Update alert class
        this.statusAlert.className = `alert alert-${type} alert-dismissible fade show`;

        // Update icon
        const icon = this.statusAlert.querySelector('i');
        if (icon) {
            icon.className = this.getIconClass(type);
        }

        // Show container
        this.statusContainer.style.display = 'block';

        // Auto-hide after 5 seconds for success messages
        if (type === 'success') {
            setTimeout(() => this.hideStatus(), 5000);
        }
    }

    hideStatus() {
        if (this.statusContainer) {
            this.statusContainer.style.display = 'none';
        }
    }

    getIconClass(type) {
        const iconMap = {
            'success': 'fas fa-check-circle',
            'danger': 'fas fa-exclamation-triangle',
            'warning': 'fas fa-exclamation-triangle',
            'info': 'fas fa-info-circle'
        };
        return iconMap[type] || 'fas fa-info-circle';
    }
};

// Make hideStatusMessage globally available for onclick handlers
window.hideStatusMessage = function() {
    if (window.dashboardController?.modules?.alerts) {
        window.dashboardController.modules.alerts.hideStatus();
    }
};