/**
 * Main Dashboard Controller
 * Coordinates all fragment modules and handles global functionality
 */
class DashboardController {
    constructor() {
        this.config = window.dbBrokerConfig;
        this.modules = {};
        this.refreshTimer = null;

        this.init();
    }

    async init() {
        console.log('Initializing Dashboard Controller');

        // Initialize all fragment modules
        await this.initializeModules();

        // Start auto-refresh
        this.startAutoRefresh();

        // Setup global event listeners
        this.setupGlobalEvents();
    }

    async initializeModules() {
        // Initialize each fragment module if it exists
        if (window.AlertsModule) {
            this.modules.alerts = new window.AlertsModule(this);
        }

        if (window.ControlsModule) {
            this.modules.controls = new window.ControlsModule(this);
        }

        if (window.ErrorsModule) {
            this.modules.errors = new window.ErrorsModule(this);
        }

        if (window.MetricsModule) {
            this.modules.metrics = new window.MetricsModule(this);
        }

        if (window.StatusModule) {
            this.modules.status = new window.StatusModule(this);
        }

        if (window.SystemModule) {
            this.modules.system = new window.SystemModule(this);
        }
    }

    async refreshLiveData() {
        try {
            const response = await fetch(window.buildApiUrl('/dashboard/live-data'));
            const data = await response.json();

            if (data.success) {
                // Update all modules with new data
                Object.values(this.modules).forEach(module => {
                    if (module.updateData) {
                        module.updateData(data);
                    }
                });

                this.updateLastRefreshTime();
            } else {
                console.error('Failed to fetch live data:', data.error);
            }
        } catch (error) {
            console.error('Error fetching live data:', error);
        }
    }

    startAutoRefresh() {
        if (this.refreshTimer) {
            clearInterval(this.refreshTimer);
        }

        this.refreshTimer = setInterval(() => {
            this.refreshLiveData();
        }, this.config.refreshInterval);
    }

    updateLastRefreshTime() {
        const now = new Date().toLocaleTimeString();
        const element = document.querySelector('[data-stat="last-update"]');
        if (element) {
            element.textContent = now;
        }
    }

    setupGlobalEvents() {
        // Global refresh button handler
        document.addEventListener('click', (e) => {
            if (e.target.matches('[data-refresh]')) {
                e.preventDefault();
                this.refreshLiveData();
            }
        });
    }

    // Utility method for modules to show status messages
    showStatusMessage(message, type = 'info') {
        if (this.modules.alerts) {
            this.modules.alerts.showStatus(message, type);
        }
    }
}

// Initialize when DOM is ready
document.addEventListener('DOMContentLoaded', () => {
    window.dashboardController = new DashboardController();
});