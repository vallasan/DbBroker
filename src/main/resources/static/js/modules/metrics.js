/**
 * Metrics Fragment Module
 * Handles performance metrics display
 */
window.MetricsModule = class MetricsModule {
    constructor(dashboard) {
        this.dashboard = dashboard;
    }

    updateData(data) {
        if (data.metrics) {
            this.updateMetrics(data.metrics);
        }
    }

    updateMetrics(metrics) {
        // Update throughput
        const throughputEl = document.querySelector('[data-metric="throughput"]');
        if (throughputEl) {
            throughputEl.textContent = metrics.throughput || '0.00 msg/sec';
        }

        // Update error rate
        const errorRateEl = document.querySelector('[data-metric="error-rate"]');
        if (errorRateEl) {
            errorRateEl.textContent = metrics.errorRate || '0.00%';
        }

        // Update listeners
        const listenersEl = document.querySelector('[data-metric="listeners"]');
        if (listenersEl) {
            listenersEl.innerHTML = `${metrics.activeListeners || 0}/${metrics.totalListeners || 0}`;
        }

        const failedListenersEl = document.querySelector('[data-metric="failed-listeners"]');
        if (failedListenersEl) {
            failedListenersEl.textContent = metrics.failedListeners || 0;
        }

        // Update uptime
        const uptimeEl = document.querySelector('[data-metric="uptime"]');
        if (uptimeEl) {
            uptimeEl.textContent = metrics.uptime || '00:00:00';
        }

        // Update metric times
        const now = new Date().toLocaleTimeString();
        document.querySelectorAll('[data-metric-time]').forEach(el => {
            if (el.dataset.metricTime !== 'uptime') {
                el.textContent = `Updated: ${now}`;
            }
        });
    }
};