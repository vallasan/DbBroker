/**
 * System Fragment Module
 * Handles system information and listeners display
 */
window.SystemModule = class SystemModule {
    constructor(dashboard) {
        this.dashboard = dashboard;
    }

    updateData(data) {
        if (data.listeners) {
            this.updateListeners(data.listeners);
        }
        if (data.systemInfo) {
            this.updateSystemInfo(data.systemInfo);
        }
    }

    updateListeners(listeners) {
        const listenerTable = document.querySelector('[data-table="listeners"] tbody');
        const listenerCount = document.querySelector('[data-stat="listener-count"]');

        if (!listenerTable) return;

        // Update listener count
        if (listenerCount) {
            listenerCount.textContent = listeners.length.toString();
        }

        // Clear existing rows
        listenerTable.innerHTML = '';

        // Add listener rows
        listeners.forEach(listener => {
            const row = this.createListenerRow(listener);
            listenerTable.appendChild(row);
        });

        // Show/hide no listeners message
        const noListenersMsg = document.querySelector('.text-center.text-muted');
        if (noListenersMsg) {
            noListenersMsg.style.display = listeners.length > 0 ? 'none' : 'block';
        }
    }

    createListenerRow(listener) {
        const row = document.createElement('tr');

        row.innerHTML = `
            <td><code>${listener.listenerName || 'N/A'}</code></td>
            <td>${listener.queueName || 'Unknown'}</td>
            <td>
                <span class="badge ${this.getStatusBadgeClass(listener.status)}">
                    ${listener.status || 'UNKNOWN'}
                </span>
            </td>
            <td>${listener.messagesPerSecond || '0.00 msg/sec'}</td>
            <td>${listener.errorRate || '0.0%'}</td>
            <td>
                <span class="badge ${this.getGradeBadgeClass(listener.grade)}">
                    ${listener.grade || 'F'}
                </span>
            </td>
            <td class="text-muted small">${listener.uptime || '00:00:00'}</td>
            <td>
                <span class="badge ${this.getActivityBadgeClass(listener.hasRecentActivity)}">
                    ${listener.hasRecentActivity ? 'Active' : 'Idle'}
                </span>
            </td>
        `;

        return row;
    }

    getStatusBadgeClass(status) {
        const statusClasses = {
            'HEALTHY': 'bg-success',
            'DEGRADED': 'bg-warning',
            'STOPPED': 'bg-danger',
            'IDLE': 'bg-secondary'
        };
        return statusClasses[status] || 'bg-secondary';
    }

    getGradeBadgeClass(grade) {
        const gradeClasses = {
            'A': 'bg-success',
            'B': 'bg-info',
            'C': 'bg-warning',
            'D': 'bg-warning',
            'F': 'bg-danger'
        };
        return gradeClasses[grade] || 'bg-danger';
    }

    getActivityBadgeClass(hasActivity) {
        return hasActivity ? 'bg-success' : 'bg-secondary';
    }

    updateSystemInfo(systemInfo) {
        // Update queue name in system details
        const queueNameEl = document.querySelector('[data-info="queue-name"]');
        if (queueNameEl) {
            queueNameEl.textContent = systemInfo.queueName || 'Not configured';
        }

        // Update engine status in system details
        const engineStatusEl = document.querySelector('[data-info="engine-status"]');
        if (engineStatusEl) {
            engineStatusEl.textContent = systemInfo.engineRunning ? 'Running' : 'Stopped';
            engineStatusEl.className = systemInfo.engineRunning ? 'text-success' : 'text-danger';
        }
    }
};