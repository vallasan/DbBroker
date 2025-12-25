/**
 * Errors Fragment Module
 * Handles error display and management
 */
window.ErrorsModule = class ErrorsModule {
    constructor(dashboard) {
        this.dashboard = dashboard;
        this.init();

        // Add a small delay to avoid race conditions
        setTimeout(() => {
            this.initializeVisibility();
        }, 100);
    }

    init() {
        // Setup clear errors button
        document.addEventListener('click', (e) => {
            if (e.target.closest('[data-clear-errors]')) {
                e.preventDefault();
                this.clearErrors();
            }
        });
    }

    initializeVisibility() {
        const errorTableBody = document.querySelector('[data-error-table-body]');
        const hasServerErrors = errorTableBody && errorTableBody.children.length > 0;

        const noErrorsSection = document.querySelector('[data-section="no-errors"]');
        const errorSummary = document.querySelector('[data-section="error-summary"]');
        const errorList = document.querySelector('[data-section="error-list"]');
        const errorStats = document.querySelector('[data-section="error-statistics"]');

        if (hasServerErrors) {
            if (noErrorsSection) {
                noErrorsSection.classList.add('error-section-hidden');
                noErrorsSection.classList.remove('error-section-visible');
            }
            [errorSummary, errorList, errorStats].forEach(section => {
                if (section) {
                    section.classList.add('error-section-visible');
                    section.classList.remove('error-section-hidden');
                }
            });
        } else {
            if (noErrorsSection) {
                noErrorsSection.classList.add('error-section-visible');
                noErrorsSection.classList.remove('error-section-hidden');
            }
            [errorSummary, errorList, errorStats].forEach(section => {
                if (section) {
                    section.classList.add('error-section-hidden');
                    section.classList.remove('error-section-visible');
                }
            });
        }
    }

    async clearErrors() {
        try {
            this.dashboard.showStatusMessage('Clearing errors...', 'info');

            const response = await fetch(window.buildApiUrl('/errors/clear'), {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json'
                }
            });

            const result = await response.json();

            if (result.success) {
                this.dashboard.showStatusMessage('Errors cleared successfully', 'success');
                if (result.errorInfo) {
                    this.updateData(result);
                }
            } else {
                this.dashboard.showStatusMessage(result.message || 'Failed to clear errors', 'danger');
            }

        } catch (error) {
            this.dashboard.showStatusMessage('Error clearing errors: ' + error.message, 'danger');
        }
    }

    updateData(data) {
        if (data.errorInfo) {
            this.updateErrorInfo(data.errorInfo);
        }
    }

    updateErrorInfo(errorInfo) {
        // Update error badge
        const badge = document.querySelector('[data-error="badge"]');
        if (badge) {
            badge.textContent = errorInfo.totalCount || 0;
            badge.className = this.getErrorBadgeClass(errorInfo);
        }

        // Update error count
        const countEl = document.querySelector('[data-error="count"]');
        if (countEl) {
            countEl.textContent = errorInfo.totalCount || 0;
        }

        // Update last error
        const lastErrorEl = document.querySelector('[data-error="last"]');
        if (lastErrorEl) {
            lastErrorEl.textContent = this.truncateMessage(errorInfo.lastError || '');
        }

        // Update error statistics
        this.updateErrorStatistics(errorInfo.errorStatistics || {});

        this.updateErrorList(errorInfo);

        this.updateSectionVisibility(errorInfo);

        // Update clear button state
        const clearBtn = document.querySelector('[data-clear-errors]');
        if (clearBtn) {
            clearBtn.disabled = !errorInfo.hasErrors;
        }
    }

    updateErrorStatistics(errorStatistics) {
        const {
            totalErrors = 0,
            criticalErrors = 0,
            sqlErrors = 0,
            poisonMessages = 0
        } = errorStatistics;

        const totalEl = document.querySelector('[data-error-stat="general"]');
        if (totalEl) totalEl.textContent = totalErrors.toString();

        const criticalEl = document.querySelector('[data-error-stat="critical"]');
        if (criticalEl) criticalEl.textContent = criticalErrors.toString();

        const sqlEl = document.querySelector('[data-error-stat="sql"]');
        if (sqlEl) sqlEl.textContent = sqlErrors.toString();

        const poisonEl = document.querySelector('[data-error-stat="poison"]');
        if (poisonEl) poisonEl.textContent = poisonMessages.toString();
    }

    updateSectionVisibility(errorInfo) {
        const hasErrors = errorInfo.hasErrors;

        const noErrorsSection = document.querySelector('[data-section="no-errors"]');
        const errorSummary = document.querySelector('[data-section="error-summary"]');
        const errorList = document.querySelector('[data-section="error-list"]');
        const errorStats = document.querySelector('[data-section="error-statistics"]');

        if (hasErrors) {
            if (noErrorsSection) {
                noErrorsSection.classList.add('error-section-hidden');
                noErrorsSection.classList.remove('error-section-visible');
            }

            // Show all error sections
            [errorSummary, errorList, errorStats].forEach(section => {
                if (section) {
                    section.classList.add('error-section-visible');
                    section.classList.remove('error-section-hidden');
                }
            });
        } else {
            if (noErrorsSection) {
                noErrorsSection.classList.add('error-section-visible');
                noErrorsSection.classList.remove('error-section-hidden');
            }

            // Hide all error sections
            [errorSummary, errorList, errorStats].forEach(section => {
                if (section) {
                    section.classList.add('error-section-hidden');
                    section.classList.remove('error-section-visible');
                }
            });
        }
    }

    /**
     * Only update table content when there are errors
     */
    updateErrorList(errorInfo) {
        const errorListSection = document.querySelector('[data-section="error-list"]');
        if (!errorListSection) return;

        const tableBody = errorListSection.querySelector('[data-error-table-body]');
        if (!tableBody) return;

        if (errorInfo.hasErrors) {
            // Clear existing rows
            tableBody.innerHTML = '';

            const errors = errorInfo.formattedErrors || [];
            const rawErrors = errorInfo.recentErrors || [];

            if (errors.length > 0) {
                errors.forEach((error, index) => {
                    const row = this.createFormattedErrorRow(error, index + 1);
                    tableBody.appendChild(row);
                });
            } else if (rawErrors.length > 0) {
                rawErrors.forEach((error, index) => {
                    const row = this.createRawErrorRow(error, index + 1);
                    tableBody.appendChild(row);
                });
            }
        }
    }

    createFormattedErrorRow(error, index) {
        const row = document.createElement('tr');
        row.className = this.getErrorRowClass(error.severity);

        row.innerHTML = `
            <td class="text-muted small">${index}</td>
            <td class="small font-monospace">${error.timestamp || 'N/A'}</td>
            <td class="small">
                <span title="${this.escapeHtml(error.fullMessage || error.mainMessage)}" style="cursor: help;">
                    ${this.escapeHtml(error.shortMessage || error.mainMessage || 'Unknown error')}
                </span>
                ${error.hasException ? '<i class="fas fa-bug text-warning ms-1" title="Has exception details"></i>' : ''}
            </td>
            <td>
                <span class="badge ${this.getSeverityBadgeClass(error.severity)}">
                    ${error.severity || 'ERROR'}
                </span>
            </td>
        `;

        return row;
    }

    createRawErrorRow(error, index) {
        const row = document.createElement('tr');
        const severity = this.detectSeverity(error);
        row.className = this.getErrorRowClass(severity);

        const timestamp = this.extractTimestamp(error);
        const message = this.extractMessage(error);

        row.innerHTML = `
            <td class="text-muted small">${index}</td>
            <td class="small font-monospace">${timestamp || 'N/A'}</td>
            <td class="small">
                <span title="${this.escapeHtml(error)}" style="cursor: help;">
                    ${this.escapeHtml(message)}
                </span>
            </td>
            <td>
                <span class="badge ${this.getSeverityBadgeClass(severity)}">
                    ${severity}
                </span>
            </td>
        `;

        return row;
    }

    // Helper methods
    getErrorRowClass(severity) {
        const classMap = {
            'CRITICAL': 'table-danger',
            'POISON': 'table-warning',
            'SQL': 'table-info',
            'ERROR': 'table-light'
        };
        return classMap[severity] || 'table-light';
    }

    getSeverityBadgeClass(severity) {
        const classMap = {
            'CRITICAL': 'bg-danger',
            'POISON': 'bg-warning',
            'SQL': 'bg-info',
            'ERROR': 'bg-secondary'
        };
        return classMap[severity] || 'bg-secondary';
    }

    detectSeverity(error) {
        const lowerError = error.toLowerCase();
        if (lowerError.includes('critical') || lowerError.includes('fatal')) {
            return 'CRITICAL';
        } else if (lowerError.includes('poison')) {
            return 'POISON';
        } else if (lowerError.includes('sql')) {
            return 'SQL';
        } else {
            return 'ERROR';
        }
    }

    extractTimestamp(error) {
        if (error.startsWith('[') && error.length > 21) {
            const closingBracket = error.indexOf(']');
            if (closingBracket === 20) {
                return error.substring(1, closingBracket);
            }
        }
        return '';
    }

    extractMessage(error) {
        let message = error;

        // Remove timestamp
        if (error.startsWith('[')) {
            const closingBracket = error.indexOf(']');
            if (closingBracket > 0) {
                message = error.substring(closingBracket + 1).trim();
            }
        }

        // Truncate if too long
        if (message.length > 100) {
            return message.substring(0, 97) + '...';
        }

        return message;
    }

    truncateMessage(message) {
        if (!message) return '';
        if (message.length <= 100) return message;
        return message.substring(0, 97) + '...';
    }

    escapeHtml(text) {
        if (!text) return '';
        const div = document.createElement('div');
        div.textContent = text;
        return div.innerHTML;
    }

    getErrorBadgeClass(errorInfo) {
        if (errorInfo.hasHighErrorCount) {
            return 'badge bg-danger';
        } else if (errorInfo.hasErrors) {
            return 'badge bg-warning';
        } else {
            return 'badge bg-success';
        }
    }
};