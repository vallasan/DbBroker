package com.omniva.dbbroker.engine.fuelsystem;

import com.omniva.dbbroker.engine.fault.DbBrokerFatalError;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class DbBrokerConnectionManager {

    private static final Logger log = LoggerFactory.getLogger(DbBrokerConnectionManager.class);

    private final DataSource dataSource;

    @Autowired
    public DbBrokerConnectionManager(DataSource dataSource) {
        this.dataSource = dataSource;
    }

    /**
     * Creates and configures a new database connection for a listener thread.
     */
    public Connection createConnection() {
        try {
            Connection connection = dataSource.getConnection();
            connection.setAutoCommit(false);
            configureConnection(connection);
            return connection;
        } catch (SQLException e) {
            throw new DbBrokerFatalError(
                    "Critical database connection failure: " + e.getMessage(), e);
        }
    }

    /**
     * Configures connection settings for optimal Service Broker performance.
     */
    private void configureConnection(Connection connection) throws SQLException {
        connection.setAutoCommit(false);
    }

    /**
     * Safely cancels a statement (interrupts WAITFOR)
     */
    public void safeCancelStatement(PreparedStatement statement, String threadName) {
        if (statement != null) {
            try {
                statement.cancel();
            } catch (SQLException e) {
                log.warn("Error cancelling statement for listener {}: {}", threadName, e.getMessage());
            }
        }
    }

    /**
     * Safely closes a statement
     */
    public void safeCloseStatement(PreparedStatement statement, String threadName) {
        if (statement != null) {
            try {
                statement.close();
            } catch (SQLException e) {
                log.warn("Error closing statement for listener {}: {}", threadName, e.getMessage());
            }
        }
    }

    /**
     * Safely rolls back a transaction.
     */
    public void safeRollback(Connection connection, String threadName) {
        if (connection != null) {
            try {
                connection.rollback();
            } catch (SQLException rollbackError) {
                log.warn("Error rolling back transaction for listener {}: {}",
                        threadName, rollbackError.getMessage());
            }
        }
    }

    /**
     * Safely closes a connection.
     */
    public void safeClose(Connection connection, String threadName) {
        if (connection != null) {
            try {
                if (!connection.isClosed()) {
                    connection.close();
                }
            } catch (SQLException closeError) {
                log.warn("Error closing connection for listener {}: {}",
                        threadName, closeError.getMessage());
            }
        }
    }

    /**
     * Safely closes connection with rollback (for shutdown scenarios)
     */
    public void safeCloseWithRollback(Connection connection, String threadName) {
        if (connection != null) {
            try {
                if (!connection.isClosed()) {
                    safeRollback(connection, threadName);
                    connection.close();
                }
            } catch (SQLException closeError) {
                log.warn("Error closing connection with rollback for listener {}: {}",
                        threadName, closeError.getMessage());
            }
        }
    }

    /**
     * Interrupt listener operations (cancel statement, close connection)
     */
    public void interruptListenerOperations(PreparedStatement statement, Connection connection, String threadName) {
        // Step 1: Cancel statement to interrupt WAITFOR
        safeCancelStatement(statement, threadName);

        // Step 2: Close connection to break the listener loop
        safeClose(connection, threadName);
    }

    /**
     * Clean shutdown of listener resources
     */
    public void cleanupListenerResources(PreparedStatement statement, Connection connection, String threadName) {
        // Step 1: Close statement
        safeCloseStatement(statement, threadName);

        // Step 2: Close connection with rollback
        safeCloseWithRollback(connection, threadName);
    }

    /**
     * Checks if Service Broker is enabled for the database.
     */
    public boolean isServiceBrokerEnabled() {
        String checkSql = "SELECT is_broker_enabled FROM sys.databases WHERE name = DB_NAME()";

        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(checkSql);
             ResultSet rs = ps.executeQuery()) {

            if (rs.next()) {
                int enabled = rs.getInt("is_broker_enabled");
                if (enabled == 0) {
                    log.warn("Service Broker is DISABLED for database.");
                    throw new DbBrokerFatalError("Service Broker is disabled on database");
                } else {
                    log.info("Service Broker is enabled for database.");
                    return true;
                }
            } else {
                log.error("Could not determine Service Broker status.");
                throw new DbBrokerFatalError("Cannot determine Service Broker status - database access failed");
            }
        } catch (SQLException e) {
            throw new DbBrokerFatalError("Critical database error checking Service Broker status", e);
        }
    }

    /**
     * Checks if a specific queue is enabled for receiving messages.
     */
    public boolean isQueueEnabled(String queueName) {
        String checkSql = "SELECT is_receive_enabled FROM sys.service_queues WHERE name = ?";

        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(checkSql)) {

            ps.setString(1, queueName);
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    int enabled = rs.getInt("is_receive_enabled");
                    if (enabled == 0) {
                        log.warn("Queue '{}' is DISABLED. Listener will not start.", queueName);
                        throw new DbBrokerFatalError("Service Broker queue '" + queueName + "' is disabled");
                    } else {
                        log.info("Queue '{}' is enabled.", queueName);
                        return true;
                    }
                } else {
                    log.error("Queue '{}' not found in sys.service_queues.", queueName);
                    throw new DbBrokerFatalError("Service Broker queue '" + queueName + "' not found");
                }
            }
        } catch (SQLException e) {
            throw new DbBrokerFatalError("Critical database error checking queue status", e);
        }
    }
}