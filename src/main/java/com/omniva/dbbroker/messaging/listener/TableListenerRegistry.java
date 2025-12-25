package com.omniva.dbbroker.messaging.listener;

import jakarta.annotation.PostConstruct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationContext;
import org.springframework.stereotype.Component;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Component
public class TableListenerRegistry {

    private static final Logger log = LoggerFactory.getLogger(TableListenerRegistry.class);
    private final ApplicationContext applicationContext;
    private final Map<String, TableListenerRegistryRecord> listeners = new ConcurrentHashMap<>();

    public TableListenerRegistry(ApplicationContext applicationContext) {
        this.applicationContext = applicationContext;
    }

    @PostConstruct
    public void discoverAndRegisterListeners() {
        log.info("Starting table listener discovery...");

        Map<String, Object> annotatedBeans = applicationContext.getBeansWithAnnotation(TableChangeListener.class);

        int registeredCount = 0;
        for (Map.Entry<String, Object> entry : annotatedBeans.entrySet()) {
            String beanName = entry.getKey();
            Object bean = entry.getValue();

            if (bean instanceof TableListener) {
                try {
                    registerListener(beanName, (TableListener) bean);
                    registeredCount++;
                } catch (Exception e) {
                    log.error("Failed to register table listener {}: {}", beanName, e.getMessage(), e);
                }
            } else {
                log.warn("Bean {} is annotated with @TableChangeListener but doesn't implement TableListener", beanName);
            }
        }

        log.info("Successfully registered {} table listeners", registeredCount);

        // Log all registered listeners
        listeners.forEach((table, registration) ->
                log.info("  {} -> {} (events: {}, recordType: {}, enabled: {})",
                        table,
                        registration.beanName(),
                        Arrays.toString(registration.config().events()),
                        registration.config().recordType().getSimpleName(),
                        registration.config().enabled())
        );
    }

    private void registerListener(String beanName, TableListener listener) {
        Class<?> listenerClass = listener.getClass();
        TableChangeListener annotation = listenerClass.getAnnotation(TableChangeListener.class);

        if (annotation == null) {
            log.warn("TableListener {} missing @TableChangeListener annotation", beanName);
            return;
        }

        String tableName = annotation.table().toUpperCase().trim();

        if (tableName.isEmpty()) {
            log.error("TableListener {} has empty table name", beanName);
            return;
        }

        // Check for duplicate registrations
        if (listeners.containsKey(tableName)) {
            log.error("Duplicate table listener registration for table {}: {} conflicts with {}",
                    tableName, beanName, listeners.get(tableName).beanName());
            return;
        }

        // Create registration
        TableListenerRegistryRecord registration = new TableListenerRegistryRecord(
                tableName,
                listener,
                annotation,
                beanName
        );

        if (!registration.isEnabled()) {
            log.warn("TableListener {} is disabled", tableName);
            return;
        }

        // Register
        listeners.put(tableName, registration);

        // Notify listener it was registered (with empty config map)
        try {
            listener.validateSetup();
            listener.onRegistered(tableName);
        } catch (Exception e) {
            log.error("Error during listener registration for table {}: {}", tableName, e.getMessage(), e);
            // Remove failed registration
            listeners.remove(tableName);
            throw e;
        }
    }

    // ===== PUBLIC API METHODS =====

    public TableListenerRegistryRecord get(String tableName) {
        return listeners.get(tableName.toUpperCase());
    }

    public Map<String, TableListenerRegistryRecord> getAllListeners() {
        return new HashMap<>(listeners);
    }

    public int getListenerCount() {
        return listeners.size();
    }

}