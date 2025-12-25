package com.omniva.dbbroker.messaging.listener;

import org.springframework.stereotype.Component;

import java.lang.annotation.*;
import java.util.Map;

/**
 * Annotation to automatically register table listeners
 * Combines @Component with table configuration mapping
 */
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
@Documented
@Component
public @interface TableChangeListener {

    /**
     * Table name this listener handles (case-insensitive)
     */
    String table();

    /**
     * DTO class for parsing record data
     * Defaults to Map (raw Map<String, Object>)
     */
    Class<?> recordType() default Map.class;

    /**
     * Supported events (default: all events)
     */
    String[] events() default {"INSERT", "UPDATE", "DELETE"};

    /**
     * Whether this listener is enabled by default
     */
    boolean enabled() default true;

}