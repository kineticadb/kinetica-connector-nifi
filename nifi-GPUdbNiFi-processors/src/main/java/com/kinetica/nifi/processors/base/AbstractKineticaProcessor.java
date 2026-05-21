package com.kinetica.nifi.processors.base;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.lifecycle.OnStopped;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.PropertyValue;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;

import com.gpudb.GPUdb;
import com.gpudb.GPUdbBase.Options;
import com.gpudb.GPUdbException;
import com.gpudb.WorkerList;

/**
 * Abstract base class for all Kinetica NiFi processors.
 *
 * <p>This class provides common functionality shared across all Kinetica processors:
 * <ul>
 *   <li>GPUdb connection management with connection pooling</li>
 *   <li>SSL/TLS support for secure connections</li>
 *   <li>Common property descriptors (server URL, credentials, table name)</li>
 *   <li>Table name validation to prevent SQL injection</li>
 *   <li>Connection timeout configuration</li>
 *   <li>Resource cleanup on processor stop</li>
 * </ul>
 *
 * <p>Subclasses should:
 * <ul>
 *   <li>Call {@code super.onScheduled(context)} at the start of their onScheduled method</li>
 *   <li>Call {@code super.onStopped()} at the end of their onStopped method</li>
 *   <li>Use {@code getGpudb()} to access the connection</li>
 * </ul>
 *
 * @author Kinetica Engineering
 * @version 7.2.0.0
 * @since 7.2.0.0
 */
public abstract class AbstractKineticaProcessor extends AbstractProcessor {

    // ========== COMMON PROPERTY DESCRIPTORS ==========

    public static final PropertyDescriptor PROP_SERVER = new PropertyDescriptor.Builder()
            .name("Server URL")
            .displayName("Server URL")
            .description("URL of the Kinetica server. Example: http://172.3.4.19:9191")
            .required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.URL_VALIDATOR)
            .build();

    public static final PropertyDescriptor PROP_TABLE = new PropertyDescriptor.Builder()
            .name("Table Name")
            .displayName("Table Name")
            .description("Name of the Kinetica table. Can include schema prefix (e.g., 'schema.table_name').")
            .required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor PROP_USERNAME = new PropertyDescriptor.Builder()
            .name("Username")
            .displayName("Username")
            .description("Username for Kinetica authentication. Leave empty if authentication is not required.")
            .required(false)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor PROP_PASSWORD = new PropertyDescriptor.Builder()
            .name("Password")
            .displayName("Password")
            .description("Password for Kinetica authentication.")
            .required(false)
            .sensitive(true)
            // Note: Password does NOT support expression language to prevent credential exposure in logs
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    // ========== SSL/TLS PROPERTY DESCRIPTORS ==========

    public static final PropertyDescriptor PROP_USE_SSL = new PropertyDescriptor.Builder()
            .name("Use SSL/TLS")
            .displayName("Use SSL/TLS")
            .description("Enable SSL/TLS for secure connections to Kinetica. " +
                    "When enabled, use https:// in the Server URL.")
            .required(false)
            .defaultValue("false")
            .allowableValues("true", "false")
            .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
            .build();

    public static final PropertyDescriptor PROP_SSL_BYPASS_CERT_CHECK = new PropertyDescriptor.Builder()
            .name("Bypass SSL Certificate Check")
            .displayName("Bypass SSL Certificate Check")
            .description("If true, bypasses SSL certificate verification. " +
                    "WARNING: Use only for development/testing with self-signed certificates. " +
                    "Do not use in production.")
            .required(false)
            .defaultValue("false")
            .allowableValues("true", "false")
            .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
            .build();

    // ========== CONNECTION PROPERTY DESCRIPTORS ==========

    public static final PropertyDescriptor PROP_CONNECTION_TIMEOUT = new PropertyDescriptor.Builder()
            .name("Connection Timeout")
            .displayName("Connection Timeout")
            .description("Maximum time to wait for a connection to be established. " +
                    "Use time unit suffix: ms, sec, min (e.g., '30 sec').")
            .required(false)
            .defaultValue("30 sec")
            .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
            .build();

    public static final PropertyDescriptor PROP_SOCKET_TIMEOUT = new PropertyDescriptor.Builder()
            .name("Socket Timeout")
            .displayName("Socket Timeout")
            .description("Maximum time to wait for data on a socket. " +
                    "Use time unit suffix: ms, sec, min (e.g., '60 sec').")
            .required(false)
            .defaultValue("60 sec")
            .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
            .build();

    public static final PropertyDescriptor PROP_CONNECTION_POOL_SIZE = new PropertyDescriptor.Builder()
            .name("Connection Pool Size")
            .displayName("Connection Pool Size")
            .description("Maximum number of connections to maintain in the connection pool. " +
                    "Higher values improve throughput for concurrent operations.")
            .required(false)
            .defaultValue("4")
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .build();

    // ========== TABLE NAME VALIDATION ==========

    /**
     * Pattern for validating Kinetica table names.
     * Valid formats:
     * - Simple: "table_name"
     * - Schema-qualified: "schema.table_name"
     * Each part must start with a letter and contain only letters, numbers, and underscores.
     */
    private static final Pattern VALID_TABLE_NAME_PATTERN = Pattern.compile(
            "^[a-zA-Z][a-zA-Z0-9_]*(\\.[a-zA-Z][a-zA-Z0-9_]*)?$"
    );

    private static final int MAX_TABLE_NAME_LENGTH = 256;

    // ========== SHARED STATE ==========

    protected volatile GPUdb gpudb;
    protected volatile WorkerList workers;
    protected volatile String tableName;

    // Connection configuration (cached from properties)
    protected volatile boolean useSSL;
    protected volatile boolean bypassCertCheck;
    protected volatile int connectionTimeout;
    protected volatile int socketTimeout;
    protected volatile int connectionPoolSize;

    // Connection pool for reuse across FlowFiles with different attribute values
    private final Map<String, GPUdb> connectionPool = new ConcurrentHashMap<>();
    private static final int MAX_POOL_SIZE = 10;

    // ========== BASE PROPERTY DESCRIPTORS ==========

    /**
     * Returns the base property descriptors common to all Kinetica processors.
     * Subclasses should call this method and add their specific descriptors.
     *
     * @return List of base property descriptors
     */
    protected List<PropertyDescriptor> getBasePropertyDescriptors() {
        List<PropertyDescriptor> descriptors = new ArrayList<>();
        descriptors.add(PROP_SERVER);
        descriptors.add(PROP_TABLE);
        descriptors.add(PROP_USERNAME);
        descriptors.add(PROP_PASSWORD);
        descriptors.add(PROP_USE_SSL);
        descriptors.add(PROP_SSL_BYPASS_CERT_CHECK);
        descriptors.add(PROP_CONNECTION_TIMEOUT);
        descriptors.add(PROP_SOCKET_TIMEOUT);
        descriptors.add(PROP_CONNECTION_POOL_SIZE);
        return descriptors;
    }

    // ========== LIFECYCLE METHODS ==========

    /**
     * Called when the processor is scheduled.
     * Creates the GPUdb connection and validates the table name.
     *
     * <p>Subclasses should call {@code super.onScheduled(context)} at the start of their implementation.
     *
     * @param context The process context
     * @throws ProcessException if connection fails or table name is invalid
     */
    @OnScheduled
    public void onScheduled(final ProcessContext context) throws ProcessException {
        try {
            // Extract and validate table name
            tableName = context.getProperty(PROP_TABLE).evaluateAttributeExpressions().getValue();
            validateTableName(tableName);

            // Cache SSL/connection configuration
            useSSL = context.getProperty(PROP_USE_SSL).asBoolean();
            bypassCertCheck = context.getProperty(PROP_SSL_BYPASS_CERT_CHECK).asBoolean();
            connectionTimeout = (int) context.getProperty(PROP_CONNECTION_TIMEOUT)
                    .asTimePeriod(TimeUnit.MILLISECONDS).longValue();
            socketTimeout = (int) context.getProperty(PROP_SOCKET_TIMEOUT)
                    .asTimePeriod(TimeUnit.MILLISECONDS).longValue();
            connectionPoolSize = context.getProperty(PROP_CONNECTION_POOL_SIZE).asInteger();

            // Create GPUdb connection
            gpudb = createGPUdbConnection(context);

            // Initialize worker list for bulk operations
            workers = new WorkerList(gpudb);

            getLogger().info("Connected to Kinetica server: {} (SSL: {}, Timeout: {}ms)",
                    gpudb.getURL(), useSSL, connectionTimeout);
        } catch (GPUdbException e) {
            throw new ProcessException("Failed to connect to Kinetica: " + e.getMessage(), e);
        }
    }

    /**
     * Called when the processor is stopped.
     * Cleans up resources (GPUdb connection, workers).
     *
     * <p>Subclasses should call {@code super.onStopped()} at the end of their implementation.
     */
    @OnStopped
    public void onStopped() {
        // Clean up connection pool
        for (GPUdb pooledConnection : connectionPool.values()) {
            try {
                // GPUdb doesn't have explicit close, but we clear the reference
                getLogger().debug("Releasing pooled connection: {}", pooledConnection.getURL());
            } catch (Exception e) {
                getLogger().warn("Error releasing pooled connection: {}", e.getMessage());
            }
        }
        connectionPool.clear();

        // Clean up main resources
        workers = null;
        gpudb = null;
        tableName = null;
        getLogger().debug("Kinetica processor stopped and resources cleaned up");
    }

    // ========== CONNECTION MANAGEMENT ==========

    /**
     * Creates a GPUdb connection using the configured properties.
     *
     * @param context The process context
     * @return Configured GPUdb instance
     * @throws GPUdbException if connection fails
     */
    protected GPUdb createGPUdbConnection(ProcessContext context) throws GPUdbException {
        String serverUrl = context.getProperty(PROP_SERVER).evaluateAttributeExpressions().getValue();
        String username = context.getProperty(PROP_USERNAME).evaluateAttributeExpressions().getValue();
        // Password does not use expression language for security
        String password = context.getProperty(PROP_PASSWORD).getValue();

        return createGPUdbConnectionWithOptions(serverUrl, username, password);
    }

    /**
     * Creates a GPUdb connection for a specific FlowFile, evaluating expression language.
     * This method supports per-FlowFile connection configuration.
     *
     * @param context The process context
     * @param flowFile The FlowFile to evaluate expressions against
     * @return Configured GPUdb instance (may be from pool)
     * @throws GPUdbException if connection fails
     */
    protected GPUdb getConnectionForFlowFile(ProcessContext context, FlowFile flowFile) throws GPUdbException {
        String serverUrl = context.getProperty(PROP_SERVER)
                .evaluateAttributeExpressions(flowFile).getValue();
        String username = context.getProperty(PROP_USERNAME)
                .evaluateAttributeExpressions(flowFile).getValue();
        String password = context.getProperty(PROP_PASSWORD).getValue();

        // Create a cache key based on connection parameters
        String cacheKey = serverUrl + "|" + (username != null ? username : "");

        // Check pool first
        GPUdb pooledConnection = connectionPool.get(cacheKey);
        if (pooledConnection != null) {
            return pooledConnection;
        }

        // Create new connection
        GPUdb newConnection = createGPUdbConnectionWithOptions(serverUrl, username, password);

        // Add to pool if not at max size
        if (connectionPool.size() < MAX_POOL_SIZE) {
            connectionPool.put(cacheKey, newConnection);
        }

        return newConnection;
    }

    /**
     * Creates a GPUdb connection with full options including SSL and timeouts.
     *
     * @param serverUrl The server URL
     * @param username The username (can be null)
     * @param password The password (can be null)
     * @return Configured GPUdb instance
     * @throws GPUdbException if connection fails
     */
    private GPUdb createGPUdbConnectionWithOptions(String serverUrl, String username, String password)
            throws GPUdbException {
        Options options = new Options();

        // Configure authentication if credentials are provided
        if (username != null && !username.isEmpty() && password != null && !password.isEmpty()) {
            options.setUsername(username);
            options.setPassword(password);
        }

        // Configure SSL/TLS
        if (bypassCertCheck) {
            options.setBypassSslCertCheck(true);
            getLogger().warn("SSL certificate verification is disabled - not recommended for production");
        }

        // Configure timeouts
        options.setTimeout(connectionTimeout);

        // Configure thread pool size for multi-head operations
        options.setThreadCount(connectionPoolSize);

        getLogger().debug("Creating GPUdb connection: URL={}, SSL={}, Timeout={}ms, Threads={}",
                serverUrl, useSSL, connectionTimeout, connectionPoolSize);

        return new GPUdb(serverUrl, options);
    }

    /**
     * Returns the GPUdb connection instance.
     *
     * @return The GPUdb connection, or null if not connected
     */
    protected GPUdb getGpudb() {
        return gpudb;
    }

    /**
     * Returns the worker list for bulk operations.
     *
     * @return The worker list, or null if not initialized
     */
    protected WorkerList getWorkers() {
        return workers;
    }

    /**
     * Returns the configured table name.
     *
     * @return The table name
     */
    protected String getTableName() {
        return tableName;
    }

    // ========== VALIDATION ==========

    /**
     * Validates that a table name conforms to Kinetica naming rules.
     * This prevents SQL injection attacks via malicious table names.
     *
     * <p>Valid formats:
     * <ul>
     *   <li>Simple: "table_name"</li>
     *   <li>Schema-qualified: "schema.table_name"</li>
     * </ul>
     *
     * <p>Each part must:
     * <ul>
     *   <li>Start with a letter (a-z, A-Z)</li>
     *   <li>Contain only letters, numbers, and underscores</li>
     *   <li>Be at most 256 characters total</li>
     * </ul>
     *
     * @param tableName The table name to validate
     * @throws ProcessException if the table name is invalid
     */
    protected void validateTableName(String tableName) throws ProcessException {
        if (tableName == null || tableName.isEmpty()) {
            throw new ProcessException("Table name cannot be null or empty");
        }

        if (tableName.length() > MAX_TABLE_NAME_LENGTH) {
            throw new ProcessException("Table name exceeds maximum length of " + MAX_TABLE_NAME_LENGTH + " characters");
        }

        if (!VALID_TABLE_NAME_PATTERN.matcher(tableName).matches()) {
            throw new ProcessException(
                    "Invalid table name: '" + tableName + "'. " +
                    "Table name must start with a letter and contain only letters, numbers, and underscores. " +
                    "Optional schema prefix format: 'schema.table_name'"
            );
        }
    }

    /**
     * Checks if a table exists in Kinetica.
     *
     * @param tableName The table name to check
     * @return true if the table exists, false otherwise
     */
    protected boolean tableExists(String tableName) {
        try {
            return gpudb.hasTable(tableName, null).getTableExists();
        } catch (GPUdbException e) {
            getLogger().error("Error checking if table '{}' exists: {}", tableName, e.getMessage());
            return false;
        }
    }
}
