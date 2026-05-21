package com.kinetica.nifi.processors.base;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Supplier;

import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.lifecycle.OnStopped;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;

import com.gpudb.BulkInserter;
import com.gpudb.GPUdb;
import com.gpudb.GPUdbException;
import com.gpudb.Record;
import com.gpudb.RecordObject;
import com.gpudb.Type;
import com.gpudb.Type.Column;
import com.gpudb.protocol.CreateTableRequest;
import com.gpudb.protocol.InsertRecordsRequest;
import com.kinetica.nifi.processors.KineticaUtilities;

/**
 * Abstract base class for Kinetica Put processors (PutKinetica, PutKineticaFromFile, etc.).
 *
 * <p>This class extends AbstractKineticaProcessor with functionality specific to
 * writing data to Kinetica:
 * <ul>
 *   <li>Schema definition and table creation</li>
 *   <li>Thread-safe BulkInserter management</li>
 *   <li>Retry logic with exponential backoff</li>
 *   <li>Common Put processor properties (batch size, schema, collection)</li>
 *   <li>Type/schema parsing and creation</li>
 * </ul>
 *
 * @author Kinetica Engineering
 * @version 7.2.0.0
 * @since 7.2.0.0
 */
public abstract class AbstractPutKineticaProcessor extends AbstractKineticaProcessor {

    // ========== PUT-SPECIFIC PROPERTY DESCRIPTORS ==========

    public static final PropertyDescriptor PROP_COLLECTION = new PropertyDescriptor.Builder()
            .name("Collection Name")
            .displayName("Collection Name")
            .description("Name of the Kinetica collection (optional). Tables can be organized into collections.")
            .required(false)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor PROP_SCHEMA = new PropertyDescriptor.Builder()
            .name("Schema")
            .displayName("Schema Definition")
            .description("Schema of the Kinetica table. Required if table doesn't exist. " +
                    "Format: column1|type|annotations,column2|type|annotations,... " +
                    "Example: x|Float|data,y|Float|data,TIMESTAMP|Long|data,TEXT|String|store_only|text_search")
            .required(false)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor PROP_BATCH_SIZE = new PropertyDescriptor.Builder()
            .name("Batch Size")
            .displayName("Batch Size")
            .description("Number of records to batch before flushing to Kinetica. " +
                    "Higher values improve throughput but use more memory. " +
                    "Supports Expression Language (evaluated at processor startup).")
            .required(true)
            .defaultValue("500")
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .build();

    public static final PropertyDescriptor PROP_UPDATE_ON_EXISTING_PK = new PropertyDescriptor.Builder()
            .name("Update on Existing PK")
            .displayName("Update on Existing Primary Key")
            .description("If true and the table has a primary key, existing records with matching keys will be updated. " +
                    "If false, records with existing keys are ignored.")
            .required(true)
            .defaultValue("false")
            .allowableValues("true", "false")
            .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
            .build();

    public static final PropertyDescriptor PROP_REPLICATE_TABLE = new PropertyDescriptor.Builder()
            .name("Replicate Table")
            .displayName("Replicate Table")
            .description("If true and table needs to be created, the table will be replicated across all cluster nodes.")
            .required(true)
            .defaultValue("false")
            .allowableValues("true", "false")
            .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
            .build();

    public static final PropertyDescriptor PROP_DATE_FORMAT = new PropertyDescriptor.Builder()
            .name("Date Format")
            .displayName("Date Format")
            .description("Date format pattern for parsing datetime values. Example: yyyy/MM/dd HH:mm:ss")
            .required(false)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor PROP_TIMEZONE = new PropertyDescriptor.Builder()
            .name("Timezone")
            .displayName("Timezone")
            .description("Timezone for datetime values. If not set, the system default will be used. Example: EST, UTC")
            .required(false)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    // ========== RELATIONSHIPS ==========

    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("FlowFiles that were successfully written to Kinetica")
            .build();

    public static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("FlowFiles that failed to be written to Kinetica")
            .build();

    // ========== RETRY CONFIGURATION ==========

    /** Maximum number of retry attempts for transient failures */
    protected static final int MAX_RETRY_ATTEMPTS = 3;

    /** Base delay in milliseconds for exponential backoff */
    protected static final long BASE_RETRY_DELAY_MS = 100;

    /** Maximum delay in milliseconds for exponential backoff */
    protected static final long MAX_RETRY_DELAY_MS = 5000;

    /** Jitter factor for retry delays (0.0 to 1.0) */
    protected static final double RETRY_JITTER_FACTOR = 0.25;

    // ========== SHARED STATE ==========

    protected volatile Type objectType;
    protected volatile boolean updateOnExistingPk;
    protected volatile String dateFormat;
    protected volatile String timeZone;
    protected volatile int batchSize;

    /** Lock for thread-safe BulkInserter operations */
    private final ReentrantLock bulkInserterLock = new ReentrantLock();

    /** Shared BulkInserter instance for batch operations */
    private volatile BulkInserter<Record> sharedBulkInserter;

    private List<PropertyDescriptor> descriptors;
    private Set<Relationship> relationships;

    // ========== INITIALIZATION ==========

    @Override
    protected void init(org.apache.nifi.processor.ProcessorInitializationContext context) {
        // Build property descriptors
        List<PropertyDescriptor> props = new ArrayList<>();
        props.addAll(getBasePropertyDescriptors());
        props.add(PROP_COLLECTION);
        props.add(PROP_SCHEMA);
        props.add(PROP_BATCH_SIZE);
        props.add(PROP_UPDATE_ON_EXISTING_PK);
        props.add(PROP_REPLICATE_TABLE);
        props.add(PROP_DATE_FORMAT);
        props.add(PROP_TIMEZONE);
        // Allow subclasses to add more properties
        props.addAll(getAdditionalPropertyDescriptors());
        this.descriptors = Collections.unmodifiableList(props);

        // Build relationships
        Set<Relationship> rels = new HashSet<>();
        rels.add(REL_SUCCESS);
        rels.add(REL_FAILURE);
        this.relationships = Collections.unmodifiableSet(rels);
    }

    /**
     * Override this method in subclasses to add additional property descriptors.
     *
     * @return List of additional property descriptors (can be empty)
     */
    protected List<PropertyDescriptor> getAdditionalPropertyDescriptors() {
        return Collections.emptyList();
    }

    @Override
    public Set<Relationship> getRelationships() {
        return relationships;
    }

    @Override
    public List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return descriptors;
    }

    // ========== LIFECYCLE ==========

    @Override
    @OnScheduled
    public void onScheduled(final ProcessContext context) throws ProcessException {
        super.onScheduled(context);

        // Read Put-specific configuration
        updateOnExistingPk = context.getProperty(PROP_UPDATE_ON_EXISTING_PK).asBoolean();
        dateFormat = context.getProperty(PROP_DATE_FORMAT).evaluateAttributeExpressions().getValue();
        timeZone = context.getProperty(PROP_TIMEZONE).evaluateAttributeExpressions().getValue();
        batchSize = context.getProperty(PROP_BATCH_SIZE).evaluateAttributeExpressions().asInteger();

        // Get or create table type
        try {
            if (tableExists(tableName)) {
                getLogger().debug("Getting type from existing table: {}", tableName);
                objectType = Type.fromTable(gpudb, tableName);
            } else if (context.getProperty(PROP_SCHEMA).isSet()) {
                String schemaStr = context.getProperty(PROP_SCHEMA).evaluateAttributeExpressions().getValue();
                objectType = createTable(context, schemaStr);
            } else {
                objectType = null;
                getLogger().warn("Table '{}' does not exist and no schema provided", tableName);
            }
        } catch (GPUdbException e) {
            throw new ProcessException("Failed to initialize table type: " + e.getMessage(), e);
        }
    }

    @Override
    @OnStopped
    public void onStopped() {
        // Flush and close the shared BulkInserter
        bulkInserterLock.lock();
        try {
            if (sharedBulkInserter != null) {
                try {
                    sharedBulkInserter.flush();
                } catch (GPUdbException e) {
                    getLogger().warn("Error flushing BulkInserter during shutdown: {}", e.getMessage());
                }
                sharedBulkInserter = null;
            }
        } finally {
            bulkInserterLock.unlock();
        }

        objectType = null;
        super.onStopped();
    }

    // ========== TABLE CREATION ==========

    /**
     * Creates a new table in Kinetica based on the provided schema string.
     *
     * @param context The process context
     * @param schemaStr The schema definition string
     * @return The Type object representing the created table
     * @throws GPUdbException if table creation fails
     */
    protected Type createTable(ProcessContext context, String schemaStr) throws GPUdbException {
        getLogger().info("Creating table '{}' with schema: {}", tableName, schemaStr);

        // Check if table already exists
        if (tableExists(tableName)) {
            getLogger().debug("Table '{}' already exists, returning existing type", tableName);
            return Type.fromTable(gpudb, tableName);
        }

        // Parse schema string into columns
        List<Column> columns = parseSchemaString(schemaStr);
        Type type = new Type("", columns);

        // Create the type in Kinetica
        String typeId = type.create(gpudb);

        // Set up table creation options
        String collection = context.getProperty(PROP_COLLECTION).evaluateAttributeExpressions().getValue();
        if (collection == null) {
            collection = "";
        }

        boolean replicated = context.getProperty(PROP_REPLICATE_TABLE).asBoolean();

        Map<String, String> createOptions = GPUdb.options(
                CreateTableRequest.Options.COLLECTION_NAME, collection,
                CreateTableRequest.Options.IS_REPLICATED,
                replicated ? CreateTableRequest.Options.TRUE : CreateTableRequest.Options.FALSE
        );

        // Create the table
        gpudb.createTable(tableName, typeId, createOptions);
        gpudb.addKnownType(typeId, RecordObject.class);

        getLogger().info("Successfully created table '{}' (replicated={})", tableName, replicated);
        return type;
    }

    /**
     * Parses a schema definition string into a list of Column objects.
     *
     * <p>Format: column1|type|annotations,column2|type|annotations,...
     * <p>Example: x|Float|data,y|Float|data,TIMESTAMP|Long|data
     *
     * @param schemaStr The schema string to parse
     * @return List of Column objects
     * @throws GPUdbException if schema parsing fails
     */
    protected List<Column> parseSchemaString(String schemaStr) throws GPUdbException {
        List<Column> columns = new ArrayList<>();
        int maxPrimaryKey = -1;

        String[] fieldArray = schemaStr.split(",");
        for (String fieldStr : fieldArray) {
            String[] parts = fieldStr.split("\\|", -1);
            if (parts.length < 1 || parts[0].trim().isEmpty()) {
                throw new GPUdbException("Invalid schema field: " + fieldStr);
            }

            String name = parts[0].trim();
            Class<?> type = String.class; // Default to String

            // Parse type if specified
            if (parts.length > 1 && !parts[1].trim().isEmpty()) {
                type = parseColumnType(parts[1].trim(), name);
            }

            // Parse annotations
            List<String> annotations = new ArrayList<>();
            for (int i = 2; i < parts.length; i++) {
                String annotation = parts[i].toLowerCase().trim();
                if (annotation.isEmpty()) continue;

                // Handle primary key annotation
                if (annotation.startsWith("$primary_key")) {
                    int keyIndex = parsePrimaryKeyIndex(annotation);
                    if (keyIndex != -1) {
                        maxPrimaryKey = Math.max(keyIndex, maxPrimaryKey);
                    } else {
                        maxPrimaryKey++;
                    }
                } else {
                    annotations.add(annotation);
                }
            }

            columns.add(new Column(name, type, annotations));
        }

        return columns;
    }

    /**
     * Parses a column type string into a Java Class.
     */
    private Class<?> parseColumnType(String typeStr, String columnName) throws GPUdbException {
        switch (typeStr.toLowerCase()) {
            case "double":
                return Double.class;
            case "float":
                return Float.class;
            case "integer":
            case "int":
                return Integer.class;
            case "long":
                return Long.class;
            case "string":
                return String.class;
            default:
                throw new GPUdbException("Invalid data type '" + typeStr + "' for column " + columnName +
                        ". Valid types: double, float, integer, int, long, string");
        }
    }

    /**
     * Parses primary key index from annotation string like "$primary_key(0)".
     */
    private int parsePrimaryKeyIndex(String annotation) {
        int openIndex = annotation.indexOf('(');
        int closeIndex = annotation.indexOf(')', openIndex);

        if (openIndex != -1 && closeIndex != -1) {
            try {
                return Integer.parseInt(annotation.substring(openIndex + 1, closeIndex));
            } catch (NumberFormatException e) {
                // Fall through to return -1
            }
        }
        return -1;
    }

    // ========== BULK INSERTER ==========

    /**
     * Creates a BulkInserter for efficient batch inserts.
     *
     * @return Configured BulkInserter
     * @throws GPUdbException if creation fails
     */
    protected BulkInserter<Record> createBulkInserter() throws GPUdbException {
        if (objectType == null) {
            throw new GPUdbException("Cannot create BulkInserter: table type is null. " +
                    "Table must exist or schema must be provided.");
        }

        Map<String, String> options = GPUdb.options(
                InsertRecordsRequest.Options.UPDATE_ON_EXISTING_PK,
                updateOnExistingPk ? InsertRecordsRequest.Options.TRUE : InsertRecordsRequest.Options.FALSE
        );

        return new BulkInserter<>(gpudb, tableName, objectType, batchSize, options, workers);
    }

    /**
     * Returns the current table type.
     *
     * @return The Type object for the current table
     */
    protected Type getObjectType() {
        return objectType;
    }

    // ========== COLUMN VALUE CONVERSION ==========

    /**
     * Sets a column value on a record with automatic type conversion.
     *
     * <p>This method centralizes type conversion logic used across all Put processors.
     * It handles null values, timestamps, and all supported Kinetica data types.
     *
     * @param record The record to update
     * @param column The column metadata
     * @param value The string value to convert and set
     * @return true if value was set successfully, false if parsing failed
     */
    protected boolean setColumnValue(Record record, Column column, String value) {
        String columnName = column.getName();

        // Handle null or empty values
        if (value == null || value.trim().isEmpty()) {
            if (column.isNullable()) {
                record.put(columnName, null);
            }
            // For non-nullable columns, leave as default
            return true;
        }

        String trimmed = value.trim();

        // Check if this is a timestamp column
        boolean isTimestamp = KineticaUtilities.checkForTimeStamp(column);

        try {
            if (isTimestamp) {
                // Handle timestamp values
                Long timestamp = KineticaUtilities.parseDateOrTimestamp(trimmed, dateFormat, timeZone, getLogger());
                if (timestamp != null) {
                    record.put(columnName, timestamp);
                } else {
                    getLogger().warn("Failed to parse timestamp '{}' for column '{}'", value, columnName);
                    return false;
                }
            } else if (column.getType() == Double.class) {
                record.put(columnName, KineticaUtilities.parseDoubleSafe(trimmed, 0.0));
            } else if (column.getType() == Float.class) {
                record.put(columnName, KineticaUtilities.parseFloatSafe(trimmed, 0.0f));
            } else if (column.getType() == Integer.class) {
                record.put(columnName, KineticaUtilities.parseIntSafe(trimmed, 0));
            } else if (column.getType() == Long.class) {
                record.put(columnName, KineticaUtilities.parseLongSafe(trimmed, 0L));
            } else {
                // String type
                String cleanValue = KineticaUtilities.trimToNull(value);
                if (cleanValue != null) {
                    record.put(columnName, cleanValue);
                }
            }
            return true;
        } catch (Exception e) {
            getLogger().error("Error setting value '{}' for column '{}': {}", value, columnName, e.getMessage());
            return false;
        }
    }

    /**
     * Creates a new empty record from the table type.
     *
     * @return A new Record instance, or null if objectType is not set
     */
    protected Record createEmptyRecord() {
        if (objectType == null) {
            getLogger().error("Object type is null, cannot create record");
            return null;
        }
        return objectType.newInstance();
    }

    // ========== THREAD-SAFE BULK INSERTER ACCESS ==========

    /**
     * Gets or creates the shared BulkInserter instance in a thread-safe manner.
     *
     * @return The shared BulkInserter
     * @throws GPUdbException if creation fails
     */
    protected BulkInserter<Record> getOrCreateBulkInserter() throws GPUdbException {
        // Fast path: check without lock
        BulkInserter<Record> inserter = sharedBulkInserter;
        if (inserter != null) {
            return inserter;
        }

        // Slow path: acquire lock and create if needed
        bulkInserterLock.lock();
        try {
            // Double-check after acquiring lock
            if (sharedBulkInserter == null) {
                sharedBulkInserter = createBulkInserter();
            }
            return sharedBulkInserter;
        } finally {
            bulkInserterLock.unlock();
        }
    }

    /**
     * Inserts a record using the shared BulkInserter with thread safety.
     *
     * @param record The record to insert
     * @throws GPUdbException if insertion fails
     */
    protected void insertRecord(Record record) throws GPUdbException {
        BulkInserter<Record> inserter = getOrCreateBulkInserter();
        bulkInserterLock.lock();
        try {
            inserter.insert(record);
        } finally {
            bulkInserterLock.unlock();
        }
    }

    /**
     * Flushes the shared BulkInserter with thread safety.
     *
     * @throws GPUdbException if flush fails
     */
    protected void flushBulkInserter() throws GPUdbException {
        bulkInserterLock.lock();
        try {
            if (sharedBulkInserter != null) {
                sharedBulkInserter.flush();
            }
        } finally {
            bulkInserterLock.unlock();
        }
    }

    // ========== RETRY LOGIC WITH EXPONENTIAL BACKOFF ==========

    /**
     * Executes an operation with retry logic using exponential backoff.
     *
     * <p>This method will retry the operation up to {@link #MAX_RETRY_ATTEMPTS} times
     * with exponentially increasing delays between attempts. Jitter is added to
     * prevent thundering herd problems.
     *
     * @param <T> The return type of the operation
     * @param operation The operation to execute
     * @param operationName Name of the operation for logging
     * @return The result of the operation
     * @throws GPUdbException if all retry attempts fail
     */
    protected <T> T executeWithRetry(Supplier<T> operation, String operationName) throws GPUdbException {
        GPUdbException lastException = null;

        for (int attempt = 1; attempt <= MAX_RETRY_ATTEMPTS; attempt++) {
            try {
                return operation.get();
            } catch (Exception e) {
                // Wrap non-GPUdbException
                if (e instanceof GPUdbException) {
                    lastException = (GPUdbException) e;
                } else {
                    lastException = new GPUdbException(e.getMessage(), e);
                }

                // Check if this is a retryable error
                if (!isRetryableError(lastException)) {
                    getLogger().error("Non-retryable error during {}: {}", operationName, e.getMessage());
                    throw lastException;
                }

                // Don't retry on last attempt
                if (attempt < MAX_RETRY_ATTEMPTS) {
                    long delay = calculateRetryDelay(attempt);
                    getLogger().warn("Attempt {}/{} failed for {}: {}. Retrying in {}ms",
                            attempt, MAX_RETRY_ATTEMPTS, operationName, e.getMessage(), delay);

                    try {
                        Thread.sleep(delay);
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        throw new GPUdbException("Retry interrupted", ie);
                    }
                }
            }
        }

        getLogger().error("All {} retry attempts failed for {}", MAX_RETRY_ATTEMPTS, operationName);
        throw lastException;
    }

    /**
     * Executes a void operation with retry logic using exponential backoff.
     *
     * @param operation The operation to execute
     * @param operationName Name of the operation for logging
     * @throws GPUdbException if all retry attempts fail
     */
    protected void executeWithRetry(Runnable operation, String operationName) throws GPUdbException {
        executeWithRetry(() -> {
            operation.run();
            return null;
        }, operationName);
    }

    /**
     * Calculates the retry delay with exponential backoff and jitter.
     *
     * @param attempt The current attempt number (1-based)
     * @return The delay in milliseconds
     */
    private long calculateRetryDelay(int attempt) {
        // Exponential backoff: baseDelay * 2^(attempt-1)
        long exponentialDelay = BASE_RETRY_DELAY_MS * (1L << (attempt - 1));

        // Cap at maximum delay
        long cappedDelay = Math.min(exponentialDelay, MAX_RETRY_DELAY_MS);

        // Add jitter: delay * (1 +/- jitterFactor * random)
        double jitter = (ThreadLocalRandom.current().nextDouble() * 2 - 1) * RETRY_JITTER_FACTOR;
        long finalDelay = (long) (cappedDelay * (1 + jitter));

        return Math.max(finalDelay, BASE_RETRY_DELAY_MS);
    }

    /**
     * Determines if an error is retryable.
     *
     * <p>Retryable errors include:
     * <ul>
     *   <li>Connection timeouts</li>
     *   <li>Network errors</li>
     *   <li>Server overload (503)</li>
     *   <li>Rate limiting (429)</li>
     * </ul>
     *
     * @param e The exception to check
     * @return true if the error is retryable
     */
    protected boolean isRetryableError(GPUdbException e) {
        String message = e.getMessage();
        if (message == null) {
            return false;
        }

        String lowerMessage = message.toLowerCase();

        // Connection/network errors
        if (lowerMessage.contains("connection") ||
                lowerMessage.contains("timeout") ||
                lowerMessage.contains("network") ||
                lowerMessage.contains("socket")) {
            return true;
        }

        // Server overload or rate limiting
        if (lowerMessage.contains("503") ||
                lowerMessage.contains("429") ||
                lowerMessage.contains("service unavailable") ||
                lowerMessage.contains("too many requests") ||
                lowerMessage.contains("rate limit")) {
            return true;
        }

        // Temporary server errors
        if (lowerMessage.contains("temporarily") ||
                lowerMessage.contains("try again") ||
                lowerMessage.contains("retry")) {
            return true;
        }

        return false;
    }
}
