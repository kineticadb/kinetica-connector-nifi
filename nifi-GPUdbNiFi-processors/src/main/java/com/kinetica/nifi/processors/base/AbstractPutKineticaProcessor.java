package com.kinetica.nifi.processors.base;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

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

/**
 * Abstract base class for Kinetica Put processors (PutKinetica, PutKineticaFromFile, etc.).
 *
 * <p>This class extends AbstractKineticaProcessor with functionality specific to
 * writing data to Kinetica:
 * <ul>
 *   <li>Schema definition and table creation</li>
 *   <li>BulkInserter management</li>
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
                    "Higher values improve throughput but use more memory.")
            .required(true)
            .defaultValue("500")
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

    // ========== SHARED STATE ==========

    protected volatile Type objectType;
    protected volatile boolean updateOnExistingPk;
    protected volatile String dateFormat;
    protected volatile String timeZone;
    protected volatile int batchSize;

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
        batchSize = context.getProperty(PROP_BATCH_SIZE).asInteger();

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
}
