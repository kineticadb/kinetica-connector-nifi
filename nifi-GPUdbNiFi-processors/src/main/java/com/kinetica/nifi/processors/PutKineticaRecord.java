package com.kinetica.nifi.processors;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.ReadsAttribute;
import org.apache.nifi.annotation.behavior.ReadsAttributes;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.serialization.RecordReader;
import org.apache.nifi.serialization.RecordReaderFactory;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.record.RecordSchema;

import com.gpudb.BulkInserter;
import com.gpudb.GPUdbException;
import com.gpudb.Type;
import com.gpudb.protocol.InsertRecordsRequest;
import com.kinetica.nifi.processors.base.AbstractKineticaProcessor;

/**
 * NiFi processor that uses the Record API to insert data into Kinetica.
 *
 * <p>This processor leverages NiFi's pluggable Record Reader framework,
 * allowing it to work with any data format that has a RecordReader implementation:
 * <ul>
 *   <li>JSON (JsonTreeReader, JsonPathReader)</li>
 *   <li>CSV (CSVReader)</li>
 *   <li>Avro (AvroReader)</li>
 *   <li>XML (XMLReader)</li>
 *   <li>Parquet (ParquetReader)</li>
 *   <li>And many more...</li>
 * </ul>
 *
 * <p>The processor automatically maps NiFi Record fields to Kinetica columns
 * based on field names. Type conversions are handled automatically where possible.
 *
 * @author Kinetica Engineering
 * @version 7.2.0.0
 * @since 7.2.0.0
 */
@Tags({"Kinetica", "put", "insert", "record", "database", "ingest"})
@CapabilityDescription("Inserts records into a Kinetica table using NiFi's Record API. " +
        "This processor works with any RecordReader implementation, supporting " +
        "JSON, CSV, Avro, XML, Parquet, and other formats. " +
        "Records are automatically mapped to Kinetica columns by field name.")
@ReadsAttributes({
        @ReadsAttribute(attribute = "record.count", description = "Number of records in the input FlowFile (if available)")
})
@WritesAttributes({
        @WritesAttribute(attribute = "kinetica.inserted.count", description = "Number of records successfully inserted"),
        @WritesAttribute(attribute = "kinetica.failed.count", description = "Number of records that failed to insert"),
        @WritesAttribute(attribute = "kinetica.table.name", description = "Name of the target Kinetica table"),
        @WritesAttribute(attribute = "kinetica.insert.time_ms", description = "Time taken for insert operation in milliseconds"),
        @WritesAttribute(attribute = "kinetica.error", description = "Error message if insertion failed")
})
@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
public class PutKineticaRecord extends AbstractKineticaProcessor {

    private static final String PROCESSOR_NAME = "PutKineticaRecord";

    // ========== PROPERTY DESCRIPTORS ==========

    public static final PropertyDescriptor RECORD_READER = new PropertyDescriptor.Builder()
            .name("record-reader")
            .displayName("Record Reader")
            .description("Specifies the Controller Service to use for reading incoming data. " +
                    "The reader determines the format (JSON, CSV, Avro, etc.) and schema.")
            .identifiesControllerService(RecordReaderFactory.class)
            .required(true)
            .build();

    public static final PropertyDescriptor PROP_BATCH_SIZE = new PropertyDescriptor.Builder()
            .name("Batch Size")
            .displayName("Batch Size")
            .description("Number of records to batch before flushing to Kinetica. " +
                    "Larger batches improve throughput but use more memory.")
            .required(false)
            .defaultValue("10000")
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .build();

    public static final PropertyDescriptor PROP_UPDATE_ON_EXISTING_PK = new PropertyDescriptor.Builder()
            .name("Update on Existing PK")
            .displayName("Update on Existing Primary Key")
            .description("If true, updates existing records when a primary key collision occurs. " +
                    "If false, inserts with duplicate primary keys will be rejected.")
            .required(false)
            .defaultValue("false")
            .allowableValues("true", "false")
            .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
            .build();

    public static final PropertyDescriptor PROP_SKIP_ERRORS = new PropertyDescriptor.Builder()
            .name("Skip Errors")
            .displayName("Skip Records on Error")
            .description("If true, records that fail to insert are skipped and processing continues. " +
                    "If false, the entire FlowFile fails on the first error.")
            .required(false)
            .defaultValue("false")
            .allowableValues("true", "false")
            .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
            .build();

    public static final PropertyDescriptor PROP_CREATE_TABLE = new PropertyDescriptor.Builder()
            .name("Create Table")
            .displayName("Create Table If Not Exists")
            .description("If true, creates the target table if it doesn't exist. " +
                    "The table schema is inferred from the first record's schema.")
            .required(false)
            .defaultValue("false")
            .allowableValues("true", "false")
            .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
            .build();

    // ========== RELATIONSHIPS ==========

    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("FlowFiles that were successfully processed")
            .build();

    public static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("FlowFiles that failed to process")
            .build();

    // ========== STATE ==========

    private volatile int batchSize;
    private volatile boolean updateOnExistingPk;
    private volatile boolean skipErrors;
    private volatile boolean createTable;
    private volatile Type kineticaType;

    private List<PropertyDescriptor> descriptors;
    private Set<Relationship> relationships;

    @Override
    protected void init(org.apache.nifi.processor.ProcessorInitializationContext context) {
        List<PropertyDescriptor> props = new ArrayList<>();
        props.addAll(getBasePropertyDescriptors());
        props.add(RECORD_READER);
        props.add(PROP_BATCH_SIZE);
        props.add(PROP_UPDATE_ON_EXISTING_PK);
        props.add(PROP_SKIP_ERRORS);
        props.add(PROP_CREATE_TABLE);
        this.descriptors = Collections.unmodifiableList(props);

        Set<Relationship> rels = new HashSet<>();
        rels.add(REL_SUCCESS);
        rels.add(REL_FAILURE);
        this.relationships = Collections.unmodifiableSet(rels);
    }

    @Override
    public Set<Relationship> getRelationships() {
        return relationships;
    }

    @Override
    public List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return descriptors;
    }

    @Override
    @OnScheduled
    public void onScheduled(final ProcessContext context) throws ProcessException {
        super.onScheduled(context);

        batchSize = context.getProperty(PROP_BATCH_SIZE).asInteger();
        updateOnExistingPk = context.getProperty(PROP_UPDATE_ON_EXISTING_PK).asBoolean();
        skipErrors = context.getProperty(PROP_SKIP_ERRORS).asBoolean();
        createTable = context.getProperty(PROP_CREATE_TABLE).asBoolean();

        // Reset cached type
        kineticaType = null;
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }

        final long startTime = System.currentTimeMillis();
        long insertedCount = 0;
        long failedCount = 0;

        final RecordReaderFactory readerFactory = context.getProperty(RECORD_READER)
                .asControllerService(RecordReaderFactory.class);

        try (final InputStream in = session.read(flowFile)) {
            try (final RecordReader reader = readerFactory.createRecordReader(flowFile, in, getLogger())) {

                // Get schema from reader
                RecordSchema schema = reader.getSchema();

                // Ensure table exists and get Kinetica type
                ensureTableExists(schema);

                // Create bulk inserter
                Map<String, String> insertOptions = new HashMap<>();
                if (updateOnExistingPk) {
                    insertOptions.put(InsertRecordsRequest.Options.UPDATE_ON_EXISTING_PK,
                            InsertRecordsRequest.Options.TRUE);
                }

                BulkInserter<com.gpudb.Record> inserter = new BulkInserter<>(
                        gpudb, tableName, kineticaType, batchSize, insertOptions, workers);

                // Process records
                Record nifiRecord;
                while ((nifiRecord = reader.nextRecord()) != null) {
                    try {
                        com.gpudb.Record kineticaRecord = convertToKineticaRecord(nifiRecord, kineticaType);
                        inserter.insert(kineticaRecord);
                        insertedCount++;
                    } catch (Exception e) {
                        failedCount++;
                        if (!skipErrors) {
                            throw new ProcessException("Failed to process record: " + e.getMessage(), e);
                        }
                        getLogger().warn("Skipping record due to error: {}", e.getMessage());
                    }
                }

                // Flush remaining records
                inserter.flush();
            }

            long duration = System.currentTimeMillis() - startTime;

            // Set success attributes
            flowFile = session.putAttribute(flowFile, "kinetica.inserted.count", String.valueOf(insertedCount));
            flowFile = session.putAttribute(flowFile, "kinetica.failed.count", String.valueOf(failedCount));
            flowFile = session.putAttribute(flowFile, "kinetica.table.name", tableName);
            flowFile = session.putAttribute(flowFile, "kinetica.insert.time_ms", String.valueOf(duration));

            session.transfer(flowFile, REL_SUCCESS);

            getLogger().info("{}: Inserted {} records ({} failed) into {} in {}ms",
                    PROCESSOR_NAME, insertedCount, failedCount, tableName, duration);

        } catch (Exception e) {
            getLogger().error("{}: Failed to process FlowFile: {}", PROCESSOR_NAME, e.getMessage(), e);

            flowFile = session.putAttribute(flowFile, "kinetica.inserted.count", String.valueOf(insertedCount));
            flowFile = session.putAttribute(flowFile, "kinetica.failed.count", String.valueOf(failedCount));
            flowFile = session.putAttribute(flowFile, "kinetica.table.name", tableName);
            flowFile = session.putAttribute(flowFile, "kinetica.error", e.getMessage());

            session.transfer(flowFile, REL_FAILURE);
        }
    }

    /**
     * Ensures the target table exists in Kinetica.
     * If createTable is true and table doesn't exist, creates it based on the schema.
     */
    private void ensureTableExists(RecordSchema schema) throws GPUdbException {
        // Check if we already have the type cached
        if (kineticaType != null) {
            return;
        }

        // Check if table exists
        boolean exists = tableExists(tableName);

        if (!exists && createTable) {
            // Create table from schema
            kineticaType = createTypeFromSchema(schema);
            String typeId = kineticaType.create(gpudb);
            gpudb.createTable(tableName, typeId, new HashMap<>());
            getLogger().info("Created table {} from record schema", tableName);
        } else if (!exists) {
            throw new GPUdbException("Table " + tableName + " does not exist and Create Table is disabled");
        } else {
            // Get existing table type
            kineticaType = Type.fromTable(gpudb, tableName);
        }
    }

    /**
     * Creates a Kinetica Type from a NiFi RecordSchema.
     */
    private Type createTypeFromSchema(RecordSchema schema) {
        List<Type.Column> columns = new ArrayList<>();

        for (RecordField field : schema.getFields()) {
            String fieldName = field.getFieldName();
            Class<?> javaType = mapRecordTypeToJavaType(field);

            // Add column without properties (simple mapping)
            columns.add(new Type.Column(fieldName, javaType));
        }

        return new Type(columns);
    }

    /**
     * Maps NiFi Record data types to Java types supported by Kinetica.
     */
    private Class<?> mapRecordTypeToJavaType(RecordField field) {
        String dataType = field.getDataType().getFieldType().name();

        switch (dataType) {
            case "BOOLEAN":
                return Integer.class; // Kinetica stores booleans as int
            case "BYTE":
            case "SHORT":
            case "INT":
                return Integer.class;
            case "LONG":
                return Long.class;
            case "FLOAT":
                return Float.class;
            case "DOUBLE":
            case "DECIMAL":
                return Double.class;
            case "STRING":
            case "CHAR":
                return String.class;
            case "DATE":
            case "TIME":
            case "TIMESTAMP":
                return Long.class; // Store as epoch millis
            case "ARRAY":
            case "MAP":
            case "RECORD":
            case "CHOICE":
            default:
                // Complex types stored as JSON string
                return String.class;
        }
    }

    /**
     * Converts a NiFi Record to a Kinetica Record.
     */
    private com.gpudb.Record convertToKineticaRecord(Record nifiRecord, Type kineticaType)
            throws Exception {
        com.gpudb.Record kRecord = kineticaType.newInstance();
        RecordSchema schema = nifiRecord.getSchema();

        for (RecordField field : schema.getFields()) {
            String fieldName = field.getFieldName();

            // Check if Kinetica type has this column
            int columnIndex = getColumnIndex(kineticaType, fieldName);
            if (columnIndex < 0) {
                // Field doesn't exist in Kinetica table - skip
                continue;
            }

            Object value = nifiRecord.getValue(field);

            if (value == null) {
                kRecord.put(columnIndex, null);
                continue;
            }

            // Convert value to Kinetica-compatible type
            Class<?> targetType = kineticaType.getColumn(columnIndex).getType();
            Object convertedValue = convertValue(value, targetType);
            kRecord.put(columnIndex, convertedValue);
        }

        return kRecord;
    }

    /**
     * Gets the column index for a field name in the Kinetica type.
     */
    private int getColumnIndex(Type type, String fieldName) {
        List<Type.Column> columns = type.getColumns();
        for (int i = 0; i < columns.size(); i++) {
            if (columns.get(i).getName().equalsIgnoreCase(fieldName)) {
                return i;
            }
        }
        return -1;
    }

    /**
     * Converts a value to the target Kinetica type.
     */
    private Object convertValue(Object value, Class<?> targetType) {
        if (value == null) {
            return null;
        }

        // If already the right type, return as-is
        if (targetType.isInstance(value)) {
            return value;
        }

        // String conversions
        if (targetType == String.class) {
            return value.toString();
        }

        // Numeric conversions
        if (targetType == Integer.class) {
            if (value instanceof Number) {
                return ((Number) value).intValue();
            }
            if (value instanceof Boolean) {
                return ((Boolean) value) ? 1 : 0;
            }
            return Integer.parseInt(value.toString());
        }

        if (targetType == Long.class) {
            if (value instanceof Number) {
                return ((Number) value).longValue();
            }
            if (value instanceof java.util.Date) {
                return ((java.util.Date) value).getTime();
            }
            if (value instanceof java.time.temporal.Temporal) {
                if (value instanceof java.time.Instant) {
                    return ((java.time.Instant) value).toEpochMilli();
                }
                if (value instanceof java.time.LocalDateTime) {
                    return ((java.time.LocalDateTime) value)
                            .atZone(java.time.ZoneId.systemDefault())
                            .toInstant()
                            .toEpochMilli();
                }
            }
            return Long.parseLong(value.toString());
        }

        if (targetType == Float.class) {
            if (value instanceof Number) {
                return ((Number) value).floatValue();
            }
            return Float.parseFloat(value.toString());
        }

        if (targetType == Double.class) {
            if (value instanceof Number) {
                return ((Number) value).doubleValue();
            }
            return Double.parseDouble(value.toString());
        }

        // Fallback - convert to string
        return value.toString();
    }
}
