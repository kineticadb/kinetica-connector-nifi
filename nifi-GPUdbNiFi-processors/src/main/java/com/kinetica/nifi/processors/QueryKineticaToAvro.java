package com.kinetica.nifi.processors;

import java.io.IOException;
import java.io.OutputStream;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericArray;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.EncoderFactory;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.OutputStreamCallback;
import org.apache.nifi.processor.util.StandardValidators;

import com.gpudb.GPUdbException;
import com.gpudb.Record;
import com.gpudb.Type;
import com.gpudb.Type.Column;
import com.gpudb.protocol.ExecuteSqlResponse;
import com.kinetica.nifi.processors.base.AbstractQueryKineticaProcessor;

/**
 * NiFi processor that executes SQL queries on Kinetica and outputs results as Avro.
 *
 * <p>This processor runs SELECT queries against Kinetica and streams the results
 * to Avro-formatted FlowFiles.
 *
 * <p>Key features:
 * <ul>
 *   <li>Avro container format output with embedded schema</li>
 *   <li>Optional raw Avro binary format (without schema)</li>
 *   <li>Automatic schema generation from query results</li>
 *   <li>Two execution modes:
 *       <ul>
 *         <li><b>Traditional pagination</b>: Re-executes query for each page (default)</li>
 *         <li><b>Streaming mode</b>: Uses server-side paging tables for efficiency with large results</li>
 *       </ul>
 *   </li>
 * </ul>
 *
 * <p><strong>Streaming Mode (recommended for large queries):</strong>
 * When enabled, uses Kinetica's GPUdbSqlIterator which creates server-side paging tables.
 * This avoids re-executing the query for each batch and provides better performance
 * for queries returning more than 100K records.
 *
 * @author Kinetica Engineering
 * @version 7.2.0.0
 * @since 7.2.0.0
 */
@Tags({"Kinetica", "query", "select", "avro", "export"})
@CapabilityDescription("Executes SQL SELECT queries on Kinetica and outputs results as Avro. " +
        "Results are streamed to avoid memory issues with large result sets. " +
        "Enable 'Use Streaming Mode' for queries returning more than 100K records - " +
        "this uses server-side paging tables for better performance. " +
        "Only SELECT queries are allowed for security.")
@WritesAttributes({
        @WritesAttribute(attribute = "record.count", description = "Number of records in the output"),
        @WritesAttribute(attribute = "mime.type", description = "Set to application/avro-binary"),
        @WritesAttribute(attribute = "avro.schema", description = "The Avro schema as JSON")
})
@InputRequirement(InputRequirement.Requirement.INPUT_ALLOWED)
public class QueryKineticaToAvro extends AbstractQueryKineticaProcessor {

    private static final String PROCESSOR_NAME = "QueryKineticaToAvro";
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    // ========== AVRO-SPECIFIC PROPERTY DESCRIPTORS ==========

    public static final PropertyDescriptor PROP_INCLUDE_SCHEMA = new PropertyDescriptor.Builder()
            .name("Include Schema")
            .displayName("Include Schema in Output")
            .description("If true, outputs Avro container format with embedded schema. " +
                    "If false, outputs raw Avro binary (schema must be known by consumer).")
            .required(true)
            .defaultValue("true")
            .allowableValues("true", "false")
            .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
            .build();

    public static final PropertyDescriptor PROP_AVRO_NAMESPACE = new PropertyDescriptor.Builder()
            .name("Avro Namespace")
            .displayName("Avro Namespace")
            .description("Namespace for the generated Avro schema.")
            .required(false)
            .defaultValue("com.kinetica.avro")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    // ========== STATE ==========

    private boolean includeSchema;
    private String avroNamespace;
    private Schema avroSchema;

    @Override
    protected List<PropertyDescriptor> getAdditionalPropertyDescriptors() {
        List<PropertyDescriptor> props = new ArrayList<>();
        props.add(PROP_INCLUDE_SCHEMA);
        props.add(PROP_AVRO_NAMESPACE);
        return props;
    }

    @Override
    @OnScheduled
    public void onScheduled(final ProcessContext context) throws ProcessException {
        super.onScheduled(context);

        includeSchema = context.getProperty(PROP_INCLUDE_SCHEMA).asBoolean();
        avroNamespace = context.getProperty(PROP_AVRO_NAMESPACE).getValue();
        if (avroNamespace == null || avroNamespace.isEmpty()) {
            avroNamespace = "com.kinetica.avro";
        }
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        // Get optional input FlowFile for expression language
        FlowFile inputFlowFile = session.get();

        // If no input FlowFile, create an empty one for expression evaluation
        boolean createdTempFlowFile = false;
        if (inputFlowFile == null) {
            inputFlowFile = session.create();
            createdTempFlowFile = true;
        }

        // Get and validate SQL query
        String query = context.getProperty(PROP_SQL_QUERY)
                .evaluateAttributeExpressions(inputFlowFile)
                .getValue();

        validateSqlQuery(query);

        final long startTime = System.currentTimeMillis();
        final long[] recordCount = {0};
        final Schema[] schemaHolder = {null};
        final FlowFile originalInputFlowFile = createdTempFlowFile ? null : inputFlowFile;
        final FlowFile exprEvalFlowFile = inputFlowFile;

        try {
            // Create output FlowFile
            FlowFile outputFlowFile = session.create(originalInputFlowFile);

            // Stream results to FlowFile
            outputFlowFile = session.write(outputFlowFile, new OutputStreamCallback() {
                @Override
                public void process(OutputStream out) throws IOException {
                    try {
                        recordCount[0] = executeQueryToAvro(query, out, schemaHolder);
                    } catch (GPUdbException e) {
                        throw new IOException("Query execution failed: " + e.getMessage(), e);
                    }
                }
            });

            // Set attributes
            outputFlowFile = session.putAttribute(outputFlowFile, "record.count",
                    String.valueOf(recordCount[0]));
            outputFlowFile = session.putAttribute(outputFlowFile, "mime.type", "application/avro-binary");
            if (schemaHolder[0] != null) {
                outputFlowFile = session.putAttribute(outputFlowFile, "avro.schema",
                        schemaHolder[0].toString());
            }

            final long duration = System.currentTimeMillis() - startTime;

            session.getProvenanceReporter().modifyContent(outputFlowFile,
                    "Executed query returning " + recordCount[0] + " records", duration);

            session.transfer(outputFlowFile, REL_SUCCESS);

            // Remove input/temp FlowFile
            session.remove(exprEvalFlowFile);

            getLogger().info("{}: Query returned {} records in {}ms",
                    PROCESSOR_NAME, recordCount[0], duration);

        } catch (Exception e) {
            getLogger().error("{}: Query failed: {}", PROCESSOR_NAME, e.getMessage(), e);

            // Remove or transfer the input FlowFile
            if (createdTempFlowFile) {
                session.remove(exprEvalFlowFile);
            } else {
                session.transfer(exprEvalFlowFile, REL_FAILURE);
            }

            // Rollback the session to clean up any created FlowFiles that weren't transferred
            session.rollback();
        }
    }

    /**
     * Executes query and writes results to Avro.
     * Automatically chooses between streaming and traditional pagination based on configuration.
     */
    private long executeQueryToAvro(String query, OutputStream out, Schema[] schemaHolder)
            throws IOException, GPUdbException {
        if (useStreaming) {
            return executeQueryToAvroStreaming(query, out, schemaHolder);
        } else {
            return executeQueryToAvroPaginated(query, out, schemaHolder);
        }
    }

    /**
     * Executes query using streaming mode with server-side paging tables.
     * This is more efficient for large result sets as it avoids re-executing the query.
     */
    private long executeQueryToAvroStreaming(String query, OutputStream out, Schema[] schemaHolder)
            throws IOException, GPUdbException {
        long recordCount = 0;
        Schema schema = null;
        Type kineticaType = null;

        getLogger().debug("{}: Using streaming mode with server-side paging tables", PROCESSOR_NAME);

        try (StreamingQueryResult result = createStreamingQuery(query)) {

            if (includeSchema) {
                // Avro container format with schema
                DataFileWriter<GenericRecord> dataFileWriter = null;

                try {
                    for (Record record : result) {
                        if (maxRecords > 0 && recordCount >= maxRecords) break;

                        // Create schema from first record
                        if (schema == null) {
                            kineticaType = record.getType();
                            schema = createAvroSchema(kineticaType, "QueryResult");
                            schemaHolder[0] = schema;

                            DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<>(schema);
                            dataFileWriter = new DataFileWriter<>(datumWriter);
                            dataFileWriter.create(schema, out);
                        }

                        GenericRecord avroRecord = convertToAvroRecord(schema, record, kineticaType);
                        dataFileWriter.append(avroRecord);
                        recordCount++;
                    }
                } finally {
                    if (dataFileWriter != null) {
                        dataFileWriter.close();
                    }
                }
            } else {
                // Raw Avro binary without schema
                DatumWriter<GenericRecord> datumWriter = null;
                BinaryEncoder encoder = null;

                for (Record record : result) {
                    if (maxRecords > 0 && recordCount >= maxRecords) break;

                    // Create schema from first record
                    if (schema == null) {
                        kineticaType = record.getType();
                        schema = createAvroSchema(kineticaType, "QueryResult");
                        schemaHolder[0] = schema;
                        datumWriter = new GenericDatumWriter<>(schema);
                        encoder = EncoderFactory.get().binaryEncoder(out, null);
                    }

                    GenericRecord avroRecord = convertToAvroRecord(schema, record, kineticaType);
                    datumWriter.write(avroRecord, encoder);
                    recordCount++;
                }

                if (encoder != null) {
                    encoder.flush();
                }
            }

            getLogger().info("{}: Streaming query completed. Processed {} of {} total records",
                    PROCESSOR_NAME, recordCount, result.getTotalCount());

        } catch (Exception e) {
            if (e instanceof IOException) {
                throw (IOException) e;
            }
            if (e instanceof GPUdbException) {
                throw (GPUdbException) e;
            }
            throw new GPUdbException("Streaming query failed: " + e.getMessage(), e);
        }

        return recordCount;
    }

    /**
     * Executes query using traditional pagination (re-executes query for each page).
     * This is the default mode for backward compatibility.
     */
    private long executeQueryToAvroPaginated(String query, OutputStream out, Schema[] schemaHolder)
            throws IOException, GPUdbException {
        long recordCount = 0;
        long offset = 0;
        Schema schema = null;
        Type kineticaType = null;

        if (includeSchema) {
            // Avro container format with schema
            DataFileWriter<GenericRecord> dataFileWriter = null;

            try {
                while (true) {
                    int limit = calculateLimit(recordCount);
                    if (limit <= 0) break;

                    ExecuteSqlResponse response = gpudb.executeSql(query, offset, limit, null, null, null);

                    // Get schema from first response
                    if (schema == null && response.getDataType() != null) {
                        kineticaType = response.getDataType();
                        schema = createAvroSchema(kineticaType, "QueryResult");
                        schemaHolder[0] = schema;

                        DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<>(schema);
                        dataFileWriter = new DataFileWriter<>(datumWriter);
                        dataFileWriter.create(schema, out);
                    }

                    List<Record> records = response.getData();
                    if (records == null || records.isEmpty()) break;

                    for (Record record : records) {
                        GenericRecord avroRecord = convertToAvroRecord(schema, record, kineticaType);
                        dataFileWriter.append(avroRecord);
                        recordCount++;
                    }

                    if (!response.getHasMoreRecords()) break;
                    offset += records.size();
                }
            } finally {
                if (dataFileWriter != null) {
                    dataFileWriter.close();
                }
            }
        } else {
            // Raw Avro binary without schema
            DatumWriter<GenericRecord> datumWriter = null;
            BinaryEncoder encoder = null;

            while (true) {
                int limit = calculateLimit(recordCount);
                if (limit <= 0) break;

                ExecuteSqlResponse response = gpudb.executeSql(query, offset, limit, null, null, null);

                // Get schema from first response
                if (schema == null && response.getDataType() != null) {
                    kineticaType = response.getDataType();
                    schema = createAvroSchema(kineticaType, "QueryResult");
                    schemaHolder[0] = schema;
                    datumWriter = new GenericDatumWriter<>(schema);
                    encoder = EncoderFactory.get().binaryEncoder(out, null);
                }

                List<Record> records = response.getData();
                if (records == null || records.isEmpty()) break;

                for (Record record : records) {
                    GenericRecord avroRecord = convertToAvroRecord(schema, record, kineticaType);
                    datumWriter.write(avroRecord, encoder);
                    recordCount++;
                }

                if (!response.getHasMoreRecords()) break;
                offset += records.size();
            }

            if (encoder != null) {
                encoder.flush();
            }
        }

        return recordCount;
    }

    /**
     * Creates an Avro schema from a Kinetica Type.
     */
    private Schema createAvroSchema(Type type, String recordName) {
        List<Schema.Field> fields = new ArrayList<>();

        for (Column column : type.getColumns()) {
            Schema fieldSchema = mapKineticaTypeToAvro(column);

            // Make nullable if column is nullable
            if (column.isNullable()) {
                fieldSchema = Schema.createUnion(Schema.create(Schema.Type.NULL), fieldSchema);
            }

            Schema.Field field = new Schema.Field(
                    column.getName(),
                    fieldSchema,
                    null,  // doc
                    null   // default value
            );
            fields.add(field);
        }

        return Schema.createRecord(
                recordName,
                "Auto-generated from Kinetica query",
                avroNamespace,
                false,  // isError
                fields
        );
    }

    /**
     * Maps a Kinetica column type to an Avro schema.
     * Handles special types like DECIMAL and ARRAY.
     */
    private Schema mapKineticaTypeToAvro(Column column) {
        // Handle DECIMAL type - use Avro's decimal logical type or string
        if (column.isDecimal()) {
            // Use string for decimal to preserve precision
            // Alternatively, could use Avro's decimal logical type with bytes
            return Schema.create(Schema.Type.STRING);
        }

        // Handle ARRAY type - create Avro array schema
        if (column.isArray()) {
            try {
                Column.ColumnType arrayElementType = column.getArrayType();
                Schema elementSchema = mapArrayElementTypeToAvro(arrayElementType);
                return Schema.createArray(elementSchema);
            } catch (Exception e) {
                // Fall back to string (JSON) if array type detection fails
                getLogger().warn("{}: Could not determine array element type for column '{}', using string",
                        PROCESSOR_NAME, column.getName());
                return Schema.create(Schema.Type.STRING);
            }
        }

        Class<?> type = column.getType();

        if (type == String.class) {
            return Schema.create(Schema.Type.STRING);
        } else if (type == Integer.class) {
            return Schema.create(Schema.Type.INT);
        } else if (type == Long.class) {
            return Schema.create(Schema.Type.LONG);
        } else if (type == Float.class) {
            return Schema.create(Schema.Type.FLOAT);
        } else if (type == Double.class) {
            return Schema.create(Schema.Type.DOUBLE);
        } else if (type == ByteBuffer.class || type == byte[].class) {
            return Schema.create(Schema.Type.BYTES);
        } else {
            // Default to string for unknown types
            return Schema.create(Schema.Type.STRING);
        }
    }

    /**
     * Maps a Kinetica array element type to an Avro schema.
     */
    private Schema mapArrayElementTypeToAvro(Column.ColumnType elementType) {
        if (elementType == null) {
            return Schema.create(Schema.Type.STRING);
        }

        switch (elementType) {
            case INTEGER:
            case BOOLEAN:
                return Schema.create(Schema.Type.INT);
            case LONG:
            case ULONG:
                return Schema.create(Schema.Type.LONG);
            case FLOAT:
                return Schema.create(Schema.Type.FLOAT);
            case DOUBLE:
                return Schema.create(Schema.Type.DOUBLE);
            case BYTES:
                return Schema.create(Schema.Type.BYTES);
            case STRING:
            default:
                return Schema.create(Schema.Type.STRING);
        }
    }

    /**
     * Converts a Kinetica Record to an Avro GenericRecord.
     * Handles special types like DECIMAL and ARRAY.
     */
    private GenericRecord convertToAvroRecord(Schema schema, Record kineticaRecord, Type type) {
        GenericRecord avroRecord = new GenericData.Record(schema);

        for (Schema.Field field : schema.getFields()) {
            String fieldName = field.name();
            Object value = kineticaRecord.get(fieldName);
            Column column = type.getColumn(fieldName);

            if (value == null) {
                avroRecord.put(fieldName, null);
            } else if (column != null && column.isDecimal()) {
                // DECIMAL is stored as String in Kinetica, pass through
                avroRecord.put(fieldName, value.toString());
            } else if (column != null && column.isArray()) {
                // ARRAY is stored as JSON string in Kinetica, convert to Avro array
                Object arrayValue = convertJsonToAvroArray(value, field.schema(), column);
                avroRecord.put(fieldName, arrayValue);
            } else if (value instanceof ByteBuffer) {
                avroRecord.put(fieldName, value);
            } else if (value instanceof byte[]) {
                avroRecord.put(fieldName, ByteBuffer.wrap((byte[]) value));
            } else {
                avroRecord.put(fieldName, value);
            }
        }

        return avroRecord;
    }

    /**
     * Converts a JSON array string from Kinetica to an Avro array.
     */
    private Object convertJsonToAvroArray(Object value, Schema fieldSchema, Column column) {
        if (value == null) {
            return null;
        }

        // Get the actual array schema (unwrap union if nullable)
        Schema arraySchema = fieldSchema;
        if (fieldSchema.getType() == Schema.Type.UNION) {
            for (Schema unionType : fieldSchema.getTypes()) {
                if (unionType.getType() == Schema.Type.ARRAY) {
                    arraySchema = unionType;
                    break;
                }
            }
        }

        // If not an array schema, return as string
        if (arraySchema.getType() != Schema.Type.ARRAY) {
            return value.toString();
        }

        try {
            String jsonValue = value.toString();

            // Parse JSON array
            List<?> jsonList = OBJECT_MAPPER.readValue(jsonValue, new TypeReference<List<Object>>() {});

            // Create Avro array
            Schema elementSchema = arraySchema.getElementType();
            GenericArray<Object> avroArray = new GenericData.Array<>(jsonList.size(), arraySchema);

            for (Object element : jsonList) {
                if (element == null) {
                    avroArray.add(null);
                } else {
                    avroArray.add(convertJsonElementToAvro(element, elementSchema));
                }
            }

            return avroArray;

        } catch (Exception e) {
            getLogger().warn("{}: Could not parse array JSON '{}' for column '{}': {}",
                    PROCESSOR_NAME, value, column.getName(), e.getMessage());
            // Return as string if parsing fails
            return value.toString();
        }
    }

    /**
     * Converts a JSON element to the appropriate Avro type.
     */
    private Object convertJsonElementToAvro(Object element, Schema elementSchema) {
        if (element == null) {
            return null;
        }

        switch (elementSchema.getType()) {
            case INT:
                if (element instanceof Number) {
                    return ((Number) element).intValue();
                }
                return Integer.parseInt(element.toString());

            case LONG:
                if (element instanceof Number) {
                    return ((Number) element).longValue();
                }
                return Long.parseLong(element.toString());

            case FLOAT:
                if (element instanceof Number) {
                    return ((Number) element).floatValue();
                }
                return Float.parseFloat(element.toString());

            case DOUBLE:
                if (element instanceof Number) {
                    return ((Number) element).doubleValue();
                }
                return Double.parseDouble(element.toString());

            case BOOLEAN:
                if (element instanceof Boolean) {
                    return element;
                }
                return Boolean.parseBoolean(element.toString());

            case BYTES:
                if (element instanceof byte[]) {
                    return ByteBuffer.wrap((byte[]) element);
                } else if (element instanceof String) {
                    // Assume base64 encoded
                    return ByteBuffer.wrap(java.util.Base64.getDecoder().decode((String) element));
                }
                return ByteBuffer.wrap(element.toString().getBytes());

            case STRING:
            default:
                return element.toString();
        }
    }

    /**
     * Calculates the limit for the next page.
     */
    private int calculateLimit(long recordCount) {
        int limit = pageSize;
        if (maxRecords > 0 && recordCount + pageSize > maxRecords) {
            limit = (int) (maxRecords - recordCount);
        }
        return limit;
    }
}
