package com.kinetica.nifi.processors;

import java.io.InputStream;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.generic.GenericArray;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.ReadsAttribute;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;

import com.gpudb.BulkInserter;
import com.gpudb.GPUdbException;
import com.gpudb.Record;
import com.gpudb.Type;
import com.gpudb.Type.Column;
import com.kinetica.nifi.processors.base.AbstractPutKineticaProcessor;

/**
 * NiFi processor that bulk loads Avro file contents to Kinetica.
 *
 * <p>This processor reads Avro files (container format with embedded schema)
 * and inserts the records into a Kinetica table.
 *
 * <p>Key features:
 * <ul>
 *   <li>Supports Avro container format with embedded schema</li>
 *   <li>Streaming parser for memory efficiency with large files</li>
 *   <li>Automatic type mapping from Avro to Kinetica types</li>
 *   <li>Batch insertion for high throughput</li>
 *   <li>Error handling (skip or fail on bad records)</li>
 * </ul>
 *
 * @author Kinetica Engineering
 * @version 7.2.0.0
 * @since 7.2.0.0
 */
@Tags({"Kinetica", "add", "bulkadd", "put", "avro", "insert"})
@CapabilityDescription("Bulk loads Avro file contents to Kinetica. " +
        "Reads Avro container format files with embedded schema. " +
        "Each Avro record's fields should match the table column names. " +
        "For best performance with large files, use appropriate batch sizes.")
@ReadsAttribute(attribute = "mime.type", description = "Determines MIME type of input file")
@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
public class PutKineticaFromAvro extends AbstractPutKineticaProcessor {

    private static final String PROCESSOR_NAME = "PutKineticaFromAvro";
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    // ========== AVRO-SPECIFIC PROPERTY DESCRIPTORS ==========

    public static final PropertyDescriptor PROP_SKIP_ERRORS = new PropertyDescriptor.Builder()
            .name("Skip Errors")
            .displayName("Skip Errors")
            .description("If true, invalid records are skipped and processing continues. " +
                    "If false, the entire file fails on the first error.")
            .required(true)
            .defaultValue("true")
            .allowableValues("true", "false")
            .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
            .build();

    // ========== STATE ==========

    private boolean skipErrors;

    @Override
    protected List<PropertyDescriptor> getAdditionalPropertyDescriptors() {
        List<PropertyDescriptor> props = new ArrayList<>();
        props.add(PROP_SKIP_ERRORS);
        return props;
    }

    @Override
    @OnScheduled
    public void onScheduled(final ProcessContext context) throws ProcessException {
        super.onScheduled(context);

        skipErrors = context.getProperty(PROP_SKIP_ERRORS).asBoolean();
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }

        // Validate table exists
        if (!tableExists(tableName)) {
            if (objectType == null) {
                throw new ProcessException(PROCESSOR_NAME + ": Table '" + tableName +
                        "' does not exist and no schema provided to create it.");
            }
        }

        // Statistics
        int recordCount = 0;
        int errorCount = 0;
        final long startTime = System.currentTimeMillis();

        BulkInserter<Record> bulkInserter = null;
        boolean success = false;

        try {
            bulkInserter = createBulkInserter();
            Type type = getObjectType();

            try (InputStream inputStream = session.read(flowFile)) {
                // Create Avro reader
                DatumReader<GenericRecord> datumReader = new GenericDatumReader<>();
                try (DataFileStream<GenericRecord> dataFileStream =
                        new DataFileStream<>(inputStream, datumReader)) {

                    Schema avroSchema = dataFileStream.getSchema();
                    getLogger().debug("{}: Reading Avro file with schema: {}",
                            PROCESSOR_NAME, avroSchema.getName());

                    int recordNum = 0;
                    while (dataFileStream.hasNext()) {
                        recordNum++;
                        GenericRecord avroRecord = dataFileStream.next();

                        try {
                            Record kineticaRecord = createRecordFromAvro(type, avroRecord, recordNum);

                            if (kineticaRecord != null) {
                                bulkInserter.insert(kineticaRecord);
                                recordCount++;
                            } else if (!skipErrors) {
                                throw new ProcessException("Failed to create record " + recordNum);
                            } else {
                                errorCount++;
                            }
                        } catch (Exception e) {
                            errorCount++;
                            if (!skipErrors) {
                                throw new ProcessException(PROCESSOR_NAME + " error at record " +
                                        recordNum + ": " + e.getMessage(), e);
                            }
                            getLogger().warn("{}: Skipping record {}: {}",
                                    PROCESSOR_NAME, recordNum, e.getMessage());
                        }
                    }
                }

                // Flush remaining records
                bulkInserter.flush();
                success = true;

            } catch (java.io.IOException e) {
                throw new ProcessException("Error reading Avro file: " + e.getMessage(), e);
            }

        } catch (BulkInserter.InsertException e) {
            getLogger().error("{}: Bulk insert error: {}", PROCESSOR_NAME, e.getMessage(), e);
            success = false;
        } catch (GPUdbException e) {
            throw new ProcessException("Kinetica error: " + e.getMessage(), e);
        }

        final long duration = System.currentTimeMillis() - startTime;

        // Handle results
        if (success) {
            session.getProvenanceReporter().send(flowFile,
                    gpudb.getURL().toString() + "/" + tableName,
                    "Inserted " + recordCount + " records", duration);
            session.transfer(flowFile, REL_SUCCESS);

            getLogger().info("{}: Loaded {} records ({} errors) to table '{}' in {}ms",
                    PROCESSOR_NAME, recordCount, errorCount, tableName, duration);
        } else {
            session.transfer(flowFile, REL_FAILURE);
        }
    }

    /**
     * Creates a Kinetica Record from an Avro GenericRecord.
     */
    private Record createRecordFromAvro(Type type, GenericRecord avroRecord, int recordNum) {
        Record record = type.newInstance();

        for (Column column : type.getColumns()) {
            String columnName = column.getName();
            Object avroValue = avroRecord.get(columnName);

            try {
                setColumnValueFromAvro(record, column, avroValue);
            } catch (Exception e) {
                if (skipErrors) {
                    getLogger().warn("{}: Error in record {}, column '{}': {}",
                            PROCESSOR_NAME, recordNum, columnName, e.getMessage());
                    return null;
                }
                throw new ProcessException("Error in record " + recordNum +
                        ", column '" + columnName + "': " + e.getMessage(), e);
            }
        }

        return record;
    }

    /**
     * Sets a column value on a record from an Avro value.
     */
    private void setColumnValueFromAvro(Record record, Column column, Object avroValue) throws GPUdbException {
        String columnName = column.getName();

        // Handle null values
        if (avroValue == null) {
            if (column.isNullable()) {
                record.put(columnName, null);
            }
            return;
        }

        // Check for special column types using ColumnType enum
        Column.ColumnType columnType = column.getColumnType();

        // Handle DECIMAL type
        if (column.isDecimal()) {
            String decimalValue = convertToDecimalString(avroValue, column.getDecimalScale());
            record.put(columnName, decimalValue);
            return;
        }

        // Handle ARRAY type
        if (column.isArray()) {
            String arrayValue = convertToArrayJson(avroValue, column);
            record.put(columnName, arrayValue);
            return;
        }

        // Check for timestamp column
        boolean isTimestamp = KineticaUtilities.checkForTimeStamp(column);

        if (isTimestamp) {
            // Handle timestamp - could be Long or String in Avro
            Long timestamp;
            if (avroValue instanceof Long) {
                timestamp = (Long) avroValue;
            } else {
                String value = avroValue.toString();
                timestamp = KineticaUtilities.parseDateOrTimestamp(value, dateFormat, timeZone, getLogger());
            }
            if (timestamp != null) {
                record.put(columnName, timestamp);
            } else {
                throw new GPUdbException("Invalid timestamp value: " + avroValue);
            }
        } else if (column.getType() == Double.class) {
            if (avroValue instanceof Number) {
                record.put(columnName, ((Number) avroValue).doubleValue());
            } else {
                record.put(columnName, Double.parseDouble(avroValue.toString()));
            }
        } else if (column.getType() == Float.class) {
            if (avroValue instanceof Number) {
                record.put(columnName, ((Number) avroValue).floatValue());
            } else {
                record.put(columnName, Float.parseFloat(avroValue.toString()));
            }
        } else if (column.getType() == Integer.class) {
            if (avroValue instanceof Number) {
                record.put(columnName, ((Number) avroValue).intValue());
            } else {
                record.put(columnName, Integer.parseInt(avroValue.toString()));
            }
        } else if (column.getType() == Long.class) {
            if (avroValue instanceof Number) {
                record.put(columnName, ((Number) avroValue).longValue());
            } else {
                record.put(columnName, Long.parseLong(avroValue.toString()));
            }
        } else if (column.getType() == ByteBuffer.class || column.getType() == byte[].class) {
            // Handle bytes
            if (avroValue instanceof ByteBuffer) {
                record.put(columnName, avroValue);
            } else if (avroValue instanceof byte[]) {
                record.put(columnName, ByteBuffer.wrap((byte[]) avroValue));
            } else {
                record.put(columnName, ByteBuffer.wrap(avroValue.toString().getBytes()));
            }
        } else {
            // String type - handle CharSequence from Avro
            if (avroValue instanceof CharSequence) {
                record.put(columnName, avroValue.toString());
            } else {
                record.put(columnName, avroValue.toString());
            }
        }
    }

    /**
     * Converts a value to a properly formatted decimal string for Kinetica.
     * Kinetica stores DECIMAL as a string with the correct scale.
     *
     * @param value the input value (Number, BigDecimal, or String)
     * @param scale the decimal scale for the column
     * @return a properly scaled decimal string
     */
    private String convertToDecimalString(Object value, int scale) {
        if (value == null) {
            return null;
        }

        BigDecimal bd;
        if (value instanceof BigDecimal) {
            bd = (BigDecimal) value;
        } else if (value instanceof Double) {
            bd = BigDecimal.valueOf((Double) value);
        } else if (value instanceof Float) {
            bd = BigDecimal.valueOf((Float) value);
        } else if (value instanceof Number) {
            bd = new BigDecimal(value.toString());
        } else if (value instanceof String) {
            // Already a string - just return as is if it's a valid number
            try {
                bd = new BigDecimal((String) value);
            } catch (NumberFormatException e) {
                return (String) value;
            }
        } else {
            // Try to convert to string
            return value.toString();
        }

        // Apply scale with rounding
        bd = bd.setScale(scale, RoundingMode.HALF_UP);
        // Format without trailing zeros after decimal point
        return bd.toPlainString();
    }

    /**
     * Converts an Avro array value to a JSON array string for Kinetica.
     * Kinetica stores ARRAY columns as JSON strings.
     *
     * @param value the input value (Avro array, Collection, or array)
     * @param column the column metadata
     * @return a JSON array string
     */
    private String convertToArrayJson(Object value, Column column) throws GPUdbException {
        if (value == null) {
            return null;
        }

        try {
            List<Object> elements = new ArrayList<>();

            if (value instanceof GenericArray) {
                // Avro GenericArray
                GenericArray<?> avroArray = (GenericArray<?>) value;
                for (Object element : avroArray) {
                    elements.add(convertArrayElement(element));
                }
            } else if (value instanceof Collection) {
                // Java Collection
                Collection<?> collection = (Collection<?>) value;
                for (Object element : collection) {
                    elements.add(convertArrayElement(element));
                }
            } else if (value.getClass().isArray()) {
                // Java array
                if (value instanceof Object[]) {
                    for (Object element : (Object[]) value) {
                        elements.add(convertArrayElement(element));
                    }
                } else if (value instanceof int[]) {
                    for (int element : (int[]) value) {
                        elements.add(element);
                    }
                } else if (value instanceof long[]) {
                    for (long element : (long[]) value) {
                        elements.add(element);
                    }
                } else if (value instanceof double[]) {
                    for (double element : (double[]) value) {
                        elements.add(element);
                    }
                } else if (value instanceof float[]) {
                    for (float element : (float[]) value) {
                        elements.add(element);
                    }
                } else if (value instanceof boolean[]) {
                    for (boolean element : (boolean[]) value) {
                        elements.add(element);
                    }
                }
            } else if (value instanceof String) {
                // Already a JSON string - return as is
                return (String) value;
            } else {
                // Try to convert to string representation
                return value.toString();
            }

            // Convert to JSON array string
            return OBJECT_MAPPER.writeValueAsString(elements);

        } catch (Exception e) {
            throw new GPUdbException("Failed to convert array value: " + e.getMessage(), e);
        }
    }

    /**
     * Converts an individual array element to the appropriate Java type.
     */
    private Object convertArrayElement(Object element) {
        if (element == null) {
            return null;
        }
        if (element instanceof CharSequence) {
            return element.toString();
        }
        if (element instanceof ByteBuffer) {
            // Convert bytes to base64 string for JSON
            ByteBuffer buffer = (ByteBuffer) element;
            byte[] bytes = new byte[buffer.remaining()];
            buffer.get(bytes);
            return java.util.Base64.getEncoder().encodeToString(bytes);
        }
        return element;
    }
}
