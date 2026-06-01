package com.kinetica.nifi.processors;

import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

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

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.gpudb.BulkInserter;
import com.gpudb.GPUdbException;
import com.gpudb.Record;
import com.gpudb.Type;
import com.gpudb.Type.Column;
import com.kinetica.nifi.processors.base.AbstractPutKineticaProcessor;

/**
 * NiFi processor that bulk loads JSON file contents to Kinetica.
 *
 * <p>This processor reads JSON files (array of objects or newline-delimited JSON)
 * and inserts the records into a Kinetica table.
 *
 * <p>Key features:
 * <ul>
 *   <li>Supports JSON array format: [{...}, {...}, ...]</li>
 *   <li>Supports newline-delimited JSON (NDJSON): {...}\n{...}\n...</li>
 *   <li>Streaming parser for memory efficiency with large files</li>
 *   <li>Batch insertion for high throughput</li>
 *   <li>Error handling (skip or fail on bad records)</li>
 * </ul>
 *
 * @author Kinetica Engineering
 * @version 7.2.0.0
 * @since 7.2.0.0
 */
@Tags({"Kinetica", "add", "bulkadd", "put", "json", "insert"})
@CapabilityDescription("Bulk loads JSON file contents to Kinetica. " +
        "Supports JSON array format ([{...}, {...}]) or newline-delimited JSON (NDJSON). " +
        "Each JSON object's fields should match the table column names. " +
        "For best performance with large files, use NDJSON format.")
@ReadsAttribute(attribute = "mime.type", description = "Determines MIME type of input file")
@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
public class PutKineticaFromJSON extends AbstractPutKineticaProcessor {

    private static final String PROCESSOR_NAME = "PutKineticaFromJSON";

    // ========== JSON-SPECIFIC PROPERTY DESCRIPTORS ==========

    public static final PropertyDescriptor PROP_JSON_FORMAT = new PropertyDescriptor.Builder()
            .name("JSON Format")
            .displayName("JSON Format")
            .description("Format of the JSON input. " +
                    "ARRAY: Standard JSON array [{...}, {...}]. " +
                    "NDJSON: Newline-delimited JSON, one object per line.")
            .required(true)
            .defaultValue("ARRAY")
            .allowableValues("ARRAY", "NDJSON")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

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

    private boolean isNdjson;
    private boolean skipErrors;
    private ObjectMapper objectMapper;
    private JsonFactory jsonFactory;

    @Override
    protected List<PropertyDescriptor> getAdditionalPropertyDescriptors() {
        List<PropertyDescriptor> props = new ArrayList<>();
        props.add(PROP_JSON_FORMAT);
        props.add(PROP_SKIP_ERRORS);
        return props;
    }

    @Override
    @OnScheduled
    public void onScheduled(final ProcessContext context) throws ProcessException {
        super.onScheduled(context);

        isNdjson = "NDJSON".equals(context.getProperty(PROP_JSON_FORMAT).getValue());
        skipErrors = context.getProperty(PROP_SKIP_ERRORS).asBoolean();
        objectMapper = new ObjectMapper();
        jsonFactory = new JsonFactory();
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

            try (InputStream inputStream = session.read(flowFile);
                 Reader reader = new InputStreamReader(inputStream, StandardCharsets.UTF_8)) {

                if (isNdjson) {
                    // Process newline-delimited JSON
                    java.io.BufferedReader bufferedReader = new java.io.BufferedReader(reader);
                    String line;
                    int lineNum = 0;

                    while ((line = bufferedReader.readLine()) != null) {
                        lineNum++;
                        line = line.trim();
                        if (line.isEmpty()) {
                            continue;
                        }

                        try {
                            JsonNode jsonNode = objectMapper.readTree(line);
                            Record record = createRecordFromJson(type, jsonNode, lineNum);

                            if (record != null) {
                                bulkInserter.insert(record);
                                recordCount++;
                            } else if (!skipErrors) {
                                throw new ProcessException("Failed to create record at line " + lineNum);
                            } else {
                                errorCount++;
                            }
                        } catch (Exception e) {
                            errorCount++;
                            if (!skipErrors) {
                                throw new ProcessException(PROCESSOR_NAME + " error at line " +
                                        lineNum + ": " + e.getMessage(), e);
                            }
                            getLogger().warn("{}: Skipping line {}: {}",
                                    PROCESSOR_NAME, lineNum, e.getMessage());
                        }
                    }
                } else {
                    // Process JSON array using streaming parser
                    try (JsonParser parser = jsonFactory.createParser(reader)) {
                        // Expect array start
                        JsonToken token = parser.nextToken();
                        if (token != JsonToken.START_ARRAY) {
                            throw new ProcessException("Expected JSON array, got: " + token);
                        }

                        int recordNum = 0;
                        while (parser.nextToken() != JsonToken.END_ARRAY) {
                            recordNum++;

                            try {
                                JsonNode jsonNode = objectMapper.readTree(parser);
                                Record record = createRecordFromJson(type, jsonNode, recordNum);

                                if (record != null) {
                                    bulkInserter.insert(record);
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
                }

                // Flush remaining records
                bulkInserter.flush();
                success = true;

            } catch (java.io.IOException e) {
                throw new ProcessException("Error reading JSON file: " + e.getMessage(), e);
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
     * Creates a Kinetica Record from a JSON node.
     */
    private Record createRecordFromJson(Type type, JsonNode jsonNode, int recordNum) {
        Record record = type.newInstance();

        for (Column column : type.getColumns()) {
            String columnName = column.getName();
            JsonNode valueNode = jsonNode.get(columnName);

            try {
                setColumnValue(record, column, valueNode);
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
     * Sets a column value on a record from a JSON node.
     */
    private void setColumnValue(Record record, Column column, JsonNode valueNode) throws GPUdbException {
        String columnName = column.getName();

        // Handle null or missing values
        if (valueNode == null || valueNode.isNull()) {
            if (column.isNullable()) {
                record.put(columnName, null);
            }
            return;
        }

        // Check for timestamp column
        boolean isTimestamp = KineticaUtilities.checkForTimeStamp(column);

        if (isTimestamp) {
            String value = valueNode.asText();
            Long timestamp = KineticaUtilities.parseDateOrTimestamp(value, dateFormat, timeZone, getLogger());
            if (timestamp != null) {
                record.put(columnName, timestamp);
            } else {
                throw new GPUdbException("Invalid timestamp value: " + value);
            }
        } else if (column.getType() == Double.class) {
            record.put(columnName, valueNode.asDouble());
        } else if (column.getType() == Float.class) {
            record.put(columnName, (float) valueNode.asDouble());
        } else if (column.getType() == Integer.class) {
            record.put(columnName, valueNode.asInt());
        } else if (column.getType() == Long.class) {
            record.put(columnName, valueNode.asLong());
        } else {
            // String type
            record.put(columnName, valueNode.asText());
        }
    }
}
