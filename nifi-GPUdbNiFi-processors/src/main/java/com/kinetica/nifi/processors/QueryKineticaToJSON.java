package com.kinetica.nifi.processors;

import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

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

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.gpudb.GPUdbException;
import com.gpudb.Record;
import com.gpudb.Type;
import com.gpudb.Type.Column;
import com.gpudb.protocol.ExecuteSqlResponse;
import com.kinetica.nifi.processors.base.AbstractQueryKineticaProcessor;

/**
 * NiFi processor that executes SQL queries on Kinetica and outputs results as JSON.
 *
 * <p>This processor runs SELECT queries against Kinetica and streams the results
 * to JSON-formatted FlowFiles.
 *
 * <p>Key features:
 * <ul>
 *   <li>Streaming JSON output for large result sets</li>
 *   <li>Configurable output format (array or NDJSON)</li>
 *   <li>Pretty printing option</li>
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
@Tags({"Kinetica", "query", "select", "json", "export"})
@CapabilityDescription("Executes SQL SELECT queries on Kinetica and outputs results as JSON. " +
        "Results are streamed to avoid memory issues with large result sets. " +
        "Enable 'Use Streaming Mode' for queries returning more than 100K records - " +
        "this uses server-side paging tables for better performance. " +
        "Only SELECT queries are allowed for security.")
@WritesAttributes({
        @WritesAttribute(attribute = "record.count", description = "Number of records in the output"),
        @WritesAttribute(attribute = "mime.type", description = "Set to application/json")
})
@InputRequirement(InputRequirement.Requirement.INPUT_ALLOWED)
public class QueryKineticaToJSON extends AbstractQueryKineticaProcessor {

    private static final String PROCESSOR_NAME = "QueryKineticaToJSON";

    // ========== JSON-SPECIFIC PROPERTY DESCRIPTORS ==========

    public static final PropertyDescriptor PROP_JSON_FORMAT = new PropertyDescriptor.Builder()
            .name("JSON Format")
            .displayName("JSON Format")
            .description("Output format. " +
                    "ARRAY: Standard JSON array [{...}, {...}]. " +
                    "NDJSON: Newline-delimited JSON, one object per line.")
            .required(true)
            .defaultValue("ARRAY")
            .allowableValues("ARRAY", "NDJSON")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor PROP_PRETTY_PRINT = new PropertyDescriptor.Builder()
            .name("Pretty Print")
            .displayName("Pretty Print")
            .description("If true, output formatted JSON with indentation. " +
                    "Only applies to ARRAY format.")
            .required(false)
            .defaultValue("false")
            .allowableValues("true", "false")
            .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
            .build();

    // ========== STATE ==========

    private boolean isNdjson;
    private boolean prettyPrint;
    private JsonFactory jsonFactory;

    @Override
    protected List<PropertyDescriptor> getAdditionalPropertyDescriptors() {
        List<PropertyDescriptor> props = new ArrayList<>();
        props.add(PROP_JSON_FORMAT);
        props.add(PROP_PRETTY_PRINT);
        return props;
    }

    @Override
    @OnScheduled
    public void onScheduled(final ProcessContext context) throws ProcessException {
        super.onScheduled(context);

        isNdjson = "NDJSON".equals(context.getProperty(PROP_JSON_FORMAT).getValue());
        prettyPrint = context.getProperty(PROP_PRETTY_PRINT).asBoolean();
        jsonFactory = new JsonFactory();
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
                        recordCount[0] = executeQueryToJSON(query, out);
                    } catch (GPUdbException e) {
                        throw new IOException("Query execution failed: " + e.getMessage(), e);
                    }
                }
            });

            // Set attributes
            outputFlowFile = session.putAttribute(outputFlowFile, "record.count",
                    String.valueOf(recordCount[0]));
            outputFlowFile = session.putAttribute(outputFlowFile, "mime.type", "application/json");

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
     * Executes query and writes results to JSON.
     * Automatically chooses between streaming and traditional pagination based on configuration.
     */
    private long executeQueryToJSON(String query, OutputStream out) throws IOException, GPUdbException {
        if (useStreaming) {
            return executeQueryToJSONStreaming(query, out);
        } else {
            return executeQueryToJSONPaginated(query, out);
        }
    }

    /**
     * Executes query using streaming mode with server-side paging tables.
     * This is more efficient for large result sets as it avoids re-executing the query.
     */
    private long executeQueryToJSONStreaming(String query, OutputStream out) throws IOException, GPUdbException {
        long recordCount = 0;
        List<String> columnNames = null;

        getLogger().debug("{}: Using streaming mode with server-side paging tables", PROCESSOR_NAME);

        try (StreamingQueryResult result = createStreamingQuery(query)) {
            if (isNdjson) {
                // NDJSON format - one object per line
                try (Writer writer = new OutputStreamWriter(out, StandardCharsets.UTF_8)) {
                    for (Record record : result) {
                        if (maxRecords > 0 && recordCount >= maxRecords) break;

                        // Get column names from first record
                        if (columnNames == null) {
                            columnNames = getColumnNames(record.getType());
                        }

                        writeNdjsonRecord(writer, columnNames, record);
                        recordCount++;
                    }
                }
            } else {
                // JSON array format
                try (JsonGenerator generator = jsonFactory.createGenerator(out)) {
                    if (prettyPrint) {
                        generator.useDefaultPrettyPrinter();
                    }

                    generator.writeStartArray();

                    for (Record record : result) {
                        if (maxRecords > 0 && recordCount >= maxRecords) break;

                        // Get column names from first record
                        if (columnNames == null) {
                            columnNames = getColumnNames(record.getType());
                        }

                        writeJsonRecord(generator, columnNames, record);
                        recordCount++;
                    }

                    generator.writeEndArray();
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
    private long executeQueryToJSONPaginated(String query, OutputStream out) throws IOException, GPUdbException {
        long recordCount = 0;
        long offset = 0;
        List<String> columnNames = null;

        if (isNdjson) {
            // NDJSON format - one object per line
            try (Writer writer = new OutputStreamWriter(out, StandardCharsets.UTF_8)) {
                while (true) {
                    int limit = calculateLimit(recordCount);
                    if (limit <= 0) break;

                    ExecuteSqlResponse response = gpudb.executeSql(query, offset, limit, null, null, null);

                    // Get column names from data type on first page
                    if (columnNames == null) {
                        columnNames = getColumnNames(response.getDataType());
                    }

                    List<Record> records = response.getData();
                    if (records == null || records.isEmpty()) break;

                    for (Record record : records) {
                        writeNdjsonRecord(writer, columnNames, record);
                        recordCount++;
                    }

                    if (!response.getHasMoreRecords()) break;
                    offset += records.size();
                }
            }
        } else {
            // JSON array format
            try (JsonGenerator generator = jsonFactory.createGenerator(out)) {
                if (prettyPrint) {
                    generator.useDefaultPrettyPrinter();
                }

                generator.writeStartArray();

                while (true) {
                    int limit = calculateLimit(recordCount);
                    if (limit <= 0) break;

                    ExecuteSqlResponse response = gpudb.executeSql(query, offset, limit, null, null, null);

                    // Get column names from data type on first page
                    if (columnNames == null) {
                        columnNames = getColumnNames(response.getDataType());
                    }

                    List<Record> records = response.getData();
                    if (records == null || records.isEmpty()) break;

                    for (Record record : records) {
                        writeJsonRecord(generator, columnNames, record);
                        recordCount++;
                    }

                    if (!response.getHasMoreRecords()) break;
                    offset += records.size();
                }

                generator.writeEndArray();
            }
        }

        return recordCount;
    }

    /**
     * Extracts column names from data type.
     */
    private List<String> getColumnNames(Type dataType) {
        List<String> names = new ArrayList<>();
        if (dataType != null) {
            for (Column col : dataType.getColumns()) {
                names.add(col.getName());
            }
        }
        return names;
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

    /**
     * Writes a single record as NDJSON (one JSON object per line).
     */
    private void writeNdjsonRecord(Writer writer, List<String> columnNames, Record record)
            throws IOException {
        writer.write("{");
        for (int i = 0; i < columnNames.size(); i++) {
            if (i > 0) {
                writer.write(",");
            }
            String colName = columnNames.get(i);
            writer.write("\"");
            writer.write(KineticaUtilities.escapeJson(colName));
            writer.write("\":");
            writeJsonValue(writer, record.get(colName));
        }
        writer.write("}\n");
    }

    /**
     * Writes a single record as a JSON object.
     */
    private void writeJsonRecord(JsonGenerator generator, List<String> columnNames, Record record)
            throws IOException {
        generator.writeStartObject();
        for (String columnName : columnNames) {
            Object value = record.get(columnName);

            if (value == null) {
                generator.writeNullField(columnName);
            } else if (value instanceof Number) {
                if (value instanceof Double || value instanceof Float) {
                    generator.writeNumberField(columnName, ((Number) value).doubleValue());
                } else {
                    generator.writeNumberField(columnName, ((Number) value).longValue());
                }
            } else if (value instanceof Boolean) {
                generator.writeBooleanField(columnName, (Boolean) value);
            } else {
                generator.writeStringField(columnName, value.toString());
            }
        }
        generator.writeEndObject();
    }

    /**
     * Writes a JSON value to a writer.
     */
    private void writeJsonValue(Writer writer, Object value) throws IOException {
        if (value == null) {
            writer.write("null");
        } else if (value instanceof Number) {
            writer.write(value.toString());
        } else if (value instanceof Boolean) {
            writer.write(value.toString());
        } else {
            writer.write("\"");
            writer.write(KineticaUtilities.escapeJson(value.toString()));
            writer.write("\"");
        }
    }
}
