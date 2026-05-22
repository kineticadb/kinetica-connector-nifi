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
 * NiFi processor that executes SQL queries on Kinetica and outputs results as CSV.
 *
 * <p>This processor runs SELECT queries against Kinetica and streams the results
 * to CSV-formatted FlowFiles.
 *
 * <p>Key features:
 * <ul>
 *   <li>Streaming output for large result sets</li>
 *   <li>Configurable delimiter and quoting</li>
 *   <li>Optional header row</li>
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
@Tags({"Kinetica", "query", "select", "csv", "export"})
@CapabilityDescription("Executes SQL SELECT queries on Kinetica and outputs results as CSV. " +
        "Results are streamed to avoid memory issues with large result sets. " +
        "Enable 'Use Streaming Mode' for queries returning more than 100K records - " +
        "this uses server-side paging tables for better performance. " +
        "Only SELECT queries are allowed for security.")
@WritesAttributes({
        @WritesAttribute(attribute = "record.count", description = "Number of records in the output"),
        @WritesAttribute(attribute = "mime.type", description = "Set to text/csv")
})
@InputRequirement(InputRequirement.Requirement.INPUT_ALLOWED)
public class QueryKineticaToCSV extends AbstractQueryKineticaProcessor {

    private static final String PROCESSOR_NAME = "QueryKineticaToCSV";

    // ========== CSV-SPECIFIC PROPERTY DESCRIPTORS ==========

    public static final PropertyDescriptor PROP_DELIMITER = new PropertyDescriptor.Builder()
            .name("Delimiter")
            .displayName("Delimiter")
            .description("Field delimiter character for CSV output.")
            .required(true)
            .defaultValue(",")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor PROP_INCLUDE_HEADER = new PropertyDescriptor.Builder()
            .name("Include Header")
            .displayName("Include Header")
            .description("If true, include column names as the first row.")
            .required(true)
            .defaultValue("true")
            .allowableValues("true", "false")
            .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
            .build();

    public static final PropertyDescriptor PROP_QUOTE_CHAR = new PropertyDescriptor.Builder()
            .name("Quote Character")
            .displayName("Quote Character")
            .description("Character to quote fields containing special characters.")
            .required(false)
            .defaultValue("\"")
            .addValidator(new StandardValidators.StringLengthValidator(0, 1))
            .build();

    // ========== STATE ==========

    private char delimiter;
    private boolean includeHeader;
    private Character quoteChar;

    @Override
    protected List<PropertyDescriptor> getAdditionalPropertyDescriptors() {
        List<PropertyDescriptor> props = new ArrayList<>();
        props.add(PROP_DELIMITER);
        props.add(PROP_INCLUDE_HEADER);
        props.add(PROP_QUOTE_CHAR);
        return props;
    }

    @Override
    public void onScheduled(final ProcessContext context) throws ProcessException {
        super.onScheduled(context);

        String delimStr = context.getProperty(PROP_DELIMITER).getValue();
        delimiter = KineticaUtilities.parseSpecialChar(delimStr, ',');

        includeHeader = context.getProperty(PROP_INCLUDE_HEADER).asBoolean();

        String quoteStr = context.getProperty(PROP_QUOTE_CHAR).getValue();
        quoteChar = (quoteStr != null && !quoteStr.isEmpty()) ? quoteStr.charAt(0) : null;
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        // Get optional input FlowFile for expression language
        FlowFile inputFlowFile = session.get();

        // Get and validate SQL query
        String query;
        if (inputFlowFile != null) {
            query = context.getProperty(PROP_SQL_QUERY)
                    .evaluateAttributeExpressions(inputFlowFile)
                    .getValue();
        } else {
            query = context.getProperty(PROP_SQL_QUERY)
                    .evaluateAttributeExpressions()
                    .getValue();
        }

        validateSqlQuery(query);

        final long startTime = System.currentTimeMillis();
        final long[] recordCount = {0};

        try {
            // Create output FlowFile
            FlowFile outputFlowFile = (inputFlowFile != null)
                    ? session.create(inputFlowFile)
                    : session.create();

            // Stream results to FlowFile
            outputFlowFile = session.write(outputFlowFile, new OutputStreamCallback() {
                @Override
                public void process(OutputStream out) throws IOException {
                    try (Writer writer = new OutputStreamWriter(out, StandardCharsets.UTF_8)) {
                        recordCount[0] = executeQueryToCSV(query, writer);
                    } catch (GPUdbException e) {
                        throw new IOException("Query execution failed: " + e.getMessage(), e);
                    }
                }
            });

            // Set attributes
            outputFlowFile = session.putAttribute(outputFlowFile, "record.count",
                    String.valueOf(recordCount[0]));
            outputFlowFile = session.putAttribute(outputFlowFile, "mime.type", "text/csv");

            final long duration = System.currentTimeMillis() - startTime;

            session.getProvenanceReporter().modifyContent(outputFlowFile,
                    "Executed query returning " + recordCount[0] + " records", duration);

            session.transfer(outputFlowFile, REL_SUCCESS);

            // Remove input FlowFile if it exists
            if (inputFlowFile != null) {
                session.remove(inputFlowFile);
            }

            getLogger().info("{}: Query returned {} records in {}ms",
                    PROCESSOR_NAME, recordCount[0], duration);

        } catch (Exception e) {
            getLogger().error("{}: Query failed: {}", PROCESSOR_NAME, e.getMessage(), e);

            if (inputFlowFile != null) {
                session.transfer(inputFlowFile, REL_FAILURE);
            }
        }
    }

    /**
     * Executes query and writes results to CSV.
     * Automatically chooses between streaming and traditional pagination based on configuration.
     */
    private long executeQueryToCSV(String query, Writer writer) throws IOException, GPUdbException {
        if (useStreaming) {
            return executeQueryToCSVStreaming(query, writer);
        } else {
            return executeQueryToCSVPaginated(query, writer);
        }
    }

    /**
     * Executes query using streaming mode with server-side paging tables.
     * This is more efficient for large result sets as it avoids re-executing the query.
     */
    private long executeQueryToCSVStreaming(String query, Writer writer) throws IOException, GPUdbException {
        long recordCount = 0;
        boolean headerWritten = false;
        List<String> columnNames = null;

        getLogger().debug("{}: Using streaming mode with server-side paging tables", PROCESSOR_NAME);

        try (StreamingQueryResult result = createStreamingQuery(query)) {
            for (Record record : result) {
                // Check max records limit
                if (maxRecords > 0 && recordCount >= maxRecords) {
                    getLogger().debug("{}: Reached max records limit: {}", PROCESSOR_NAME, maxRecords);
                    break;
                }

                // Get column names from first record
                if (columnNames == null) {
                    Type recordType = record.getType();
                    columnNames = new ArrayList<>();
                    for (Column col : recordType.getColumns()) {
                        columnNames.add(col.getName());
                    }

                    // Write header if needed
                    if (includeHeader) {
                        writeCSVRow(writer, columnNames);
                        headerWritten = true;
                    }
                }

                // Write data row
                List<String> values = new ArrayList<>();
                for (String colName : columnNames) {
                    Object value = record.get(colName);
                    values.add(formatValue(value));
                }
                writeCSVRow(writer, values);
                recordCount++;
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
    private long executeQueryToCSVPaginated(String query, Writer writer) throws IOException, GPUdbException {
        long recordCount = 0;
        long offset = 0;
        boolean headerWritten = false;
        List<String> columnNames = null;

        while (true) {
            // Check max records limit
            int limit = pageSize;
            if (maxRecords > 0 && recordCount + pageSize > maxRecords) {
                limit = (int) (maxRecords - recordCount);
            }

            if (limit <= 0) {
                break;
            }

            // Execute paginated query
            ExecuteSqlResponse response = gpudb.executeSql(
                    query,
                    offset,
                    limit,
                    null,
                    null,
                    null
            );

            // Get column names from the data type on first page
            if (columnNames == null) {
                Type dataType = response.getDataType();
                if (dataType != null) {
                    columnNames = new ArrayList<>();
                    for (Column col : dataType.getColumns()) {
                        columnNames.add(col.getName());
                    }
                }
            }

            // Write header if needed
            if (!headerWritten && includeHeader && columnNames != null) {
                writeCSVRow(writer, columnNames);
                headerWritten = true;
            }

            // Get records from response
            List<Record> records = response.getData();
            if (records == null || records.isEmpty()) {
                break;
            }

            // Write data rows
            for (Record record : records) {
                List<String> values = new ArrayList<>();
                if (columnNames != null) {
                    for (String colName : columnNames) {
                        Object value = record.get(colName);
                        values.add(formatValue(value));
                    }
                }
                writeCSVRow(writer, values);
                recordCount++;
            }

            // Check if more records
            if (!response.getHasMoreRecords()) {
                break;
            }

            offset += records.size();
        }

        return recordCount;
    }

    /**
     * Writes a CSV row with proper quoting.
     */
    private void writeCSVRow(Writer writer, List<String> values) throws IOException {
        for (int i = 0; i < values.size(); i++) {
            if (i > 0) {
                writer.write(delimiter);
            }
            writeCSVField(writer, values.get(i));
        }
        writer.write('\n');
    }

    /**
     * Writes a single CSV field with quoting if necessary.
     */
    private void writeCSVField(Writer writer, String value) throws IOException {
        if (value == null) {
            return;
        }

        boolean needsQuoting = quoteChar != null &&
                (value.indexOf(delimiter) >= 0 ||
                        value.indexOf('\n') >= 0 ||
                        value.indexOf('\r') >= 0 ||
                        value.indexOf(quoteChar) >= 0);

        if (needsQuoting) {
            writer.write(quoteChar);
            // Escape quote characters by doubling them
            for (char c : value.toCharArray()) {
                if (c == quoteChar) {
                    writer.write(quoteChar);
                }
                writer.write(c);
            }
            writer.write(quoteChar);
        } else {
            writer.write(value);
        }
    }
}
