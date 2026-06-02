package com.kinetica.nifi.processors;

import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
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

import com.gpudb.GPUdbException;
import com.gpudb.protocol.ExecuteSqlResponse;
import com.kinetica.nifi.processors.base.AbstractKineticaProcessor;

/**
 * NiFi processor that executes arbitrary SQL statements against Kinetica.
 *
 * <p>This processor can execute any SQL statement supported by Kinetica including:
 * <ul>
 *   <li>DDL: CREATE TABLE, DROP TABLE, ALTER TABLE, CREATE INDEX</li>
 *   <li>DML: INSERT, UPDATE, DELETE, UPSERT</li>
 *   <li>DCL: GRANT, REVOKE</li>
 *   <li>Utility: TRUNCATE, VACUUM, ANALYZE</li>
 * </ul>
 *
 * <p>For SELECT queries, consider using QueryKineticaToCSV or QueryKineticaToJSON
 * processors which are optimized for result handling.
 *
 * <p>The SQL can be provided either as a property value or from the FlowFile content.
 *
 * @author Kinetica Engineering
 * @version 7.2.0.0
 * @since 7.2.0.0
 */
@Tags({"Kinetica", "SQL", "execute", "DDL", "DML", "database", "update", "insert", "delete", "create"})
@CapabilityDescription("Executes arbitrary SQL statements against a Kinetica database. " +
        "Supports DDL (CREATE, DROP, ALTER), DML (INSERT, UPDATE, DELETE), and utility statements. " +
        "The SQL can be provided as a property or read from the FlowFile content. " +
        "Emits FlowFiles with execution status and affected row counts.")
@ReadsAttributes({
        @ReadsAttribute(attribute = "sql.statement", description = "SQL statement to execute (when using 'From Attribute' mode)")
})
@WritesAttributes({
        @WritesAttribute(attribute = "kinetica.sql.statement", description = "The SQL statement that was executed"),
        @WritesAttribute(attribute = "kinetica.sql.affected_rows", description = "Number of rows affected by the statement"),
        @WritesAttribute(attribute = "kinetica.sql.execution_time_ms", description = "Time taken to execute the statement in milliseconds"),
        @WritesAttribute(attribute = "kinetica.sql.status", description = "Execution status: SUCCESS or ERROR"),
        @WritesAttribute(attribute = "kinetica.sql.error", description = "Error message if execution failed")
})
@InputRequirement(InputRequirement.Requirement.INPUT_ALLOWED)
public class ExecuteKineticaSQL extends AbstractKineticaProcessor {

    private static final String PROCESSOR_NAME = "ExecuteKineticaSQL";

    // ========== SQL SOURCE OPTIONS ==========
    public static final String SQL_SOURCE_PROPERTY = "Property Value";
    public static final String SQL_SOURCE_CONTENT = "FlowFile Content";
    public static final String SQL_SOURCE_ATTRIBUTE = "FlowFile Attribute";

    // ========== PROPERTY DESCRIPTORS ==========

    public static final PropertyDescriptor PROP_SQL_SOURCE = new PropertyDescriptor.Builder()
            .name("SQL Source")
            .displayName("SQL Source")
            .description("Specifies where the SQL statement comes from: " +
                    "'Property Value' uses the SQL Statement property, " +
                    "'FlowFile Content' reads SQL from the FlowFile body, " +
                    "'FlowFile Attribute' reads from the 'sql.statement' attribute.")
            .required(true)
            .defaultValue(SQL_SOURCE_PROPERTY)
            .allowableValues(SQL_SOURCE_PROPERTY, SQL_SOURCE_CONTENT, SQL_SOURCE_ATTRIBUTE)
            .build();

    public static final PropertyDescriptor PROP_SQL_STATEMENT = new PropertyDescriptor.Builder()
            .name("SQL Statement")
            .displayName("SQL Statement")
            .description("The SQL statement to execute. Supports Expression Language. " +
                    "Used when 'SQL Source' is set to 'Property Value'.")
            .required(false)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor PROP_EXECUTION_TIMEOUT = new PropertyDescriptor.Builder()
            .name("Execution Timeout")
            .displayName("Execution Timeout (seconds)")
            .description("Maximum time in seconds to wait for SQL execution to complete. " +
                    "Use 0 for no timeout (not recommended for long-running statements).")
            .required(false)
            .defaultValue("300")
            .addValidator(StandardValidators.NON_NEGATIVE_INTEGER_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();

    public static final PropertyDescriptor PROP_FAIL_ON_ERROR = new PropertyDescriptor.Builder()
            .name("Fail on Error")
            .displayName("Fail on Error")
            .description("If true, route FlowFile to failure on SQL execution error. " +
                    "If false, route to success with error details in attributes.")
            .required(false)
            .defaultValue("true")
            .allowableValues("true", "false")
            .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
            .build();

    public static final PropertyDescriptor PROP_RETURN_RESULTS = new PropertyDescriptor.Builder()
            .name("Return Results")
            .displayName("Return Results as JSON")
            .description("If true and the SQL returns results (e.g., SELECT, SHOW), " +
                    "include results as JSON in the FlowFile content. " +
                    "For large result sets, consider using Query processors instead.")
            .required(false)
            .defaultValue("false")
            .allowableValues("true", "false")
            .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
            .build();

    // ========== RELATIONSHIPS ==========

    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("Successfully executed SQL statements")
            .build();

    public static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("FlowFiles with SQL statements that failed to execute")
            .build();

    // ========== STATE ==========

    private volatile String sqlSource;
    private volatile int executionTimeout;
    private volatile boolean failOnError;
    private volatile boolean returnResults;

    private List<PropertyDescriptor> descriptors;
    private Set<Relationship> relationships;

    @Override
    protected void init(org.apache.nifi.processor.ProcessorInitializationContext context) {
        List<PropertyDescriptor> props = new ArrayList<>();
        // Add base descriptors but remove TABLE since we're using raw SQL
        props.add(PROP_SERVER);
        props.add(PROP_USERNAME);
        props.add(PROP_PASSWORD);
        props.add(PROP_USE_SSL);
        props.add(PROP_SSL_BYPASS_CERT_CHECK);
        props.add(PROP_CONNECTION_TIMEOUT);
        props.add(PROP_SOCKET_TIMEOUT);
        props.add(PROP_CONNECTION_POOL_SIZE);
        // Processor-specific properties
        props.add(PROP_SQL_SOURCE);
        props.add(PROP_SQL_STATEMENT);
        props.add(PROP_EXECUTION_TIMEOUT);
        props.add(PROP_FAIL_ON_ERROR);
        props.add(PROP_RETURN_RESULTS);
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
        // Initialize connection (skip table validation for SQL execution)
        try {
            // Cache SSL/connection configuration
            useSSL = context.getProperty(PROP_USE_SSL).asBoolean();
            bypassCertCheck = context.getProperty(PROP_SSL_BYPASS_CERT_CHECK).asBoolean();
            connectionTimeout = (int) context.getProperty(PROP_CONNECTION_TIMEOUT)
                    .asTimePeriod(java.util.concurrent.TimeUnit.MILLISECONDS).longValue();
            socketTimeout = (int) context.getProperty(PROP_SOCKET_TIMEOUT)
                    .asTimePeriod(java.util.concurrent.TimeUnit.MILLISECONDS).longValue();
            connectionPoolSize = context.getProperty(PROP_CONNECTION_POOL_SIZE).asInteger();

            // Create GPUdb connection
            gpudb = createGPUdbConnection(context);

            getLogger().info("Connected to Kinetica server for SQL execution: {}",
                    gpudb.getURL());
        } catch (com.gpudb.GPUdbException e) {
            throw new ProcessException("Failed to connect to Kinetica: " + e.getMessage(), e);
        }

        // Cache processor-specific settings
        sqlSource = context.getProperty(PROP_SQL_SOURCE).getValue();
        executionTimeout = context.getProperty(PROP_EXECUTION_TIMEOUT).asInteger();
        failOnError = context.getProperty(PROP_FAIL_ON_ERROR).asBoolean();
        returnResults = context.getProperty(PROP_RETURN_RESULTS).asBoolean();
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        FlowFile flowFile = session.get();

        // For property-based SQL without input, we still need to execute
        if (flowFile == null) {
            if (SQL_SOURCE_PROPERTY.equals(sqlSource)) {
                flowFile = session.create();
            } else {
                return; // Need input FlowFile for content or attribute source
            }
        }

        final long startTime = System.currentTimeMillis();
        String sqlStatement = null;

        try {
            // Get SQL statement based on source
            sqlStatement = getSqlStatement(context, session, flowFile);

            if (sqlStatement == null || sqlStatement.trim().isEmpty()) {
                getLogger().error("No SQL statement provided");
                flowFile = session.putAttribute(flowFile, "kinetica.sql.status", "ERROR");
                flowFile = session.putAttribute(flowFile, "kinetica.sql.error", "No SQL statement provided");
                session.transfer(flowFile, REL_FAILURE);
                return;
            }

            // Log the SQL (truncated for long statements)
            String logSql = sqlStatement.length() > 200
                    ? sqlStatement.substring(0, 200) + "..."
                    : sqlStatement;
            getLogger().debug("{}: Executing SQL: {}", PROCESSOR_NAME, logSql);

            // Build execution options
            Map<String, String> options = new HashMap<>();

            // Execute SQL
            ExecuteSqlResponse response = gpudb.executeSql(
                    sqlStatement,
                    0,  // offset
                    -1, // limit (-1 = all, but typically DML doesn't return rows)
                    null, // request_schema_str
                    null, // data
                    options
            );

            long executionTime = System.currentTimeMillis() - startTime;

            // Set success attributes
            flowFile = session.putAttribute(flowFile, "kinetica.sql.statement", truncateForAttribute(sqlStatement));
            flowFile = session.putAttribute(flowFile, "kinetica.sql.affected_rows",
                    String.valueOf(response.getCountAffected()));
            flowFile = session.putAttribute(flowFile, "kinetica.sql.execution_time_ms",
                    String.valueOf(executionTime));
            flowFile = session.putAttribute(flowFile, "kinetica.sql.status", "SUCCESS");

            // If returning results, write to FlowFile content
            if (returnResults && response.getTotalNumberOfRecords() > 0) {
                final String jsonResults = buildResultsJson(response);
                flowFile = session.write(flowFile, (OutputStream out) ->
                        out.write(jsonResults.getBytes(StandardCharsets.UTF_8)));
                flowFile = session.putAttribute(flowFile, "mime.type", "application/json");
                flowFile = session.putAttribute(flowFile, "kinetica.sql.result_count",
                        String.valueOf(response.getTotalNumberOfRecords()));
            }

            session.transfer(flowFile, REL_SUCCESS);

            getLogger().info("{}: Executed SQL successfully. Affected rows: {}, Time: {}ms",
                    PROCESSOR_NAME, response.getCountAffected(), executionTime);

        } catch (GPUdbException e) {
            long executionTime = System.currentTimeMillis() - startTime;
            String errorMessage = e.getMessage();

            getLogger().error("{}: SQL execution failed: {}", PROCESSOR_NAME, errorMessage, e);

            flowFile = session.putAttribute(flowFile, "kinetica.sql.statement",
                    truncateForAttribute(sqlStatement != null ? sqlStatement : ""));
            flowFile = session.putAttribute(flowFile, "kinetica.sql.execution_time_ms",
                    String.valueOf(executionTime));
            flowFile = session.putAttribute(flowFile, "kinetica.sql.status", "ERROR");
            flowFile = session.putAttribute(flowFile, "kinetica.sql.error", errorMessage);

            if (failOnError) {
                session.transfer(flowFile, REL_FAILURE);
            } else {
                session.transfer(flowFile, REL_SUCCESS);
            }
        } catch (Exception e) {
            getLogger().error("{}: Unexpected error: {}", PROCESSOR_NAME, e.getMessage(), e);
            flowFile = session.putAttribute(flowFile, "kinetica.sql.status", "ERROR");
            flowFile = session.putAttribute(flowFile, "kinetica.sql.error", e.getMessage());
            session.transfer(flowFile, REL_FAILURE);
        }
    }

    /**
     * Gets the SQL statement based on the configured source.
     */
    private String getSqlStatement(ProcessContext context, ProcessSession session, FlowFile flowFile) {
        switch (sqlSource) {
            case SQL_SOURCE_PROPERTY:
                return context.getProperty(PROP_SQL_STATEMENT)
                        .evaluateAttributeExpressions(flowFile)
                        .getValue();

            case SQL_SOURCE_CONTENT:
                StringBuilder contentBuilder = new StringBuilder();
                session.read(flowFile, in -> {
                    byte[] buffer = new byte[8192];
                    int bytesRead;
                    while ((bytesRead = in.read(buffer)) != -1) {
                        contentBuilder.append(new String(buffer, 0, bytesRead, StandardCharsets.UTF_8));
                    }
                });
                return contentBuilder.toString().trim();

            case SQL_SOURCE_ATTRIBUTE:
                return flowFile.getAttribute("sql.statement");

            default:
                return null;
        }
    }

    /**
     * Builds a JSON representation of query results.
     */
    private String buildResultsJson(ExecuteSqlResponse response) {
        StringBuilder json = new StringBuilder();
        json.append("{\n");
        json.append("  \"countAffected\": ").append(response.getCountAffected()).append(",\n");
        json.append("  \"totalRecords\": ").append(response.getTotalNumberOfRecords()).append(",\n");

        // Include response info if available
        Map<String, String> info = response.getInfo();
        if (info != null && !info.isEmpty()) {
            json.append("  \"info\": {\n");
            int i = 0;
            for (Map.Entry<String, String> entry : info.entrySet()) {
                if (i > 0) json.append(",\n");
                json.append("    \"").append(KineticaUtilities.escapeJson(entry.getKey()))
                    .append("\": \"").append(KineticaUtilities.escapeJson(entry.getValue())).append("\"");
                i++;
            }
            json.append("\n  }\n");
        } else {
            json.append("  \"info\": {}\n");
        }

        json.append("}");
        return json.toString();
    }

    /**
     * Truncates a string for safe storage in FlowFile attributes.
     */
    private String truncateForAttribute(String value) {
        if (value == null) return "";
        final int MAX_ATTR_LENGTH = 1000;
        if (value.length() <= MAX_ATTR_LENGTH) {
            return value;
        }
        return value.substring(0, MAX_ATTR_LENGTH) + "...[truncated]";
    }
}
