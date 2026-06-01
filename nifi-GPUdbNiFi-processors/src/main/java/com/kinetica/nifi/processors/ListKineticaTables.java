package com.kinetica.nifi.processors;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.TriggerSerially;
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
import com.gpudb.protocol.ShowTableRequest;
import com.gpudb.protocol.ShowTableResponse;
import com.kinetica.nifi.processors.base.AbstractKineticaProcessor;

/**
 * NiFi processor that lists tables in a Kinetica database.
 *
 * <p>This processor queries Kinetica for table metadata and emits a FlowFile
 * for each table found. This is useful for:
 * <ul>
 *   <li>Dynamic table discovery</li>
 *   <li>Metadata-driven pipelines</li>
 *   <li>Schema documentation</li>
 *   <li>Monitoring table counts and sizes</li>
 * </ul>
 *
 * <p>The processor can list all tables, tables in a specific schema,
 * or filter by table name pattern.
 *
 * @author Kinetica Engineering
 * @version 7.2.0.0
 * @since 7.2.0.0
 */
@Tags({"Kinetica", "list", "tables", "metadata", "schema", "discovery"})
@CapabilityDescription("Lists tables in a Kinetica database. " +
        "Emits one FlowFile per table with metadata attributes including " +
        "table name, schema, row count, and column information. " +
        "Useful for dynamic table discovery and metadata-driven workflows.")
@WritesAttributes({
        @WritesAttribute(attribute = "kinetica.table.name", description = "Fully qualified table name"),
        @WritesAttribute(attribute = "kinetica.table.schema", description = "Schema/collection name"),
        @WritesAttribute(attribute = "kinetica.table.type", description = "Table type (TABLE, VIEW, COLLECTION, etc.)"),
        @WritesAttribute(attribute = "kinetica.table.row_count", description = "Number of rows in the table"),
        @WritesAttribute(attribute = "kinetica.table.type_id", description = "Kinetica type ID"),
        @WritesAttribute(attribute = "kinetica.table.type_schema", description = "Avro schema definition"),
        @WritesAttribute(attribute = "mime.type", description = "Set to application/json")
})
@InputRequirement(InputRequirement.Requirement.INPUT_FORBIDDEN)
@TriggerSerially
public class ListKineticaTables extends AbstractKineticaProcessor {

    private static final String PROCESSOR_NAME = "ListKineticaTables";

    // ========== PROPERTY DESCRIPTORS ==========

    public static final PropertyDescriptor PROP_TABLE_PATTERN = new PropertyDescriptor.Builder()
            .name("Table Name Pattern")
            .displayName("Table Name Pattern")
            .description("Pattern to match table names. Use '*' to list all tables, " +
                    "or specify a schema name to list tables in that schema (e.g., 'my_schema'). " +
                    "Can also use wildcards like 'my_schema.sales_*'.")
            .required(true)
            .defaultValue("*")
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor PROP_INCLUDE_SIZES = new PropertyDescriptor.Builder()
            .name("Include Sizes")
            .displayName("Include Table Sizes")
            .description("If true, include row counts and size information for each table. " +
                    "This may increase query time for databases with many tables.")
            .required(false)
            .defaultValue("true")
            .allowableValues("true", "false")
            .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
            .build();

    public static final PropertyDescriptor PROP_INCLUDE_CHILDREN = new PropertyDescriptor.Builder()
            .name("Include Children")
            .displayName("Include Child Tables")
            .description("If true, include child tables (views, projections) in the listing.")
            .required(false)
            .defaultValue("true")
            .allowableValues("true", "false")
            .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
            .build();

    public static final PropertyDescriptor PROP_TABLE_TYPE_FILTER = new PropertyDescriptor.Builder()
            .name("Table Type Filter")
            .displayName("Table Type Filter")
            .description("Filter tables by type. Leave empty for all types. " +
                    "Valid types: TABLE, VIEW, COLLECTION, MATERIALIZED_VIEW, REPLICATED, JOIN")
            .required(false)
            .defaultValue("")
            .addValidator(StandardValidators.createRegexMatchingValidator(
                    java.util.regex.Pattern.compile("^(TABLE|VIEW|COLLECTION|MATERIALIZED_VIEW|REPLICATED|JOIN|SCHEMA)?$",
                    java.util.regex.Pattern.CASE_INSENSITIVE)))
            .build();

    // ========== RELATIONSHIPS ==========

    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("FlowFiles containing table metadata")
            .build();

    public static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("FlowFiles that failed to process")
            .build();

    // ========== STATE ==========

    private volatile String tablePattern;
    private volatile boolean includeSizes;
    private volatile boolean includeChildren;
    private volatile String tableTypeFilter;

    private List<PropertyDescriptor> descriptors;
    private Set<Relationship> relationships;

    @Override
    protected void init(org.apache.nifi.processor.ProcessorInitializationContext context) {
        List<PropertyDescriptor> props = new ArrayList<>();
        // Add base properties except PROP_TABLE (this processor doesn't use a specific table)
        props.add(PROP_SERVER);
        props.add(PROP_USERNAME);
        props.add(PROP_PASSWORD);
        props.add(PROP_USE_SSL);
        props.add(PROP_SSL_BYPASS_CERT_CHECK);
        props.add(PROP_CONNECTION_TIMEOUT);
        props.add(PROP_SOCKET_TIMEOUT);
        props.add(PROP_CONNECTION_POOL_SIZE);
        // Processor-specific properties
        props.add(PROP_TABLE_PATTERN);
        props.add(PROP_INCLUDE_SIZES);
        props.add(PROP_INCLUDE_CHILDREN);
        props.add(PROP_TABLE_TYPE_FILTER);
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
        // Initialize connection (skip table validation for list operation)
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

            getLogger().info("Connected to Kinetica server for table listing: {}",
                    gpudb.getURL());
        } catch (com.gpudb.GPUdbException e) {
            throw new ProcessException("Failed to connect to Kinetica: " + e.getMessage(), e);
        }

        tablePattern = context.getProperty(PROP_TABLE_PATTERN)
                .evaluateAttributeExpressions()
                .getValue();
        includeSizes = context.getProperty(PROP_INCLUDE_SIZES).asBoolean();
        includeChildren = context.getProperty(PROP_INCLUDE_CHILDREN).asBoolean();
        tableTypeFilter = context.getProperty(PROP_TABLE_TYPE_FILTER).getValue();

        getLogger().info("ListKineticaTables configured: tablePattern='{}', includeSizes={}, includeChildren={}",
                tablePattern, includeSizes, includeChildren);

        if (tableTypeFilter != null) {
            tableTypeFilter = tableTypeFilter.trim().toUpperCase();
            if (tableTypeFilter.isEmpty()) {
                tableTypeFilter = null;
            }
        }
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        final long startTime = System.currentTimeMillis();
        int tableCount = 0;

        try {
            // Build request options
            Map<String, String> options = new HashMap<>();

            if (includeSizes) {
                options.put(ShowTableRequest.Options.GET_SIZES, ShowTableRequest.Options.TRUE);
            }

            options.put(ShowTableRequest.Options.SHOW_CHILDREN,
                    includeChildren ? ShowTableRequest.Options.TRUE : ShowTableRequest.Options.FALSE);

            // Don't error if pattern doesn't match any tables
            options.put(ShowTableRequest.Options.NO_ERROR_IF_NOT_EXISTS, ShowTableRequest.Options.TRUE);

            // Execute show table request
            // Kinetica's showTable API requires special handling for schema.table_name format:
            // - If pattern is "schema.table_name" (exact table), query the schema and filter
            // - Otherwise pass the pattern directly
            String queryPattern = tablePattern;
            String exactTableFilter = null;

            if (tablePattern != null && tablePattern.contains(".") && !tablePattern.contains("*")) {
                // This looks like an exact table name (schema.table_name)
                // Query the schema and filter for the specific table
                int dotIndex = tablePattern.lastIndexOf('.');
                queryPattern = tablePattern.substring(0, dotIndex);  // Just the schema name
                exactTableFilter = tablePattern;  // The full name to filter for
                getLogger().debug("Pattern '{}' appears to be exact table name, querying schema '{}' and filtering",
                        tablePattern, queryPattern);
            }

            getLogger().debug("Executing showTable with pattern: '{}', options: {}", queryPattern, options);
            ShowTableResponse response = gpudb.showTable(queryPattern, options);
            getLogger().debug("showTable response: {} tables found", response.getTableNames().size());

            List<String> tableNames = response.getTableNames();
            List<List<String>> tableDescriptions = response.getTableDescriptions();
            List<String> typeIds = response.getTypeIds();
            List<String> typeSchemas = response.getTypeSchemas();
            List<Long> sizes = response.getSizes();
            List<Map<String, String>> additionalInfo = response.getAdditionalInfo();

            // If we're filtering for an exact table, find matching indices
            // Note: showTable with schema returns short table names, so we need to handle both cases
            List<Integer> matchingIndices = new ArrayList<>();
            if (exactTableFilter != null) {
                // Extract the short name from the filter for comparison
                String shortFilterName = exactTableFilter;
                if (exactTableFilter.contains(".")) {
                    shortFilterName = exactTableFilter.substring(exactTableFilter.lastIndexOf('.') + 1);
                }

                for (int i = 0; i < tableNames.size(); i++) {
                    String tableName = tableNames.get(i);
                    // Match against either full name or short name
                    if (tableName.equals(exactTableFilter) || tableName.equals(shortFilterName)) {
                        matchingIndices.add(i);
                    }
                }
                getLogger().debug("Filtered from {} to {} tables matching exact name '{}' (short: '{}')",
                        tableNames.size(), matchingIndices.size(), exactTableFilter, shortFilterName);
            } else {
                // No filter - include all tables
                for (int i = 0; i < tableNames.size(); i++) {
                    matchingIndices.add(i);
                }
            }

            // Process each matching table
            for (int i : matchingIndices) {
                String tableName = tableNames.get(i);
                List<String> descriptions = (tableDescriptions != null && i < tableDescriptions.size())
                        ? tableDescriptions.get(i) : new ArrayList<>();

                // Determine table type from descriptions
                String tableType = determineTableType(descriptions);

                // Apply table type filter
                if (tableTypeFilter != null && !tableTypeFilter.isEmpty()) {
                    if (!tableType.contains(tableTypeFilter)) {
                        continue;
                    }
                }

                // Create FlowFile for this table
                FlowFile flowFile = session.create();

                // Extract schema name from fully qualified table name
                // If table name is short (no dot), use the queryPattern as schema if it was a schema query
                String schemaName = "";
                String shortName = tableName;
                if (tableName.contains(".")) {
                    int dotIndex = tableName.lastIndexOf('.');
                    schemaName = tableName.substring(0, dotIndex);
                    shortName = tableName.substring(dotIndex + 1);
                } else if (exactTableFilter != null && queryPattern != null && !queryPattern.equals("*")) {
                    // We queried a specific schema, so prefix the table name with schema
                    schemaName = queryPattern;
                    shortName = tableName;
                    tableName = schemaName + "." + tableName;  // Create fully qualified name
                }

                // Set attributes
                Map<String, String> attributes = new HashMap<>();
                attributes.put("kinetica.table.name", tableName);
                attributes.put("kinetica.table.short_name", shortName);
                attributes.put("kinetica.table.schema", schemaName);
                attributes.put("kinetica.table.type", tableType);
                attributes.put("mime.type", "application/json");

                if (typeIds != null && i < typeIds.size()) {
                    attributes.put("kinetica.table.type_id", typeIds.get(i));
                }

                if (typeSchemas != null && i < typeSchemas.size()) {
                    attributes.put("kinetica.table.type_schema", typeSchemas.get(i));
                }

                if (sizes != null && i < sizes.size()) {
                    attributes.put("kinetica.table.row_count", String.valueOf(sizes.get(i)));
                }

                // Add additional info as attributes
                if (additionalInfo != null && i < additionalInfo.size()) {
                    Map<String, String> info = additionalInfo.get(i);
                    if (info != null) {
                        for (Map.Entry<String, String> entry : info.entrySet()) {
                            attributes.put("kinetica.table.info." + entry.getKey(), entry.getValue());
                        }
                    }
                }

                flowFile = session.putAllAttributes(flowFile, attributes);

                // Write table metadata as JSON content
                String jsonContent = buildTableMetadataJson(tableName, schemaName, tableType,
                        typeIds != null && i < typeIds.size() ? typeIds.get(i) : null,
                        typeSchemas != null && i < typeSchemas.size() ? typeSchemas.get(i) : null,
                        sizes != null && i < sizes.size() ? sizes.get(i) : null);

                flowFile = session.write(flowFile, out ->
                        out.write(jsonContent.getBytes(java.nio.charset.StandardCharsets.UTF_8)));

                session.transfer(flowFile, REL_SUCCESS);
                tableCount++;
            }

            final long duration = System.currentTimeMillis() - startTime;
            getLogger().info("{}: Listed {} tables matching '{}' in {}ms",
                    PROCESSOR_NAME, tableCount, tablePattern, duration);

        } catch (GPUdbException e) {
            getLogger().error("{}: Failed to list tables: {}", PROCESSOR_NAME, e.getMessage(), e);
            throw new ProcessException("Failed to list tables: " + e.getMessage(), e);
        }
    }

    /**
     * Determines the table type from description strings.
     */
    private String determineTableType(List<String> descriptions) {
        if (descriptions == null || descriptions.isEmpty()) {
            return "TABLE";
        }

        // Join descriptions for type determination
        StringBuilder sb = new StringBuilder();
        for (String desc : descriptions) {
            if (sb.length() > 0) {
                sb.append(",");
            }
            sb.append(desc);
        }

        String descStr = sb.toString().toUpperCase();

        if (descStr.contains("COLLECTION")) {
            return "COLLECTION";
        } else if (descStr.contains("MATERIALIZED_VIEW")) {
            return "MATERIALIZED_VIEW";
        } else if (descStr.contains("VIEW")) {
            return "VIEW";
        } else if (descStr.contains("JOIN")) {
            return "JOIN";
        } else if (descStr.contains("REPLICATED")) {
            return "REPLICATED";
        } else if (descStr.contains("SCHEMA")) {
            return "SCHEMA";
        }

        return "TABLE";
    }

    /**
     * Builds JSON representation of table metadata.
     */
    private String buildTableMetadataJson(String tableName, String schemaName, String tableType,
                                          String typeId, String typeSchema, Long rowCount) {
        StringBuilder json = new StringBuilder();
        json.append("{\n");
        json.append("  \"tableName\": \"").append(KineticaUtilities.escapeJson(tableName)).append("\",\n");
        json.append("  \"schemaName\": \"").append(KineticaUtilities.escapeJson(schemaName)).append("\",\n");
        json.append("  \"tableType\": \"").append(tableType).append("\"");

        if (typeId != null) {
            json.append(",\n  \"typeId\": \"").append(KineticaUtilities.escapeJson(typeId)).append("\"");
        }

        if (rowCount != null) {
            json.append(",\n  \"rowCount\": ").append(rowCount);
        }

        if (typeSchema != null) {
            json.append(",\n  \"typeSchema\": ").append(typeSchema);
        }

        json.append("\n}");
        return json.toString();
    }
}
