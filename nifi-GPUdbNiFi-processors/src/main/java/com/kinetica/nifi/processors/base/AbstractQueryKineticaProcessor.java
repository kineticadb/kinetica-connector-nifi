package com.kinetica.nifi.processors.base;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Pattern;

import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;

import com.gpudb.GPUdbException;
import com.gpudb.Record;
import com.gpudb.Type;
import com.gpudb.protocol.ExecuteSqlResponse;

/**
 * Abstract base class for Kinetica Query processors (QueryKineticaToCSV, QueryKineticaToJSON, etc.).
 *
 * <p>This class extends AbstractKineticaProcessor with functionality specific to
 * querying data from Kinetica:
 * <ul>
 *   <li>SQL query execution with validation</li>
 *   <li>Pagination for large result sets</li>
 *   <li>Result streaming to avoid memory issues</li>
 *   <li>Common Query processor properties</li>
 * </ul>
 *
 * @author Kinetica Engineering
 * @version 7.2.0.0
 * @since 7.2.0.0
 */
public abstract class AbstractQueryKineticaProcessor extends AbstractKineticaProcessor {

    // ========== QUERY-SPECIFIC PROPERTY DESCRIPTORS ==========

    public static final PropertyDescriptor PROP_SQL_QUERY = new PropertyDescriptor.Builder()
            .name("SQL Query")
            .displayName("SQL Query")
            .description("The SQL SELECT query to execute. " +
                    "Only SELECT statements are allowed for safety. " +
                    "Supports Expression Language for dynamic queries.")
            .required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor PROP_PAGE_SIZE = new PropertyDescriptor.Builder()
            .name("Page Size")
            .displayName("Page Size")
            .description("Number of records to fetch per page. " +
                    "Larger values improve throughput but use more memory.")
            .required(true)
            .defaultValue("10000")
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .build();

    public static final PropertyDescriptor PROP_MAX_RECORDS = new PropertyDescriptor.Builder()
            .name("Max Records")
            .displayName("Maximum Records")
            .description("Maximum number of records to return. Use -1 for unlimited.")
            .required(false)
            .defaultValue("-1")
            .addValidator(StandardValidators.INTEGER_VALIDATOR)
            .build();

    // ========== RELATIONSHIPS ==========

    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("FlowFiles containing query results")
            .build();

    public static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("FlowFiles that failed to process")
            .build();

    // ========== SQL VALIDATION ==========

    /**
     * Dangerous SQL keywords that are not allowed in queries.
     */
    private static final List<String> DANGEROUS_KEYWORDS = List.of(
            "drop table", "drop schema", "drop database",
            "truncate", "delete from", "update ",
            "insert into", "create table", "alter table",
            "grant ", "revoke ", "exec ", "execute "
    );

    /**
     * Pattern to validate query starts with SELECT.
     */
    private static final Pattern SELECT_PATTERN = Pattern.compile(
            "^\\s*select\\s+", Pattern.CASE_INSENSITIVE
    );

    // ========== SHARED STATE ==========

    protected volatile String sqlQuery;
    protected volatile int pageSize;
    protected volatile int maxRecords;

    private List<PropertyDescriptor> descriptors;
    private Set<Relationship> relationships;

    // ========== INITIALIZATION ==========

    @Override
    protected void init(org.apache.nifi.processor.ProcessorInitializationContext context) {
        // Build property descriptors
        List<PropertyDescriptor> props = new ArrayList<>();
        props.addAll(getBasePropertyDescriptors());
        props.add(PROP_SQL_QUERY);
        props.add(PROP_PAGE_SIZE);
        props.add(PROP_MAX_RECORDS);
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

        // Read Query-specific configuration
        pageSize = context.getProperty(PROP_PAGE_SIZE).evaluateAttributeExpressions().asInteger();
        maxRecords = context.getProperty(PROP_MAX_RECORDS).asInteger();

        getLogger().info("Query processor configured: pageSize={}, maxRecords={}",
                pageSize, maxRecords);
    }

    // ========== SQL VALIDATION ==========

    /**
     * Validates that a SQL query is safe to execute.
     * Only SELECT queries are allowed.
     *
     * @param query The SQL query to validate
     * @throws ProcessException if the query is invalid or dangerous
     */
    protected void validateSqlQuery(String query) throws ProcessException {
        if (query == null || query.trim().isEmpty()) {
            throw new ProcessException("SQL query cannot be null or empty");
        }

        String normalizedQuery = query.toLowerCase().trim();

        // Check for dangerous keywords
        for (String keyword : DANGEROUS_KEYWORDS) {
            if (normalizedQuery.contains(keyword)) {
                throw new ProcessException(
                        "SQL query contains dangerous operation: '" + keyword + "'. " +
                                "Only SELECT queries are allowed in Query processors."
                );
            }
        }

        // Verify starts with SELECT
        if (!SELECT_PATTERN.matcher(query).find()) {
            throw new ProcessException(
                    "Only SELECT queries are allowed. Query must start with 'SELECT'."
            );
        }
    }

    // ========== QUERY EXECUTION ==========

    /**
     * Executes a SQL query and returns the result type.
     *
     * @param query The SQL query to execute
     * @return The Type describing the result schema
     * @throws GPUdbException if query execution fails
     */
    protected Type executeQueryAndGetType(String query) throws GPUdbException {
        // Execute with limit 1 to get schema
        ExecuteSqlResponse response = gpudb.executeSql(
                query,
                0,   // offset
                1,   // limit - just need schema
                null,
                null,
                null
        );

        // Get the response table type
        Type dataType = response.getDataType();
        if (dataType != null) {
            return dataType;
        }

        throw new GPUdbException("Unable to determine result schema for query");
    }

    /**
     * Result holder for paginated query execution.
     */
    protected static class QueryPage {
        public final List<Record> records;
        public final boolean hasMore;
        public final long totalCount;

        public QueryPage(List<Record> records, boolean hasMore, long totalCount) {
            this.records = records;
            this.hasMore = hasMore;
            this.totalCount = totalCount;
        }
    }

    /**
     * Executes a paginated SQL query.
     *
     * @param query The SQL query
     * @param offset The starting offset
     * @param limit The maximum records to return
     * @return QueryPage containing results and pagination info
     * @throws GPUdbException if query execution fails
     */
    protected QueryPage executePaginatedQuery(String query, long offset, int limit)
            throws GPUdbException {

        ExecuteSqlResponse response = gpudb.executeSql(
                query,
                offset,
                limit,
                null,
                null,
                null
        );

        // Convert response to records
        List<Record> records = new ArrayList<>();
        // Note: The actual record extraction depends on response format
        // This is a simplified version - actual implementation would parse response data

        boolean hasMore = response.getHasMoreRecords();
        long totalCount = response.getTotalNumberOfRecords();

        return new QueryPage(records, hasMore, totalCount);
    }

    /**
     * Formats a record value for output.
     *
     * @param value The value to format
     * @return String representation
     */
    protected String formatValue(Object value) {
        if (value == null) {
            return "";
        }
        return value.toString();
    }
}
