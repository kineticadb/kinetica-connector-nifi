package com.kinetica.nifi.processors.base;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;
import java.util.regex.Pattern;

import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;

import com.gpudb.GPUdbException;
import com.gpudb.GPUdbSqlIterator;
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

    public static final PropertyDescriptor PROP_USE_STREAMING = new PropertyDescriptor.Builder()
            .name("Use Streaming Mode")
            .displayName("Use Streaming Mode")
            .description("If true, uses Kinetica's GPUdbSqlIterator with server-side paging tables " +
                    "for memory-efficient streaming of large result sets. " +
                    "This avoids re-executing the query for each page and automatically cleans up " +
                    "temporary tables. Recommended for queries returning more than 100K records.")
            .required(false)
            .defaultValue("false")
            .allowableValues("true", "false")
            .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
            .build();

    public static final PropertyDescriptor PROP_PAGING_TABLE_TTL = new PropertyDescriptor.Builder()
            .name("Paging Table TTL")
            .displayName("Paging Table TTL (seconds)")
            .description("Time-to-live in seconds for server-side paging tables when streaming mode is enabled. " +
                    "The paging table will be automatically deleted after this duration if not cleaned up normally. " +
                    "Set higher values for long-running queries.")
            .required(false)
            .defaultValue("300")
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .dependsOn(PROP_USE_STREAMING, "true")
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
    protected volatile boolean useStreaming;
    protected volatile int pagingTableTtl;

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
        props.add(PROP_USE_STREAMING);
        props.add(PROP_PAGING_TABLE_TTL);
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
        useStreaming = context.getProperty(PROP_USE_STREAMING).asBoolean();
        pagingTableTtl = context.getProperty(PROP_PAGING_TABLE_TTL).asInteger();

        getLogger().info("Query processor configured: pageSize={}, maxRecords={}, streaming={}, pagingTTL={}",
                pageSize, maxRecords, useStreaming, pagingTableTtl);
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

    // ========== STREAMING EXECUTION ==========

    /**
     * Creates a streaming iterator for executing a SQL query.
     *
     * <p>This method uses Kinetica's GPUdbSqlIterator which:
     * <ul>
     *   <li>Creates server-side paging tables to store query results</li>
     *   <li>Fetches records in configurable batches (page size)</li>
     *   <li>Automatically cleans up paging tables when closed</li>
     *   <li>Avoids re-executing the query for each page</li>
     * </ul>
     *
     * <p><strong>IMPORTANT:</strong> The returned iterator MUST be closed after use
     * to clean up server-side paging tables. Use try-with-resources pattern.
     *
     * @param query The SQL query to execute
     * @return StreamingQueryResult containing iterator and metadata
     * @throws GPUdbException if query execution fails
     */
    protected StreamingQueryResult createStreamingQuery(String query) throws GPUdbException {
        Map<String, String> sqlOptions = new HashMap<>();

        // Set paging table TTL for automatic cleanup if not closed properly
        sqlOptions.put("paging_table_ttl", String.valueOf(pagingTableTtl));

        GPUdbSqlIterator<Record> iterator = new GPUdbSqlIterator<>(
                gpudb,
                query,
                pageSize,
                sqlOptions
        );

        return new StreamingQueryResult(iterator);
    }

    /**
     * Executes a streaming query and processes each record with a consumer.
     *
     * <p>This is the recommended method for processing large result sets as it:
     * <ul>
     *   <li>Automatically manages the iterator lifecycle</li>
     *   <li>Ensures proper cleanup of server-side paging tables</li>
     *   <li>Respects the maxRecords limit</li>
     *   <li>Provides record count tracking</li>
     * </ul>
     *
     * @param query The SQL query to execute
     * @param recordProcessor Consumer function to process each record
     * @return Total number of records processed
     * @throws GPUdbException if query execution fails
     */
    protected long executeStreamingQuery(String query, Consumer<Record> recordProcessor)
            throws GPUdbException {

        long recordCount = 0;

        try (StreamingQueryResult result = createStreamingQuery(query)) {
            for (Record record : result) {
                // Check max records limit
                if (maxRecords > 0 && recordCount >= maxRecords) {
                    break;
                }

                recordProcessor.accept(record);
                recordCount++;
            }

            getLogger().debug("Streaming query processed {} records (total available: {})",
                    recordCount, result.getTotalCount());

        } catch (Exception e) {
            if (e instanceof GPUdbException) {
                throw (GPUdbException) e;
            }
            throw new GPUdbException("Error during streaming query: " + e.getMessage(), e);
        }

        return recordCount;
    }

    /**
     * Result wrapper for streaming SQL queries.
     *
     * <p>This class wraps GPUdbSqlIterator and provides:
     * <ul>
     *   <li>AutoCloseable for try-with-resources pattern</li>
     *   <li>Iterable for enhanced for-loop support</li>
     *   <li>Access to total record count</li>
     * </ul>
     *
     * <p><strong>Usage example:</strong>
     * <pre>
     * try (StreamingQueryResult result = createStreamingQuery(query)) {
     *     for (Record record : result) {
     *         // Process record
     *     }
     * }
     * </pre>
     */
    protected static class StreamingQueryResult implements Iterable<Record>, AutoCloseable {
        private final GPUdbSqlIterator<Record> iterator;

        public StreamingQueryResult(GPUdbSqlIterator<Record> iterator) {
            this.iterator = iterator;
        }

        /**
         * Returns the total number of records matching the query.
         * This is available after the first batch is fetched.
         */
        public long getTotalCount() {
            return iterator.size();
        }

        @Override
        public Iterator<Record> iterator() {
            return iterator.iterator();
        }

        @Override
        public void close() throws Exception {
            iterator.close();
        }
    }
}
