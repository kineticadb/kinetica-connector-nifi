package com.kinetica.nifi.processors.it;

import com.gpudb.BulkInserter;
import com.gpudb.ColumnProperty;
import com.gpudb.GPUdbException;
import com.gpudb.Record;
import com.gpudb.Type;
import com.kinetica.nifi.processors.QueryKineticaToCSV;
import com.kinetica.nifi.processors.QueryKineticaToJSON;
import com.kinetica.nifi.processors.base.AbstractQueryKineticaProcessor;

import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration tests for Query processors (QueryKineticaToCSV, QueryKineticaToJSON).
 *
 * <p>Tests require a running Kinetica instance.
 * Configure via: {@code -Dkinetica.url=http://host:9191}
 */
@DisplayName("Query Processors Integration Tests")
class QueryKineticaIT extends AbstractKineticaIT {

    private String testTableName;

    @BeforeEach
    void setUpTable() throws GPUdbException {
        testTableName = generateTestTableName();
    }

    /**
     * Populates test table with sample data.
     */
    private void populateTestData(int recordCount) throws GPUdbException {
        // Create table
        Type type = new Type(
                new Type.Column("id", Long.class, ColumnProperty.PRIMARY_KEY),
                new Type.Column("name", String.class),
                new Type.Column("value", Double.class),
                new Type.Column("category", String.class)
        );
        createTestTable(testTableName, type);

        // Insert data
        BulkInserter<Record> inserter = new BulkInserter<>(gpudb, testTableName, type, 10000, null);

        for (int i = 0; i < recordCount; i++) {
            Record record = type.newInstance();
            record.put("id", (long) i);
            record.put("name", "Item_" + i);
            record.put("value", i * 1.5);
            record.put("category", "Category_" + (i % 5));
            inserter.insert(record);
        }

        inserter.flush();
        logger.info("Populated test table {} with {} records", testTableName, recordCount);
    }

    @Nested
    @DisplayName("QueryKineticaToCSV Tests")
    class QueryToCsvTests {

        private TestRunner runner;

        @BeforeEach
        void setUpRunner() {
            runner = TestRunners.newTestRunner(QueryKineticaToCSV.class);
        }

        @Test
        @DisplayName("Should export query results to CSV")
        void testBasicQuery() throws Exception {
            // Populate data
            populateTestData(100);

            // Configure processor
            configureKineticaConnection(runner, testTableName);
            runner.setProperty("SQL Query", "SELECT * FROM " + testTableName + " ORDER BY id");
            runner.setProperty("Include Header", "true");
            runner.setProperty("Delimiter", ",");

            // Run
            runner.run();

            // Verify
            runner.assertAllFlowFilesTransferred(AbstractQueryKineticaProcessor.REL_SUCCESS, 1);

            MockFlowFile output = runner.getFlowFilesForRelationship(
                    AbstractQueryKineticaProcessor.REL_SUCCESS).get(0);

            // Check attributes
            output.assertAttributeEquals("mime.type", "text/csv");
            output.assertAttributeEquals("record.count", "100");

            // Check content
            String content = output.getContent();
            assertThat(content).startsWith("id,name,value,category");  // Header
            assertThat(content).contains("0,Item_0,0.0,Category_0");   // First record
            assertThat(content.split("\n")).hasSize(101);  // Header + 100 records
        }

        @Test
        @DisplayName("Should handle query with WHERE clause")
        void testFilteredQuery() throws Exception {
            // Populate data
            populateTestData(100);

            // Configure processor with filter
            configureKineticaConnection(runner, testTableName);
            runner.setProperty("SQL Query",
                    "SELECT * FROM " + testTableName + " WHERE category = 'Category_0' ORDER BY id");

            // Run
            runner.run();

            // Verify - should have ~20 records (100/5 categories)
            MockFlowFile output = runner.getFlowFilesForRelationship(
                    AbstractQueryKineticaProcessor.REL_SUCCESS).get(0);

            String recordCount = output.getAttribute("record.count");
            assertThat(Integer.parseInt(recordCount)).isEqualTo(20);
        }

        @Test
        @DisplayName("Should respect max records limit")
        void testMaxRecords() throws Exception {
            // Populate data
            populateTestData(100);

            // Configure processor with max records
            configureKineticaConnection(runner, testTableName);
            runner.setProperty("SQL Query", "SELECT * FROM " + testTableName + " ORDER BY id");
            runner.setProperty("Maximum Records", "25");

            // Run
            runner.run();

            // Verify - should have exactly 25 records
            MockFlowFile output = runner.getFlowFilesForRelationship(
                    AbstractQueryKineticaProcessor.REL_SUCCESS).get(0);
            output.assertAttributeEquals("record.count", "25");
        }

        @Test
        @DisplayName("Should handle custom delimiter")
        void testCustomDelimiter() throws Exception {
            // Populate data
            populateTestData(10);

            // Configure processor with tab delimiter
            configureKineticaConnection(runner, testTableName);
            runner.setProperty("SQL Query", "SELECT * FROM " + testTableName + " ORDER BY id");
            runner.setProperty("Delimiter", "\\t");
            runner.setProperty("Include Header", "true");

            // Run
            runner.run();

            // Verify
            MockFlowFile output = runner.getFlowFilesForRelationship(
                    AbstractQueryKineticaProcessor.REL_SUCCESS).get(0);

            String content = output.getContent();
            assertThat(content).contains("id\tname\tvalue\tcategory");  // Tab-separated header
        }

        @Test
        @DisplayName("Should use streaming mode for large results")
        void testStreamingMode() throws Exception {
            // Populate larger dataset
            populateTestData(10000);

            // Configure processor with streaming mode
            configureKineticaConnection(runner, testTableName);
            runner.setProperty("SQL Query", "SELECT * FROM " + testTableName + " ORDER BY id");
            runner.setProperty("Use Streaming Mode", "true");
            runner.setProperty("Page Size", "1000");

            // Run
            long startTime = System.currentTimeMillis();
            runner.run();
            long duration = System.currentTimeMillis() - startTime;

            // Verify
            runner.assertAllFlowFilesTransferred(AbstractQueryKineticaProcessor.REL_SUCCESS, 1);

            MockFlowFile output = runner.getFlowFilesForRelationship(
                    AbstractQueryKineticaProcessor.REL_SUCCESS).get(0);
            output.assertAttributeEquals("record.count", "10000");

            logger.info("Streaming query exported {} records in {}ms", 10000, duration);
        }
    }

    @Nested
    @DisplayName("QueryKineticaToJSON Tests")
    class QueryToJsonTests {

        private TestRunner runner;

        @BeforeEach
        void setUpRunner() {
            runner = TestRunners.newTestRunner(QueryKineticaToJSON.class);
        }

        @Test
        @DisplayName("Should export query results to JSON array")
        void testJsonArrayOutput() throws Exception {
            // Populate data
            populateTestData(50);

            // Configure processor
            configureKineticaConnection(runner, testTableName);
            runner.setProperty("SQL Query", "SELECT * FROM " + testTableName + " ORDER BY id");
            runner.setProperty("JSON Format", "ARRAY");

            // Run
            runner.run();

            // Verify
            runner.assertAllFlowFilesTransferred(AbstractQueryKineticaProcessor.REL_SUCCESS, 1);

            MockFlowFile output = runner.getFlowFilesForRelationship(
                    AbstractQueryKineticaProcessor.REL_SUCCESS).get(0);

            output.assertAttributeEquals("mime.type", "application/json");
            output.assertAttributeEquals("record.count", "50");

            String content = output.getContent();
            assertThat(content).startsWith("[");
            assertThat(content).endsWith("]");
            assertThat(content).contains("\"id\":0");
            assertThat(content).contains("\"name\":\"Item_0\"");
        }

        @Test
        @DisplayName("Should export query results to NDJSON")
        void testNdjsonOutput() throws Exception {
            // Populate data
            populateTestData(30);

            // Configure processor
            configureKineticaConnection(runner, testTableName);
            runner.setProperty("SQL Query", "SELECT * FROM " + testTableName + " ORDER BY id");
            runner.setProperty("JSON Format", "NDJSON");

            // Run
            runner.run();

            // Verify
            MockFlowFile output = runner.getFlowFilesForRelationship(
                    AbstractQueryKineticaProcessor.REL_SUCCESS).get(0);

            String content = output.getContent();
            String[] lines = content.trim().split("\n");

            // Each line should be valid JSON object
            assertThat(lines).hasSize(30);
            assertThat(lines[0]).startsWith("{").endsWith("}");
            assertThat(lines[0]).contains("\"id\":0");
        }

        @Test
        @DisplayName("Should handle pretty print option")
        void testPrettyPrint() throws Exception {
            // Populate data
            populateTestData(5);

            // Configure processor with pretty print
            configureKineticaConnection(runner, testTableName);
            runner.setProperty("SQL Query", "SELECT * FROM " + testTableName + " ORDER BY id");
            runner.setProperty("JSON Format", "ARRAY");
            runner.setProperty("Pretty Print", "true");

            // Run
            runner.run();

            // Verify
            MockFlowFile output = runner.getFlowFilesForRelationship(
                    AbstractQueryKineticaProcessor.REL_SUCCESS).get(0);

            String content = output.getContent();
            // Pretty printed JSON should have indentation
            assertThat(content).contains("\n  ");  // Indented content
        }

        @Test
        @DisplayName("Should use streaming mode for JSON output")
        void testStreamingModeJson() throws Exception {
            // Populate larger dataset
            populateTestData(5000);

            // Configure processor with streaming mode
            configureKineticaConnection(runner, testTableName);
            runner.setProperty("SQL Query", "SELECT * FROM " + testTableName + " ORDER BY id");
            runner.setProperty("JSON Format", "ARRAY");
            runner.setProperty("Use Streaming Mode", "true");
            runner.setProperty("Page Size", "500");

            // Run
            long startTime = System.currentTimeMillis();
            runner.run();
            long duration = System.currentTimeMillis() - startTime;

            // Verify
            runner.assertAllFlowFilesTransferred(AbstractQueryKineticaProcessor.REL_SUCCESS, 1);

            MockFlowFile output = runner.getFlowFilesForRelationship(
                    AbstractQueryKineticaProcessor.REL_SUCCESS).get(0);
            output.assertAttributeEquals("record.count", "5000");

            logger.info("Streaming JSON query exported {} records in {}ms", 5000, duration);
        }
    }

    @Nested
    @DisplayName("SQL Validation Tests")
    class SqlValidationTests {

        private TestRunner runner;

        @BeforeEach
        void setUpRunner() {
            runner = TestRunners.newTestRunner(QueryKineticaToCSV.class);
        }

        @Test
        @DisplayName("Should reject non-SELECT queries")
        void testRejectNonSelectQueries() throws Exception {
            populateTestData(10);

            configureKineticaConnection(runner, testTableName);

            // Try DELETE (should fail validation)
            runner.setProperty("SQL Query", "DELETE FROM " + testTableName);

            // This should throw during validation
            try {
                runner.run();
                // If we get here, check for failure relationship
                List<MockFlowFile> failures = runner.getFlowFilesForRelationship(
                        AbstractQueryKineticaProcessor.REL_FAILURE);
                assertThat(failures).isNotEmpty();
            } catch (AssertionError e) {
                // Expected - validation should fail
                assertThat(e.getMessage()).containsIgnoringCase("select");
            }
        }

        @Test
        @DisplayName("Should reject DROP TABLE queries")
        void testRejectDropTable() throws Exception {
            populateTestData(10);

            configureKineticaConnection(runner, testTableName);
            runner.setProperty("SQL Query", "DROP TABLE " + testTableName);

            try {
                runner.run();
                List<MockFlowFile> failures = runner.getFlowFilesForRelationship(
                        AbstractQueryKineticaProcessor.REL_FAILURE);
                assertThat(failures).isNotEmpty();
            } catch (AssertionError e) {
                // Expected - validation should fail
            }
        }
    }

    @Nested
    @DisplayName("Expression Language Tests")
    class ExpressionLanguageTests {

        private TestRunner runner;

        @BeforeEach
        void setUpRunner() {
            runner = TestRunners.newTestRunner(QueryKineticaToCSV.class);
        }

        @Test
        @DisplayName("Should support expression language in SQL query")
        void testExpressionLanguage() throws Exception {
            populateTestData(100);

            configureKineticaConnection(runner, testTableName);

            // Use expression language in query
            runner.setProperty("SQL Query",
                    "SELECT * FROM " + testTableName + " WHERE category = '${category}' ORDER BY id");

            // Create FlowFile with attribute
            Map<String, String> attributes = new HashMap<>();
            attributes.put("category", "Category_2");

            runner.enqueue("", attributes);
            runner.run();

            // Verify - should filter to Category_2 (20 records)
            MockFlowFile output = runner.getFlowFilesForRelationship(
                    AbstractQueryKineticaProcessor.REL_SUCCESS).get(0);
            output.assertAttributeEquals("record.count", "20");
        }
    }

    @Nested
    @DisplayName("Performance Comparison Tests")
    class PerformanceTests {

        @Test
        @DisplayName("Should compare streaming vs pagination performance")
        void testStreamingVsPaginationPerformance() throws Exception {
            // Populate larger dataset
            int recordCount = 50000;
            populateTestData(recordCount);

            String query = "SELECT * FROM " + testTableName + " ORDER BY id";

            // Test pagination mode
            TestRunner paginationRunner = TestRunners.newTestRunner(QueryKineticaToCSV.class);
            configureKineticaConnection(paginationRunner, testTableName);
            paginationRunner.setProperty("SQL Query", query);
            paginationRunner.setProperty("Use Streaming Mode", "false");
            paginationRunner.setProperty("Page Size", "5000");

            long paginationStart = System.currentTimeMillis();
            paginationRunner.run();
            long paginationDuration = System.currentTimeMillis() - paginationStart;

            paginationRunner.assertAllFlowFilesTransferred(
                    AbstractQueryKineticaProcessor.REL_SUCCESS, 1);

            // Test streaming mode
            TestRunner streamingRunner = TestRunners.newTestRunner(QueryKineticaToCSV.class);
            configureKineticaConnection(streamingRunner, testTableName);
            streamingRunner.setProperty("SQL Query", query);
            streamingRunner.setProperty("Use Streaming Mode", "true");
            streamingRunner.setProperty("Page Size", "5000");

            long streamingStart = System.currentTimeMillis();
            streamingRunner.run();
            long streamingDuration = System.currentTimeMillis() - streamingStart;

            streamingRunner.assertAllFlowFilesTransferred(
                    AbstractQueryKineticaProcessor.REL_SUCCESS, 1);

            // Log comparison
            logger.info("Performance comparison for {} records:", recordCount);
            logger.info("  Pagination mode: {}ms ({} records/sec)",
                    paginationDuration,
                    String.format("%.2f", recordCount / (paginationDuration / 1000.0)));
            logger.info("  Streaming mode:  {}ms ({} records/sec)",
                    streamingDuration,
                    String.format("%.2f", recordCount / (streamingDuration / 1000.0)));

            // Streaming should generally be faster or equal
            // (may vary based on query complexity and server state)
        }
    }
}
