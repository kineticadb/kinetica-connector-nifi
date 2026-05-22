package com.kinetica.nifi.processors.it;

import com.gpudb.ColumnProperty;
import com.gpudb.GPUdbException;
import com.gpudb.Record;
import com.gpudb.Type;
import com.gpudb.protocol.GetRecordsResponse;
import com.kinetica.nifi.processors.PutKineticaFromJSON;
import com.kinetica.nifi.processors.base.AbstractPutKineticaProcessor;

import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration tests for PutKineticaFromJSON processor.
 *
 * <p>Tests require a running Kinetica instance.
 * Configure via: {@code -Dkinetica.url=http://host:9191}
 */
@DisplayName("PutKineticaFromJSON Integration Tests")
class PutKineticaFromJSONIT extends AbstractKineticaIT {

    private TestRunner runner;
    private String testTableName;

    @BeforeEach
    void setUpRunner() throws GPUdbException {
        runner = TestRunners.newTestRunner(PutKineticaFromJSON.class);
        testTableName = generateTestTableName();
    }

    @Nested
    @DisplayName("JSON Array Processing")
    class JsonArrayTests {

        @Test
        @DisplayName("Should ingest JSON array")
        void testJsonArray() throws Exception {
            // Create test table
            createSimpleTestTable(testTableName);

            // Configure processor
            configureKineticaConnection(runner, testTableName);
            runner.setProperty("JSON Format", "ARRAY");

            // Create JSON array content
            String jsonContent = generateTestJsonArrayData(100);

            // Enqueue and run
            runner.enqueue(jsonContent);
            runner.run();

            // Verify success
            runner.assertAllFlowFilesTransferred(AbstractPutKineticaProcessor.REL_SUCCESS, 1);
            assertTableRecordCount(testTableName, 100);
        }

        @Test
        @DisplayName("Should handle empty JSON array")
        void testEmptyJsonArray() throws Exception {
            // Create test table
            createSimpleTestTable(testTableName);

            // Configure processor
            configureKineticaConnection(runner, testTableName);
            runner.setProperty("JSON Format", "ARRAY");

            // Empty array
            runner.enqueue("[]");
            runner.run();

            // Should succeed with 0 records
            runner.assertAllFlowFilesTransferred(AbstractPutKineticaProcessor.REL_SUCCESS, 1);
            assertTableRecordCount(testTableName, 0);
        }

        @Test
        @DisplayName("Should handle nested JSON objects")
        void testNestedJsonObjects() throws Exception {
            // Create table with nested-friendly schema
            Type type = new Type(
                    new Type.Column("id", Long.class, ColumnProperty.PRIMARY_KEY),
                    new Type.Column("name", String.class),
                    new Type.Column("metadata", String.class)  // Store nested as string
            );
            createTestTable(testTableName, type);

            // Configure processor
            configureKineticaConnection(runner, testTableName);
            runner.setProperty("JSON Format", "ARRAY");

            // JSON with nested objects (will be flattened or stored as string)
            String jsonContent = "[" +
                    "{\"id\":1,\"name\":\"Test1\",\"metadata\":\"{\\\"key\\\":\\\"value\\\"}\"}," +
                    "{\"id\":2,\"name\":\"Test2\",\"metadata\":\"{\\\"count\\\":42}\"}" +
                    "]";

            runner.enqueue(jsonContent);
            runner.run();

            // Verify
            runner.assertAllFlowFilesTransferred(AbstractPutKineticaProcessor.REL_SUCCESS, 1);
            assertTableRecordCount(testTableName, 2);
        }
    }

    @Nested
    @DisplayName("NDJSON Processing")
    class NdjsonTests {

        @Test
        @DisplayName("Should ingest NDJSON (newline-delimited JSON)")
        void testNdjson() throws Exception {
            // Create test table
            createSimpleTestTable(testTableName);

            // Configure processor
            configureKineticaConnection(runner, testTableName);
            runner.setProperty("JSON Format", "NDJSON");

            // Create NDJSON content
            String ndjsonContent = generateTestNdjsonData(50);

            runner.enqueue(ndjsonContent);
            runner.run();

            // Verify success
            runner.assertAllFlowFilesTransferred(AbstractPutKineticaProcessor.REL_SUCCESS, 1);
            assertTableRecordCount(testTableName, 50);
        }

        @Test
        @DisplayName("Should handle NDJSON with blank lines")
        void testNdjsonWithBlankLines() throws Exception {
            // Create test table
            createSimpleTestTable(testTableName);

            // Configure processor
            configureKineticaConnection(runner, testTableName);
            runner.setProperty("JSON Format", "NDJSON");

            // NDJSON with blank lines
            long ts = System.currentTimeMillis();
            String ndjsonContent =
                    "{\"id\":1,\"name\":\"First\",\"value\":1.0,\"timestamp\":" + ts + "}\n" +
                    "\n" +  // Blank line
                    "{\"id\":2,\"name\":\"Second\",\"value\":2.0,\"timestamp\":" + (ts+1) + "}\n" +
                    "   \n" +  // Whitespace line
                    "{\"id\":3,\"name\":\"Third\",\"value\":3.0,\"timestamp\":" + (ts+2) + "}\n";

            runner.enqueue(ndjsonContent);
            runner.run();

            // Should skip blank lines and succeed
            runner.assertAllFlowFilesTransferred(AbstractPutKineticaProcessor.REL_SUCCESS, 1);
            assertTableRecordCount(testTableName, 3);
        }
    }

    @Nested
    @DisplayName("Data Type Handling")
    class DataTypeTests {

        @Test
        @DisplayName("Should handle various JSON data types")
        void testVariousDataTypes() throws Exception {
            // Create table with various types
            Type type = new Type(
                    new Type.Column("id", Long.class, ColumnProperty.PRIMARY_KEY),
                    new Type.Column("int_val", Integer.class),
                    new Type.Column("long_val", Long.class),
                    new Type.Column("float_val", Float.class),
                    new Type.Column("double_val", Double.class),
                    new Type.Column("string_val", String.class)
            );
            createTestTable(testTableName, type);

            // Configure processor
            configureKineticaConnection(runner, testTableName);
            runner.setProperty("JSON Format", "ARRAY");

            // JSON with various types
            String jsonContent = "[" +
                    "{\"id\":1,\"int_val\":42,\"long_val\":9223372036854775807," +
                    "\"float_val\":3.14,\"double_val\":2.718281828459045,\"string_val\":\"hello\"}," +
                    "{\"id\":2,\"int_val\":-100,\"long_val\":-1," +
                    "\"float_val\":-0.5,\"double_val\":0.0,\"string_val\":\"world\"}" +
                    "]";

            runner.enqueue(jsonContent);
            runner.run();

            // Verify
            runner.assertAllFlowFilesTransferred(AbstractPutKineticaProcessor.REL_SUCCESS, 1);
            assertTableRecordCount(testTableName, 2);

            // Verify data types preserved
            GetRecordsResponse<Record> response = gpudb.getRecords(testTableName, 0, 10, null);
            Record record = response.getData().stream()
                    .filter(r -> r.get("id").equals(1L))
                    .findFirst()
                    .orElseThrow();

            assertThat(record.get("int_val")).isEqualTo(42);
            assertThat(record.get("string_val")).isEqualTo("hello");
        }

        @Test
        @DisplayName("Should handle null values")
        void testNullValues() throws Exception {
            // Create table with nullable columns
            Type type = new Type(
                    new Type.Column("id", Long.class, ColumnProperty.PRIMARY_KEY),
                    new Type.Column("optional_value", Double.class, ColumnProperty.NULLABLE),
                    new Type.Column("optional_name", String.class, ColumnProperty.NULLABLE)
            );
            createTestTable(testTableName, type);

            // Configure processor
            configureKineticaConnection(runner, testTableName);
            runner.setProperty("JSON Format", "ARRAY");

            // JSON with null values
            String jsonContent = "[" +
                    "{\"id\":1,\"optional_value\":null,\"optional_name\":\"has_name\"}," +
                    "{\"id\":2,\"optional_value\":100.5,\"optional_name\":null}," +
                    "{\"id\":3,\"optional_value\":null,\"optional_name\":null}" +
                    "]";

            runner.enqueue(jsonContent);
            runner.run();

            // Verify
            runner.assertAllFlowFilesTransferred(AbstractPutKineticaProcessor.REL_SUCCESS, 1);
            assertTableRecordCount(testTableName, 3);
        }
    }

    @Nested
    @DisplayName("Error Handling")
    class ErrorHandlingTests {

        @Test
        @DisplayName("Should handle invalid JSON gracefully")
        void testInvalidJson() throws Exception {
            // Create test table
            createSimpleTestTable(testTableName);

            // Configure processor
            configureKineticaConnection(runner, testTableName);
            runner.setProperty("JSON Format", "ARRAY");

            // Invalid JSON
            runner.enqueue("{invalid json");
            runner.run();

            // Should fail
            runner.assertAllFlowFilesTransferred(AbstractPutKineticaProcessor.REL_FAILURE, 1);
        }

        @Test
        @DisplayName("Should skip bad records in NDJSON when configured")
        void testSkipBadNdjsonRecords() throws Exception {
            // Create test table
            createSimpleTestTable(testTableName);

            // Configure processor to skip errors
            configureKineticaConnection(runner, testTableName);
            runner.setProperty("JSON Format", "NDJSON");
            runner.setProperty("Skip Errors", "true");

            // NDJSON with some bad records
            long ts = System.currentTimeMillis();
            String ndjsonContent =
                    "{\"id\":1,\"name\":\"good\",\"value\":1.0,\"timestamp\":" + ts + "}\n" +
                    "{invalid json line}\n" +  // Bad
                    "{\"id\":2,\"name\":\"good2\",\"value\":2.0,\"timestamp\":" + (ts+1) + "}\n" +
                    "{\"id\":\"not_a_number\",\"name\":\"bad_id\",\"value\":3.0,\"timestamp\":" + (ts+2) + "}\n" +  // Bad
                    "{\"id\":3,\"name\":\"good3\",\"value\":4.0,\"timestamp\":" + (ts+3) + "}\n";

            runner.enqueue(ndjsonContent);
            runner.run();

            // Good records should be inserted
            assertTableRecordCount(testTableName, 3);
        }
    }

    @Nested
    @DisplayName("Performance Tests")
    class PerformanceTests {

        @Test
        @DisplayName("Should handle large JSON array efficiently")
        void testLargeJsonArray() throws Exception {
            // Create test table
            createSimpleTestTable(testTableName);

            // Configure processor
            configureKineticaConnection(runner, testTableName);
            runner.setProperty("JSON Format", "ARRAY");

            // Generate large JSON (50K records)
            int recordCount = 50000;
            String jsonContent = generateTestJsonArrayData(recordCount);

            // Time the ingestion
            long startTime = System.currentTimeMillis();
            runner.enqueue(jsonContent);
            runner.run();
            long duration = System.currentTimeMillis() - startTime;

            // Verify
            runner.assertAllFlowFilesTransferred(AbstractPutKineticaProcessor.REL_SUCCESS, 1);
            assertTableRecordCount(testTableName, recordCount);

            // Log performance
            double recordsPerSecond = recordCount / (duration / 1000.0);
            logger.info("Ingested {} JSON records in {}ms ({} records/sec)",
                    recordCount, duration, String.format("%.2f", recordsPerSecond));
        }

        @Test
        @DisplayName("Should handle large NDJSON file efficiently")
        void testLargeNdjson() throws Exception {
            // Create test table
            createSimpleTestTable(testTableName);

            // Configure processor
            configureKineticaConnection(runner, testTableName);
            runner.setProperty("JSON Format", "NDJSON");

            // Generate large NDJSON (50K records)
            int recordCount = 50000;
            String ndjsonContent = generateTestNdjsonData(recordCount);

            // Time the ingestion
            long startTime = System.currentTimeMillis();
            runner.enqueue(ndjsonContent);
            runner.run();
            long duration = System.currentTimeMillis() - startTime;

            // Verify
            runner.assertAllFlowFilesTransferred(AbstractPutKineticaProcessor.REL_SUCCESS, 1);
            assertTableRecordCount(testTableName, recordCount);

            // Log performance
            double recordsPerSecond = recordCount / (duration / 1000.0);
            logger.info("Ingested {} NDJSON records in {}ms ({} records/sec)",
                    recordCount, duration, String.format("%.2f", recordsPerSecond));
        }
    }
}
