package com.kinetica.nifi.processors.it;

import com.gpudb.ColumnProperty;
import com.gpudb.GPUdbException;
import com.gpudb.Record;
import com.gpudb.Type;
import com.gpudb.protocol.GetRecordsResponse;
import com.kinetica.nifi.processors.PutKinetica;
import com.kinetica.nifi.processors.base.AbstractPutKineticaProcessor;

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
 * Integration tests for PutKinetica processor.
 *
 * <p>Tests require a running Kinetica instance.
 * Configure via: {@code -Dkinetica.url=http://host:9191}
 */
@DisplayName("PutKinetica Integration Tests")
class PutKineticaIT extends AbstractKineticaIT {

    private TestRunner runner;
    private String testTableName;

    @BeforeEach
    void setUpRunner() throws GPUdbException {
        runner = TestRunners.newTestRunner(PutKinetica.class);
        testTableName = generateTestTableName();
    }

    @Nested
    @DisplayName("Basic Insert Operations")
    class BasicInsertTests {

        @Test
        @DisplayName("Should insert single record from FlowFile attributes")
        void testInsertSingleRecord() throws Exception {
            // Create test table
            createSimpleTestTable(testTableName);

            // Configure processor
            configureKineticaConnection(runner, testTableName);

            // Create FlowFile with attributes matching table schema
            Map<String, String> attributes = new HashMap<>();
            attributes.put("id", "1");
            attributes.put("name", "Test Record");
            attributes.put("value", "123.45");
            attributes.put("timestamp", String.valueOf(System.currentTimeMillis()));

            runner.enqueue("", attributes);
            runner.run();

            // Verify success
            runner.assertAllFlowFilesTransferred(AbstractPutKineticaProcessor.REL_SUCCESS, 1);

            // Verify record in Kinetica
            assertTableRecordCount(testTableName, 1);

            // Verify record content
            GetRecordsResponse<Record> response = gpudb.getRecords(testTableName, 0, 1, null);
            assertThat(response.getData()).hasSize(1);

            Record record = response.getData().get(0);
            assertThat(record.get("id")).isEqualTo(1L);
            assertThat(record.get("name")).isEqualTo("Test Record");
            assertThat(record.get("value")).isEqualTo(123.45);
        }

        @Test
        @DisplayName("Should insert multiple records in batch")
        void testInsertMultipleRecords() throws Exception {
            // Create test table
            createSimpleTestTable(testTableName);

            // Configure processor
            configureKineticaConnection(runner, testTableName);

            // Enqueue multiple FlowFiles
            long baseTimestamp = System.currentTimeMillis();
            for (int i = 0; i < 100; i++) {
                Map<String, String> attributes = new HashMap<>();
                attributes.put("id", String.valueOf(i));
                attributes.put("name", "Record_" + i);
                attributes.put("value", String.valueOf(i * 1.5));
                attributes.put("timestamp", String.valueOf(baseTimestamp + i));

                runner.enqueue("", attributes);
            }

            runner.run(100);

            // Verify all succeeded
            runner.assertAllFlowFilesTransferred(AbstractPutKineticaProcessor.REL_SUCCESS, 100);

            // Verify record count in Kinetica
            assertTableRecordCount(testTableName, 100);
        }
    }

    @Nested
    @DisplayName("Table Creation")
    class TableCreationTests {

        @Test
        @DisplayName("Should create table from schema definition")
        void testAutoCreateTable() throws Exception {
            // Table doesn't exist yet
            assertThat(tableExists(testTableName)).isFalse();

            // Configure processor with schema
            configureKineticaConnection(runner, testTableName);
            runner.setProperty(AbstractPutKineticaProcessor.PROP_SCHEMA,
                    "id|Long|data|primary_key,name|String|data,score|Double|data");

            // Insert record
            Map<String, String> attributes = new HashMap<>();
            attributes.put("id", "1");
            attributes.put("name", "Auto-created");
            attributes.put("score", "99.9");

            runner.enqueue("", attributes);
            runner.run();

            // Verify table was created
            assertThat(tableExists(testTableName)).isTrue();

            // Verify record inserted
            runner.assertAllFlowFilesTransferred(AbstractPutKineticaProcessor.REL_SUCCESS, 1);
            assertTableRecordCount(testTableName, 1);
        }
    }

    @Nested
    @DisplayName("Update on Existing PK")
    class UpdateOnPkTests {

        @Test
        @DisplayName("Should update existing record when PK matches")
        void testUpdateOnExistingPk() throws Exception {
            // Create test table with primary key
            Type type = new Type(
                    new Type.Column("id", Long.class, ColumnProperty.PRIMARY_KEY),
                    new Type.Column("name", String.class),
                    new Type.Column("value", Double.class)
            );
            createTestTable(testTableName, type);

            // Configure processor with update on existing PK
            configureKineticaConnection(runner, testTableName);
            runner.setProperty(AbstractPutKineticaProcessor.PROP_UPDATE_ON_EXISTING_PK, "true");

            // Insert initial record
            Map<String, String> attrs1 = new HashMap<>();
            attrs1.put("id", "1");
            attrs1.put("name", "Original");
            attrs1.put("value", "100.0");
            runner.enqueue("", attrs1);
            runner.run();

            // Update same record (same PK)
            Map<String, String> attrs2 = new HashMap<>();
            attrs2.put("id", "1");
            attrs2.put("name", "Updated");
            attrs2.put("value", "200.0");
            runner.enqueue("", attrs2);
            runner.run();

            // Should still have 1 record (updated, not inserted)
            assertTableRecordCount(testTableName, 1);

            // Verify updated values
            GetRecordsResponse<Record> response = gpudb.getRecords(testTableName, 0, 1, null);
            Record record = response.getData().get(0);
            assertThat(record.get("name")).isEqualTo("Updated");
            assertThat(record.get("value")).isEqualTo(200.0);
        }
    }

    @Nested
    @DisplayName("Error Handling")
    class ErrorHandlingTests {

        @Test
        @DisplayName("Should route to failure for invalid data type")
        void testInvalidDataType() throws Exception {
            // Create test table
            createSimpleTestTable(testTableName);

            // Configure processor
            configureKineticaConnection(runner, testTableName);

            // Insert record with invalid value (string in numeric field)
            Map<String, String> attributes = new HashMap<>();
            attributes.put("id", "not_a_number");  // Invalid for Long
            attributes.put("name", "Test");
            attributes.put("value", "123.45");
            attributes.put("timestamp", String.valueOf(System.currentTimeMillis()));

            runner.enqueue("", attributes);
            runner.run();

            // Should route to failure
            runner.assertAllFlowFilesTransferred(AbstractPutKineticaProcessor.REL_FAILURE, 1);

            // Table should be empty
            assertTableRecordCount(testTableName, 0);
        }

        @Test
        @DisplayName("Should handle missing required columns gracefully")
        void testMissingColumns() throws Exception {
            // Create test table
            createSimpleTestTable(testTableName);

            // Configure processor
            configureKineticaConnection(runner, testTableName);

            // Insert record with missing columns
            Map<String, String> attributes = new HashMap<>();
            attributes.put("id", "1");
            // Missing: name, value, timestamp

            runner.enqueue("", attributes);
            runner.run();

            // Behavior depends on column nullability
            // For non-nullable columns, should fail
            List<MockFlowFile> failures = runner.getFlowFilesForRelationship(
                    AbstractPutKineticaProcessor.REL_FAILURE);
            List<MockFlowFile> successes = runner.getFlowFilesForRelationship(
                    AbstractPutKineticaProcessor.REL_SUCCESS);

            // Either failed or succeeded with nulls
            assertThat(failures.size() + successes.size()).isEqualTo(1);
        }
    }

    @Nested
    @DisplayName("Performance Tests")
    class PerformanceTests {

        @Test
        @DisplayName("Should handle large batch efficiently")
        void testLargeBatch() throws Exception {
            // Create test table
            createSimpleTestTable(testTableName);

            // Configure processor with larger batch size
            configureKineticaConnection(runner, testTableName);
            runner.setProperty(AbstractPutKineticaProcessor.PROP_BATCH_SIZE, "10000");

            // Insert 10,000 records
            int recordCount = 10000;
            long baseTimestamp = System.currentTimeMillis();

            for (int i = 0; i < recordCount; i++) {
                Map<String, String> attributes = new HashMap<>();
                attributes.put("id", String.valueOf(i));
                attributes.put("name", "Record_" + i);
                attributes.put("value", String.valueOf(i * 0.1));
                attributes.put("timestamp", String.valueOf(baseTimestamp + i));

                runner.enqueue("", attributes);
            }

            // Run all
            long startTime = System.currentTimeMillis();
            runner.run(recordCount);
            long duration = System.currentTimeMillis() - startTime;

            // Verify all succeeded
            runner.assertAllFlowFilesTransferred(AbstractPutKineticaProcessor.REL_SUCCESS, recordCount);

            // Verify record count
            assertTableRecordCount(testTableName, recordCount);

            // Log performance
            double recordsPerSecond = recordCount / (duration / 1000.0);
            logger.info("Inserted {} records in {}ms ({} records/sec)",
                    recordCount, duration, String.format("%.2f", recordsPerSecond));
        }
    }
}
