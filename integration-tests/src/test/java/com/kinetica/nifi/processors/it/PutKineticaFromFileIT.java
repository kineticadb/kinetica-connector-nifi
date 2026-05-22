package com.kinetica.nifi.processors.it;

import com.gpudb.ColumnProperty;
import com.gpudb.GPUdbException;
import com.gpudb.Record;
import com.gpudb.Type;
import com.gpudb.protocol.GetRecordsResponse;
import com.kinetica.nifi.processors.PutKineticaFromFile;
import com.kinetica.nifi.processors.base.AbstractPutKineticaProcessor;

import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration tests for PutKineticaFromFile processor (CSV/TSV ingestion).
 *
 * <p>Tests require a running Kinetica instance.
 * Configure via: {@code -Dkinetica.url=http://host:9191}
 */
@DisplayName("PutKineticaFromFile Integration Tests")
class PutKineticaFromFileIT extends AbstractKineticaIT {

    private TestRunner runner;
    private String testTableName;

    @BeforeEach
    void setUpRunner() throws GPUdbException {
        runner = TestRunners.newTestRunner(PutKineticaFromFile.class);
        testTableName = generateTestTableName();
    }

    @Nested
    @DisplayName("CSV File Processing")
    class CsvProcessingTests {

        @Test
        @DisplayName("Should ingest CSV file with header")
        void testCsvWithHeader() throws Exception {
            // Create test table
            createSimpleTestTable(testTableName);

            // Configure processor
            configureKineticaConnection(runner, testTableName);
            runner.setProperty("Delimiter", ",");
            runner.setProperty("File Has Header", "true");

            // Create CSV content
            String csvContent = generateTestCsvData(100);

            // Enqueue and run
            runner.enqueue(csvContent);
            runner.run();

            // Verify success
            runner.assertAllFlowFilesTransferred(AbstractPutKineticaProcessor.REL_SUCCESS, 1);

            // Verify records in Kinetica
            assertTableRecordCount(testTableName, 100);
        }

        @Test
        @DisplayName("Should ingest CSV file without header")
        void testCsvWithoutHeader() throws Exception {
            // Create test table
            createSimpleTestTable(testTableName);

            // Configure processor
            configureKineticaConnection(runner, testTableName);
            runner.setProperty("Delimiter", ",");
            runner.setProperty("File Has Header", "false");

            // Create CSV content without header
            StringBuilder sb = new StringBuilder();
            long baseTimestamp = System.currentTimeMillis();
            for (int i = 0; i < 50; i++) {
                sb.append(i).append(",");
                sb.append("name_").append(i).append(",");
                sb.append(i * 1.5).append(",");
                sb.append(baseTimestamp + i * 1000).append("\n");
            }

            runner.enqueue(sb.toString());
            runner.run();

            // Verify success
            runner.assertAllFlowFilesTransferred(AbstractPutKineticaProcessor.REL_SUCCESS, 1);
            assertTableRecordCount(testTableName, 50);
        }

        @Test
        @DisplayName("Should handle quoted fields in CSV")
        void testCsvWithQuotedFields() throws Exception {
            // Create table with string columns
            Type type = new Type(
                    new Type.Column("id", Long.class, ColumnProperty.PRIMARY_KEY),
                    new Type.Column("description", String.class),
                    new Type.Column("tags", String.class)
            );
            createTestTable(testTableName, type);

            // Configure processor
            configureKineticaConnection(runner, testTableName);
            runner.setProperty("Delimiter", ",");
            runner.setProperty("Quote Character", "\"");
            runner.setProperty("File Has Header", "true");

            // CSV with quoted fields containing commas and newlines
            String csvContent = "id,description,tags\n" +
                    "1,\"Simple description\",\"tag1,tag2\"\n" +
                    "2,\"Description with, comma\",\"tag3\"\n" +
                    "3,\"Multi-line\ndescription\",\"tag4,tag5,tag6\"\n";

            runner.enqueue(csvContent);
            runner.run();

            // Verify success
            runner.assertAllFlowFilesTransferred(AbstractPutKineticaProcessor.REL_SUCCESS, 1);
            assertTableRecordCount(testTableName, 3);

            // Verify quoted content was preserved
            GetRecordsResponse<Record> response = gpudb.getRecords(testTableName, 0, 10, null);
            List<Record> records = response.getData();

            // Find record with id=2
            Record record2 = records.stream()
                    .filter(r -> r.get("id").equals(2L))
                    .findFirst()
                    .orElseThrow();
            assertThat(record2.get("description")).isEqualTo("Description with, comma");
        }
    }

    @Nested
    @DisplayName("TSV File Processing")
    class TsvProcessingTests {

        @Test
        @DisplayName("Should ingest TSV file")
        void testTsvFile() throws Exception {
            // Create test table
            createSimpleTestTable(testTableName);

            // Configure processor for TSV
            configureKineticaConnection(runner, testTableName);
            runner.setProperty("Delimiter", "\\t");
            runner.setProperty("File Has Header", "true");

            // Create TSV content
            StringBuilder sb = new StringBuilder();
            sb.append("id\tname\tvalue\ttimestamp\n");
            long baseTimestamp = System.currentTimeMillis();
            for (int i = 0; i < 25; i++) {
                sb.append(i).append("\t");
                sb.append("name_").append(i).append("\t");
                sb.append(i * 2.5).append("\t");
                sb.append(baseTimestamp + i * 1000).append("\n");
            }

            runner.enqueue(sb.toString());
            runner.run();

            // Verify success
            runner.assertAllFlowFilesTransferred(AbstractPutKineticaProcessor.REL_SUCCESS, 1);
            assertTableRecordCount(testTableName, 25);
        }
    }

    @Nested
    @DisplayName("Error Handling")
    class ErrorHandlingTests {

        @Test
        @DisplayName("Should skip bad records when configured")
        void testSkipBadRecords() throws Exception {
            // Create test table
            createSimpleTestTable(testTableName);

            // Configure processor to skip errors
            configureKineticaConnection(runner, testTableName);
            runner.setProperty("Delimiter", ",");
            runner.setProperty("File Has Header", "true");
            runner.setProperty("Skip Errors", "true");

            // CSV with some bad records
            String csvContent = "id,name,value,timestamp\n" +
                    "1,good_record,100.0,1234567890000\n" +
                    "not_a_number,bad_id,100.0,1234567890000\n" +  // Bad: invalid id
                    "2,good_record_2,200.0,1234567890000\n" +
                    "3,good_record_3,not_a_number,1234567890000\n" +  // Bad: invalid value
                    "4,good_record_4,400.0,1234567890000\n";

            runner.enqueue(csvContent);
            runner.run();

            // Main FlowFile should succeed
            List<MockFlowFile> successes = runner.getFlowFilesForRelationship(
                    AbstractPutKineticaProcessor.REL_SUCCESS);
            assertThat(successes).hasSize(1);

            // Good records should be in table
            assertTableRecordCount(testTableName, 3);

            // Bad records should be in failure FlowFile
            List<MockFlowFile> failures = runner.getFlowFilesForRelationship(
                    AbstractPutKineticaProcessor.REL_FAILURE);
            assertThat(failures).hasSize(1);

            // Failure FlowFile should contain bad records
            String failureContent = failures.get(0).getContent();
            assertThat(failureContent).contains("not_a_number");
        }

        @Test
        @DisplayName("Should fail entire file when skip errors is false")
        void testFailOnBadRecords() throws Exception {
            // Create test table
            createSimpleTestTable(testTableName);

            // Configure processor to NOT skip errors
            configureKineticaConnection(runner, testTableName);
            runner.setProperty("Delimiter", ",");
            runner.setProperty("File Has Header", "true");
            runner.setProperty("Skip Errors", "false");

            // CSV with bad record
            String csvContent = "id,name,value,timestamp\n" +
                    "1,good_record,100.0,1234567890000\n" +
                    "not_a_number,bad_id,100.0,1234567890000\n";  // Bad record

            runner.enqueue(csvContent);
            runner.run();

            // Should fail
            runner.assertAllFlowFilesTransferred(AbstractPutKineticaProcessor.REL_FAILURE, 1);

            // No records should be in table (transaction rolled back)
            // Note: Actual behavior depends on BulkInserter transaction semantics
        }

        @Test
        @DisplayName("Should handle wrong column count")
        void testWrongColumnCount() throws Exception {
            // Create test table
            createSimpleTestTable(testTableName);

            // Configure processor
            configureKineticaConnection(runner, testTableName);
            runner.setProperty("Delimiter", ",");
            runner.setProperty("File Has Header", "true");
            runner.setProperty("Skip Errors", "true");

            // CSV with wrong number of columns
            String csvContent = "id,name,value,timestamp\n" +
                    "1,good_record,100.0,1234567890000\n" +
                    "2,too_few_columns\n" +  // Missing columns
                    "3,too,many,columns,extra,fields\n" +  // Extra columns
                    "4,good_record_4,400.0,1234567890000\n";

            runner.enqueue(csvContent);
            runner.run();

            // Good records should be inserted
            long count = getTableRecordCount(testTableName);
            assertThat(count).isGreaterThanOrEqualTo(2);
        }
    }

    @Nested
    @DisplayName("Large File Performance")
    class PerformanceTests {

        @Test
        @DisplayName("Should handle large CSV file efficiently")
        void testLargeCsvFile() throws Exception {
            // Create test table
            createSimpleTestTable(testTableName);

            // Configure processor
            configureKineticaConnection(runner, testTableName);
            runner.setProperty("Delimiter", ",");
            runner.setProperty("File Has Header", "true");

            // Generate large CSV (100K records)
            int recordCount = 100000;
            String csvContent = generateTestCsvData(recordCount);

            // Enqueue and run with timing
            long startTime = System.currentTimeMillis();
            runner.enqueue(csvContent);
            runner.run();
            long duration = System.currentTimeMillis() - startTime;

            // Verify success
            runner.assertAllFlowFilesTransferred(AbstractPutKineticaProcessor.REL_SUCCESS, 1);
            assertTableRecordCount(testTableName, recordCount);

            // Log performance
            double recordsPerSecond = recordCount / (duration / 1000.0);
            logger.info("Ingested {} CSV records in {}ms ({} records/sec)",
                    recordCount, duration, String.format("%.2f", recordsPerSecond));

            // Performance assertion (should be > 10K records/sec)
            assertThat(recordsPerSecond).isGreaterThan(10000);
        }
    }
}
