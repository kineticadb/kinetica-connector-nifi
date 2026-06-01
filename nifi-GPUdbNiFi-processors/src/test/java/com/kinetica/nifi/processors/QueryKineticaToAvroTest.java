package com.kinetica.nifi.processors;

import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;

/**
 * Unit tests for QueryKineticaToAvro processor.
 * Tests property validation and Avro output configuration options.
 */
public class QueryKineticaToAvroTest {

    private TestRunner testRunner;

    @Before
    public void setUp() {
        testRunner = TestRunners.newTestRunner(QueryKineticaToAvro.class);
    }

    @Test
    public void testProcessorIsConfigurable() {
        assertNotNull(testRunner.getProcessor());
    }

    @Test
    public void testRequiredPropertiesValidation() {
        testRunner.assertNotValid();

        testRunner.setProperty(QueryKineticaToAvro.PROP_SERVER, "http://localhost:9191");
        testRunner.assertNotValid();

        testRunner.setProperty(QueryKineticaToAvro.PROP_TABLE, "test_table");
        testRunner.assertNotValid();

        testRunner.setProperty(QueryKineticaToAvro.PROP_SQL_QUERY, "SELECT * FROM test_table");
        testRunner.assertValid();
    }

    @Test
    public void testIncludeSchemaProperty() {
        testRunner.setProperty(QueryKineticaToAvro.PROP_SERVER, "http://localhost:9191");
        testRunner.setProperty(QueryKineticaToAvro.PROP_TABLE, "test_table");
        testRunner.setProperty(QueryKineticaToAvro.PROP_SQL_QUERY, "SELECT * FROM test_table");

        // Include schema (default - container format)
        testRunner.setProperty(QueryKineticaToAvro.PROP_INCLUDE_SCHEMA, "true");
        testRunner.assertValid();

        // Raw binary without schema
        testRunner.setProperty(QueryKineticaToAvro.PROP_INCLUDE_SCHEMA, "false");
        testRunner.assertValid();
    }

    @Test
    public void testAvroNamespaceProperty() {
        testRunner.setProperty(QueryKineticaToAvro.PROP_SERVER, "http://localhost:9191");
        testRunner.setProperty(QueryKineticaToAvro.PROP_TABLE, "test_table");
        testRunner.setProperty(QueryKineticaToAvro.PROP_SQL_QUERY, "SELECT * FROM test_table");

        // Custom namespace
        testRunner.setProperty(QueryKineticaToAvro.PROP_AVRO_NAMESPACE, "com.mycompany.data");
        testRunner.assertValid();
    }

    @Test
    public void testPageSizeProperty() {
        testRunner.setProperty(QueryKineticaToAvro.PROP_SERVER, "http://localhost:9191");
        testRunner.setProperty(QueryKineticaToAvro.PROP_TABLE, "test_table");
        testRunner.setProperty(QueryKineticaToAvro.PROP_SQL_QUERY, "SELECT * FROM test_table");

        // Small page size
        testRunner.setProperty(QueryKineticaToAvro.PROP_PAGE_SIZE, "1000");
        testRunner.assertValid();

        // Large page size
        testRunner.setProperty(QueryKineticaToAvro.PROP_PAGE_SIZE, "100000");
        testRunner.assertValid();
    }

    @Test
    public void testMaxRecordsProperty() {
        testRunner.setProperty(QueryKineticaToAvro.PROP_SERVER, "http://localhost:9191");
        testRunner.setProperty(QueryKineticaToAvro.PROP_TABLE, "test_table");
        testRunner.setProperty(QueryKineticaToAvro.PROP_SQL_QUERY, "SELECT * FROM test_table");

        // Unlimited records (default)
        testRunner.setProperty(QueryKineticaToAvro.PROP_MAX_RECORDS, "-1");
        testRunner.assertValid();

        // Limited records
        testRunner.setProperty(QueryKineticaToAvro.PROP_MAX_RECORDS, "1000000");
        testRunner.assertValid();
    }

    @Test
    public void testTypicalContainerFormatConfiguration() {
        testRunner.setProperty(QueryKineticaToAvro.PROP_SERVER, "http://localhost:9191");
        testRunner.setProperty(QueryKineticaToAvro.PROP_TABLE, "my_table");
        testRunner.setProperty(QueryKineticaToAvro.PROP_SQL_QUERY,
                "SELECT id, name, value FROM my_table WHERE status = 'active'");
        testRunner.setProperty(QueryKineticaToAvro.PROP_INCLUDE_SCHEMA, "true");
        testRunner.setProperty(QueryKineticaToAvro.PROP_AVRO_NAMESPACE, "com.kinetica.export");
        testRunner.setProperty(QueryKineticaToAvro.PROP_PAGE_SIZE, "10000");

        testRunner.assertValid();
    }

    @Test
    public void testTypicalRawBinaryConfiguration() {
        testRunner.setProperty(QueryKineticaToAvro.PROP_SERVER, "http://localhost:9191");
        testRunner.setProperty(QueryKineticaToAvro.PROP_TABLE, "my_table");
        testRunner.setProperty(QueryKineticaToAvro.PROP_SQL_QUERY, "SELECT * FROM my_table");
        testRunner.setProperty(QueryKineticaToAvro.PROP_INCLUDE_SCHEMA, "false");
        testRunner.setProperty(QueryKineticaToAvro.PROP_PAGE_SIZE, "50000");

        testRunner.assertValid();
    }

    @Test
    public void testProcessorHasCorrectRelationships() {
        QueryKineticaToAvro processor = (QueryKineticaToAvro) testRunner.getProcessor();

        assertTrue(processor.getRelationships().contains(QueryKineticaToAvro.REL_SUCCESS));
        assertTrue(processor.getRelationships().contains(QueryKineticaToAvro.REL_FAILURE));
        assertEquals(2, processor.getRelationships().size());
    }

    @Test
    public void testAllAvroSpecificPropertiesExist() {
        QueryKineticaToAvro processor = (QueryKineticaToAvro) testRunner.getProcessor();

        // Verify all Avro-specific properties are available
        assertTrue(processor.getSupportedPropertyDescriptors().contains(QueryKineticaToAvro.PROP_INCLUDE_SCHEMA));
        assertTrue(processor.getSupportedPropertyDescriptors().contains(QueryKineticaToAvro.PROP_AVRO_NAMESPACE));
        assertTrue(processor.getSupportedPropertyDescriptors().contains(QueryKineticaToAvro.PROP_SQL_QUERY));
        assertTrue(processor.getSupportedPropertyDescriptors().contains(QueryKineticaToAvro.PROP_PAGE_SIZE));
        assertTrue(processor.getSupportedPropertyDescriptors().contains(QueryKineticaToAvro.PROP_MAX_RECORDS));
    }

    @Test
    public void testStreamingModeProperty() {
        testRunner.setProperty(QueryKineticaToAvro.PROP_SERVER, "http://localhost:9191");
        testRunner.setProperty(QueryKineticaToAvro.PROP_TABLE, "test_table");
        testRunner.setProperty(QueryKineticaToAvro.PROP_SQL_QUERY, "SELECT * FROM test_table");

        // Streaming mode disabled (default)
        testRunner.setProperty(QueryKineticaToAvro.PROP_USE_STREAMING, "false");
        testRunner.assertValid();

        // Streaming mode enabled
        testRunner.setProperty(QueryKineticaToAvro.PROP_USE_STREAMING, "true");
        testRunner.assertValid();
    }

    @Test
    public void testStreamingModeWithPagingTableTtl() {
        testRunner.setProperty(QueryKineticaToAvro.PROP_SERVER, "http://localhost:9191");
        testRunner.setProperty(QueryKineticaToAvro.PROP_TABLE, "test_table");
        testRunner.setProperty(QueryKineticaToAvro.PROP_SQL_QUERY, "SELECT * FROM test_table");
        testRunner.setProperty(QueryKineticaToAvro.PROP_USE_STREAMING, "true");
        testRunner.setProperty(QueryKineticaToAvro.PROP_PAGING_TABLE_TTL, "600");

        testRunner.assertValid();
    }

    @Test
    public void testComplexSqlQuery() {
        testRunner.setProperty(QueryKineticaToAvro.PROP_SERVER, "http://localhost:9191");
        testRunner.setProperty(QueryKineticaToAvro.PROP_TABLE, "my_table");
        testRunner.setProperty(QueryKineticaToAvro.PROP_SQL_QUERY,
                "SELECT t1.id, t1.name, t2.value FROM table1 t1 JOIN table2 t2 ON t1.id = t2.id WHERE t1.status = 'active' ORDER BY t1.id");

        testRunner.assertValid();
    }
}
