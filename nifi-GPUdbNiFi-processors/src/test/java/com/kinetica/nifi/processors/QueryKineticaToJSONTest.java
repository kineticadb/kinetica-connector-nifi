package com.kinetica.nifi.processors;

import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;

/**
 * Unit tests for QueryKineticaToJSON processor.
 * Tests property validation and JSON output configuration options.
 */
public class QueryKineticaToJSONTest {

    private TestRunner testRunner;

    @Before
    public void setUp() {
        testRunner = TestRunners.newTestRunner(QueryKineticaToJSON.class);
    }

    @Test
    public void testProcessorIsConfigurable() {
        assertNotNull(testRunner.getProcessor());
    }

    @Test
    public void testRequiredPropertiesValidation() {
        testRunner.assertNotValid();

        testRunner.setProperty(QueryKineticaToJSON.PROP_SERVER, "http://localhost:9191");
        testRunner.assertNotValid();

        testRunner.setProperty(QueryKineticaToJSON.PROP_TABLE, "test_table");
        testRunner.assertNotValid();

        testRunner.setProperty(QueryKineticaToJSON.PROP_SQL_QUERY, "SELECT * FROM test_table");
        testRunner.assertValid();
    }

    @Test
    public void testJsonFormatProperty() {
        testRunner.setProperty(QueryKineticaToJSON.PROP_SERVER, "http://localhost:9191");
        testRunner.setProperty(QueryKineticaToJSON.PROP_TABLE, "test_table");
        testRunner.setProperty(QueryKineticaToJSON.PROP_SQL_QUERY, "SELECT * FROM test_table");

        // Array format (default)
        testRunner.setProperty(QueryKineticaToJSON.PROP_JSON_FORMAT, "ARRAY");
        testRunner.assertValid();

        // NDJSON format
        testRunner.setProperty(QueryKineticaToJSON.PROP_JSON_FORMAT, "NDJSON");
        testRunner.assertValid();
    }

    @Test
    public void testPrettyPrintProperty() {
        testRunner.setProperty(QueryKineticaToJSON.PROP_SERVER, "http://localhost:9191");
        testRunner.setProperty(QueryKineticaToJSON.PROP_TABLE, "test_table");
        testRunner.setProperty(QueryKineticaToJSON.PROP_SQL_QUERY, "SELECT * FROM test_table");

        // Pretty print disabled (default)
        testRunner.setProperty(QueryKineticaToJSON.PROP_PRETTY_PRINT, "false");
        testRunner.assertValid();

        // Pretty print enabled
        testRunner.setProperty(QueryKineticaToJSON.PROP_PRETTY_PRINT, "true");
        testRunner.assertValid();
    }

    @Test
    public void testPageSizeProperty() {
        testRunner.setProperty(QueryKineticaToJSON.PROP_SERVER, "http://localhost:9191");
        testRunner.setProperty(QueryKineticaToJSON.PROP_TABLE, "test_table");
        testRunner.setProperty(QueryKineticaToJSON.PROP_SQL_QUERY, "SELECT * FROM test_table");

        // Small page size
        testRunner.setProperty(QueryKineticaToJSON.PROP_PAGE_SIZE, "1000");
        testRunner.assertValid();

        // Large page size
        testRunner.setProperty(QueryKineticaToJSON.PROP_PAGE_SIZE, "100000");
        testRunner.assertValid();
    }

    @Test
    public void testMaxRecordsProperty() {
        testRunner.setProperty(QueryKineticaToJSON.PROP_SERVER, "http://localhost:9191");
        testRunner.setProperty(QueryKineticaToJSON.PROP_TABLE, "test_table");
        testRunner.setProperty(QueryKineticaToJSON.PROP_SQL_QUERY, "SELECT * FROM test_table");

        // Unlimited records (default)
        testRunner.setProperty(QueryKineticaToJSON.PROP_MAX_RECORDS, "-1");
        testRunner.assertValid();

        // Limited records
        testRunner.setProperty(QueryKineticaToJSON.PROP_MAX_RECORDS, "1000000");
        testRunner.assertValid();
    }

    @Test
    public void testTypicalArrayConfiguration() {
        testRunner.setProperty(QueryKineticaToJSON.PROP_SERVER, "http://localhost:9191");
        testRunner.setProperty(QueryKineticaToJSON.PROP_TABLE, "my_table");
        testRunner.setProperty(QueryKineticaToJSON.PROP_SQL_QUERY, "SELECT id, name, value FROM my_table WHERE status = 'active'");
        testRunner.setProperty(QueryKineticaToJSON.PROP_JSON_FORMAT, "ARRAY");
        testRunner.setProperty(QueryKineticaToJSON.PROP_PRETTY_PRINT, "false");
        testRunner.setProperty(QueryKineticaToJSON.PROP_PAGE_SIZE, "10000");

        testRunner.assertValid();
    }

    @Test
    public void testTypicalNdjsonConfiguration() {
        testRunner.setProperty(QueryKineticaToJSON.PROP_SERVER, "http://localhost:9191");
        testRunner.setProperty(QueryKineticaToJSON.PROP_TABLE, "my_table");
        testRunner.setProperty(QueryKineticaToJSON.PROP_SQL_QUERY, "SELECT * FROM my_table");
        testRunner.setProperty(QueryKineticaToJSON.PROP_JSON_FORMAT, "NDJSON");
        testRunner.setProperty(QueryKineticaToJSON.PROP_PAGE_SIZE, "50000");

        testRunner.assertValid();
    }

    @Test
    public void testProcessorHasCorrectRelationships() {
        QueryKineticaToJSON processor = (QueryKineticaToJSON) testRunner.getProcessor();

        assertTrue(processor.getRelationships().contains(QueryKineticaToJSON.REL_SUCCESS));
        assertTrue(processor.getRelationships().contains(QueryKineticaToJSON.REL_FAILURE));
        assertEquals(2, processor.getRelationships().size());
    }

    @Test
    public void testAllJsonSpecificPropertiesExist() {
        QueryKineticaToJSON processor = (QueryKineticaToJSON) testRunner.getProcessor();

        // Verify all JSON-specific properties are available
        assertTrue(processor.getSupportedPropertyDescriptors().contains(QueryKineticaToJSON.PROP_JSON_FORMAT));
        assertTrue(processor.getSupportedPropertyDescriptors().contains(QueryKineticaToJSON.PROP_PRETTY_PRINT));
        assertTrue(processor.getSupportedPropertyDescriptors().contains(QueryKineticaToJSON.PROP_SQL_QUERY));
        assertTrue(processor.getSupportedPropertyDescriptors().contains(QueryKineticaToJSON.PROP_PAGE_SIZE));
        assertTrue(processor.getSupportedPropertyDescriptors().contains(QueryKineticaToJSON.PROP_MAX_RECORDS));
    }
}
