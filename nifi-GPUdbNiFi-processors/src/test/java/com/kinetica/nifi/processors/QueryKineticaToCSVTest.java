package com.kinetica.nifi.processors;

import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;

/**
 * Unit tests for QueryKineticaToCSV processor.
 * Tests property validation and CSV output configuration options.
 */
public class QueryKineticaToCSVTest {

    private TestRunner testRunner;

    @Before
    public void setUp() {
        testRunner = TestRunners.newTestRunner(QueryKineticaToCSV.class);
    }

    @Test
    public void testProcessorIsConfigurable() {
        assertNotNull(testRunner.getProcessor());
    }

    @Test
    public void testRequiredPropertiesValidation() {
        testRunner.assertNotValid();

        testRunner.setProperty(QueryKineticaToCSV.PROP_SERVER, "http://localhost:9191");
        testRunner.assertNotValid();

        testRunner.setProperty(QueryKineticaToCSV.PROP_TABLE, "test_table");
        testRunner.assertNotValid();

        testRunner.setProperty(QueryKineticaToCSV.PROP_SQL_QUERY, "SELECT * FROM test_table");
        testRunner.assertValid();
    }

    @Test
    public void testDelimiterProperty() {
        testRunner.setProperty(QueryKineticaToCSV.PROP_SERVER, "http://localhost:9191");
        testRunner.setProperty(QueryKineticaToCSV.PROP_TABLE, "test_table");
        testRunner.setProperty(QueryKineticaToCSV.PROP_SQL_QUERY, "SELECT * FROM test_table");

        // Comma delimiter (default)
        testRunner.setProperty(QueryKineticaToCSV.PROP_DELIMITER, ",");
        testRunner.assertValid();

        // Tab delimiter
        testRunner.setProperty(QueryKineticaToCSV.PROP_DELIMITER, "\\t");
        testRunner.assertValid();

        // Pipe delimiter
        testRunner.setProperty(QueryKineticaToCSV.PROP_DELIMITER, "|");
        testRunner.assertValid();
    }

    @Test
    public void testIncludeHeaderProperty() {
        testRunner.setProperty(QueryKineticaToCSV.PROP_SERVER, "http://localhost:9191");
        testRunner.setProperty(QueryKineticaToCSV.PROP_TABLE, "test_table");
        testRunner.setProperty(QueryKineticaToCSV.PROP_SQL_QUERY, "SELECT * FROM test_table");

        // Include header (default)
        testRunner.setProperty(QueryKineticaToCSV.PROP_INCLUDE_HEADER, "true");
        testRunner.assertValid();

        // No header
        testRunner.setProperty(QueryKineticaToCSV.PROP_INCLUDE_HEADER, "false");
        testRunner.assertValid();
    }

    @Test
    public void testQuoteCharProperty() {
        testRunner.setProperty(QueryKineticaToCSV.PROP_SERVER, "http://localhost:9191");
        testRunner.setProperty(QueryKineticaToCSV.PROP_TABLE, "test_table");
        testRunner.setProperty(QueryKineticaToCSV.PROP_SQL_QUERY, "SELECT * FROM test_table");

        // Double quote (default)
        testRunner.setProperty(QueryKineticaToCSV.PROP_QUOTE_CHAR, "\"");
        testRunner.assertValid();

        // Single quote
        testRunner.setProperty(QueryKineticaToCSV.PROP_QUOTE_CHAR, "'");
        testRunner.assertValid();
    }

    @Test
    public void testPageSizeProperty() {
        testRunner.setProperty(QueryKineticaToCSV.PROP_SERVER, "http://localhost:9191");
        testRunner.setProperty(QueryKineticaToCSV.PROP_TABLE, "test_table");
        testRunner.setProperty(QueryKineticaToCSV.PROP_SQL_QUERY, "SELECT * FROM test_table");

        // Small page size
        testRunner.setProperty(QueryKineticaToCSV.PROP_PAGE_SIZE, "1000");
        testRunner.assertValid();

        // Large page size
        testRunner.setProperty(QueryKineticaToCSV.PROP_PAGE_SIZE, "100000");
        testRunner.assertValid();
    }

    @Test
    public void testMaxRecordsProperty() {
        testRunner.setProperty(QueryKineticaToCSV.PROP_SERVER, "http://localhost:9191");
        testRunner.setProperty(QueryKineticaToCSV.PROP_TABLE, "test_table");
        testRunner.setProperty(QueryKineticaToCSV.PROP_SQL_QUERY, "SELECT * FROM test_table");

        // Unlimited records (default)
        testRunner.setProperty(QueryKineticaToCSV.PROP_MAX_RECORDS, "-1");
        testRunner.assertValid();

        // Limited records
        testRunner.setProperty(QueryKineticaToCSV.PROP_MAX_RECORDS, "1000000");
        testRunner.assertValid();
    }

    @Test
    public void testTypicalConfiguration() {
        testRunner.setProperty(QueryKineticaToCSV.PROP_SERVER, "http://localhost:9191");
        testRunner.setProperty(QueryKineticaToCSV.PROP_TABLE, "my_table");
        testRunner.setProperty(QueryKineticaToCSV.PROP_SQL_QUERY, "SELECT id, name, value FROM my_table WHERE status = 'active'");
        testRunner.setProperty(QueryKineticaToCSV.PROP_DELIMITER, ",");
        testRunner.setProperty(QueryKineticaToCSV.PROP_INCLUDE_HEADER, "true");
        testRunner.setProperty(QueryKineticaToCSV.PROP_QUOTE_CHAR, "\"");
        testRunner.setProperty(QueryKineticaToCSV.PROP_PAGE_SIZE, "10000");

        testRunner.assertValid();
    }

    @Test
    public void testProcessorHasCorrectRelationships() {
        QueryKineticaToCSV processor = (QueryKineticaToCSV) testRunner.getProcessor();

        assertTrue(processor.getRelationships().contains(QueryKineticaToCSV.REL_SUCCESS));
        assertTrue(processor.getRelationships().contains(QueryKineticaToCSV.REL_FAILURE));
        assertEquals(2, processor.getRelationships().size());
    }

    @Test
    public void testAllCsvSpecificPropertiesExist() {
        QueryKineticaToCSV processor = (QueryKineticaToCSV) testRunner.getProcessor();

        // Verify all CSV-specific properties are available
        assertTrue(processor.getSupportedPropertyDescriptors().contains(QueryKineticaToCSV.PROP_DELIMITER));
        assertTrue(processor.getSupportedPropertyDescriptors().contains(QueryKineticaToCSV.PROP_INCLUDE_HEADER));
        assertTrue(processor.getSupportedPropertyDescriptors().contains(QueryKineticaToCSV.PROP_QUOTE_CHAR));
        assertTrue(processor.getSupportedPropertyDescriptors().contains(QueryKineticaToCSV.PROP_SQL_QUERY));
        assertTrue(processor.getSupportedPropertyDescriptors().contains(QueryKineticaToCSV.PROP_PAGE_SIZE));
        assertTrue(processor.getSupportedPropertyDescriptors().contains(QueryKineticaToCSV.PROP_MAX_RECORDS));
    }
}
