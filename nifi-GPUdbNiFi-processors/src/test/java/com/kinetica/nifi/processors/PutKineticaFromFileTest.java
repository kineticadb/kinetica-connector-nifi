package com.kinetica.nifi.processors;

import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;

/**
 * Unit tests for PutKineticaFromFile processor.
 * Tests property validation and CSV configuration options.
 */
public class PutKineticaFromFileTest {

    private TestRunner testRunner;

    @Before
    public void setUp() {
        testRunner = TestRunners.newTestRunner(PutKineticaFromFile.class);
    }

    @Test
    public void testProcessorIsConfigurable() {
        assertNotNull(testRunner.getProcessor());
    }

    @Test
    public void testRequiredPropertiesValidation() {
        testRunner.assertNotValid();

        testRunner.setProperty(PutKineticaFromFile.PROP_SERVER, "http://localhost:9191");
        testRunner.assertNotValid();

        testRunner.setProperty(PutKineticaFromFile.PROP_TABLE, "test_table");
        testRunner.assertValid();
    }

    @Test
    public void testDelimiterProperty() {
        testRunner.setProperty(PutKineticaFromFile.PROP_SERVER, "http://localhost:9191");
        testRunner.setProperty(PutKineticaFromFile.PROP_TABLE, "test_table");

        // Default comma delimiter
        testRunner.assertValid();

        // Tab delimiter
        testRunner.setProperty(PutKineticaFromFile.PROP_DELIMITER, "\\t");
        testRunner.assertValid();

        // Pipe delimiter
        testRunner.setProperty(PutKineticaFromFile.PROP_DELIMITER, "|");
        testRunner.assertValid();

        // Semicolon delimiter
        testRunner.setProperty(PutKineticaFromFile.PROP_DELIMITER, ";");
        testRunner.assertValid();
    }

    @Test
    public void testQuoteCharProperty() {
        testRunner.setProperty(PutKineticaFromFile.PROP_SERVER, "http://localhost:9191");
        testRunner.setProperty(PutKineticaFromFile.PROP_TABLE, "test_table");

        // Double quote (default)
        testRunner.setProperty(PutKineticaFromFile.PROP_QUOTE_CHAR, "\"");
        testRunner.assertValid();

        // Single quote
        testRunner.setProperty(PutKineticaFromFile.PROP_QUOTE_CHAR, "'");
        testRunner.assertValid();

        // No quoting (empty string)
        testRunner.setProperty(PutKineticaFromFile.PROP_QUOTE_CHAR, "");
        testRunner.assertValid();
    }

    @Test
    public void testEscapeCharProperty() {
        testRunner.setProperty(PutKineticaFromFile.PROP_SERVER, "http://localhost:9191");
        testRunner.setProperty(PutKineticaFromFile.PROP_TABLE, "test_table");

        // Backslash escape
        testRunner.setProperty(PutKineticaFromFile.PROP_ESCAPE_CHAR, "\\");
        testRunner.assertValid();

        // Double quote escape (CSV standard)
        testRunner.setProperty(PutKineticaFromFile.PROP_ESCAPE_CHAR, "\"");
        testRunner.assertValid();
    }

    @Test
    public void testHasHeaderProperty() {
        testRunner.setProperty(PutKineticaFromFile.PROP_SERVER, "http://localhost:9191");
        testRunner.setProperty(PutKineticaFromFile.PROP_TABLE, "test_table");

        // File has header (default)
        testRunner.setProperty(PutKineticaFromFile.PROP_HAS_HEADER, "true");
        testRunner.assertValid();

        // File has no header
        testRunner.setProperty(PutKineticaFromFile.PROP_HAS_HEADER, "false");
        testRunner.assertValid();
    }

    @Test
    public void testSkipErrorsProperty() {
        testRunner.setProperty(PutKineticaFromFile.PROP_SERVER, "http://localhost:9191");
        testRunner.setProperty(PutKineticaFromFile.PROP_TABLE, "test_table");

        // Skip errors (default)
        testRunner.setProperty(PutKineticaFromFile.PROP_SKIP_ERRORS, "true");
        testRunner.assertValid();

        // Fail on first error
        testRunner.setProperty(PutKineticaFromFile.PROP_SKIP_ERRORS, "false");
        testRunner.assertValid();
    }

    @Test
    public void testTypicalCsvConfiguration() {
        testRunner.setProperty(PutKineticaFromFile.PROP_SERVER, "http://localhost:9191");
        testRunner.setProperty(PutKineticaFromFile.PROP_TABLE, "my_table");
        testRunner.setProperty(PutKineticaFromFile.PROP_SCHEMA, "id|Long|data,name|String|data,value|Float|data");
        testRunner.setProperty(PutKineticaFromFile.PROP_DELIMITER, ",");
        testRunner.setProperty(PutKineticaFromFile.PROP_QUOTE_CHAR, "\"");
        testRunner.setProperty(PutKineticaFromFile.PROP_HAS_HEADER, "true");
        testRunner.setProperty(PutKineticaFromFile.PROP_SKIP_ERRORS, "true");
        testRunner.setProperty(PutKineticaFromFile.PROP_BATCH_SIZE, "10000");

        testRunner.assertValid();
    }

    @Test
    public void testTypicalTsvConfiguration() {
        testRunner.setProperty(PutKineticaFromFile.PROP_SERVER, "http://localhost:9191");
        testRunner.setProperty(PutKineticaFromFile.PROP_TABLE, "my_table");
        testRunner.setProperty(PutKineticaFromFile.PROP_SCHEMA, "x|Float|data,y|Float|data,label|String|data");
        testRunner.setProperty(PutKineticaFromFile.PROP_DELIMITER, "\\t");
        testRunner.setProperty(PutKineticaFromFile.PROP_HAS_HEADER, "false");

        testRunner.assertValid();
    }

    @Test
    public void testProcessorHasCorrectRelationships() {
        PutKineticaFromFile processor = (PutKineticaFromFile) testRunner.getProcessor();

        assertTrue(processor.getRelationships().contains(PutKineticaFromFile.REL_SUCCESS));
        assertTrue(processor.getRelationships().contains(PutKineticaFromFile.REL_FAILURE));
        assertEquals(2, processor.getRelationships().size());
    }

    @Test
    public void testNoInputDoesNothing() {
        testRunner.setProperty(PutKineticaFromFile.PROP_SERVER, "http://localhost:9191");
        testRunner.setProperty(PutKineticaFromFile.PROP_TABLE, "test_table");

        testRunner.run();

        testRunner.assertTransferCount(PutKineticaFromFile.REL_SUCCESS, 0);
        testRunner.assertTransferCount(PutKineticaFromFile.REL_FAILURE, 0);
    }

    @Test
    public void testAllFileSpecificPropertiesExist() {
        PutKineticaFromFile processor = (PutKineticaFromFile) testRunner.getProcessor();

        // Verify all file-specific properties are available
        assertTrue(processor.getSupportedPropertyDescriptors().contains(PutKineticaFromFile.PROP_DELIMITER));
        assertTrue(processor.getSupportedPropertyDescriptors().contains(PutKineticaFromFile.PROP_QUOTE_CHAR));
        assertTrue(processor.getSupportedPropertyDescriptors().contains(PutKineticaFromFile.PROP_ESCAPE_CHAR));
        assertTrue(processor.getSupportedPropertyDescriptors().contains(PutKineticaFromFile.PROP_HAS_HEADER));
        assertTrue(processor.getSupportedPropertyDescriptors().contains(PutKineticaFromFile.PROP_SKIP_ERRORS));
    }
}
