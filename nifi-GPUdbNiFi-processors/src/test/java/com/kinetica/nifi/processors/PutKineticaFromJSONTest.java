package com.kinetica.nifi.processors;

import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;

/**
 * Unit tests for PutKineticaFromJSON processor.
 * Tests property validation and JSON configuration options.
 */
public class PutKineticaFromJSONTest {

    private TestRunner testRunner;

    @Before
    public void setUp() {
        testRunner = TestRunners.newTestRunner(PutKineticaFromJSON.class);
    }

    @Test
    public void testProcessorIsConfigurable() {
        assertNotNull(testRunner.getProcessor());
    }

    @Test
    public void testRequiredPropertiesValidation() {
        testRunner.assertNotValid();

        testRunner.setProperty(PutKineticaFromJSON.PROP_SERVER, "http://localhost:9191");
        testRunner.assertNotValid();

        testRunner.setProperty(PutKineticaFromJSON.PROP_TABLE, "test_table");
        testRunner.assertValid();
    }

    @Test
    public void testJsonFormatProperty() {
        testRunner.setProperty(PutKineticaFromJSON.PROP_SERVER, "http://localhost:9191");
        testRunner.setProperty(PutKineticaFromJSON.PROP_TABLE, "test_table");

        // Array format (default)
        testRunner.setProperty(PutKineticaFromJSON.PROP_JSON_FORMAT, "ARRAY");
        testRunner.assertValid();

        // NDJSON format
        testRunner.setProperty(PutKineticaFromJSON.PROP_JSON_FORMAT, "NDJSON");
        testRunner.assertValid();
    }

    @Test
    public void testSkipErrorsProperty() {
        testRunner.setProperty(PutKineticaFromJSON.PROP_SERVER, "http://localhost:9191");
        testRunner.setProperty(PutKineticaFromJSON.PROP_TABLE, "test_table");

        // Skip errors
        testRunner.setProperty(PutKineticaFromJSON.PROP_SKIP_ERRORS, "true");
        testRunner.assertValid();

        // Fail on first error
        testRunner.setProperty(PutKineticaFromJSON.PROP_SKIP_ERRORS, "false");
        testRunner.assertValid();
    }

    @Test
    public void testTypicalJsonArrayConfiguration() {
        testRunner.setProperty(PutKineticaFromJSON.PROP_SERVER, "http://localhost:9191");
        testRunner.setProperty(PutKineticaFromJSON.PROP_TABLE, "my_table");
        testRunner.setProperty(PutKineticaFromJSON.PROP_SCHEMA, "id|Long|data,name|String|data,value|Float|data");
        testRunner.setProperty(PutKineticaFromJSON.PROP_JSON_FORMAT, "ARRAY");
        testRunner.setProperty(PutKineticaFromJSON.PROP_SKIP_ERRORS, "true");
        testRunner.setProperty(PutKineticaFromJSON.PROP_BATCH_SIZE, "10000");

        testRunner.assertValid();
    }

    @Test
    public void testTypicalNdjsonConfiguration() {
        testRunner.setProperty(PutKineticaFromJSON.PROP_SERVER, "http://localhost:9191");
        testRunner.setProperty(PutKineticaFromJSON.PROP_TABLE, "my_table");
        testRunner.setProperty(PutKineticaFromJSON.PROP_SCHEMA, "x|Float|data,y|Float|data,label|String|data");
        testRunner.setProperty(PutKineticaFromJSON.PROP_JSON_FORMAT, "NDJSON");

        testRunner.assertValid();
    }

    @Test
    public void testProcessorHasCorrectRelationships() {
        PutKineticaFromJSON processor = (PutKineticaFromJSON) testRunner.getProcessor();

        assertTrue(processor.getRelationships().contains(PutKineticaFromJSON.REL_SUCCESS));
        assertTrue(processor.getRelationships().contains(PutKineticaFromJSON.REL_FAILURE));
        assertEquals(2, processor.getRelationships().size());
    }

    @Test
    public void testNoInputDoesNothing() {
        // Disable expression language scope validation for this test
        // since onScheduled runs before any FlowFile is available
        testRunner.setValidateExpressionUsage(false);

        testRunner.setProperty(PutKineticaFromJSON.PROP_SERVER, "http://localhost:9191");
        testRunner.setProperty(PutKineticaFromJSON.PROP_TABLE, "test_table");
        testRunner.setProperty(PutKineticaFromJSON.PROP_USERNAME, "admin");
        testRunner.setProperty(PutKineticaFromJSON.PROP_PASSWORD, "Kinetica1.");

        testRunner.run();

        testRunner.assertTransferCount(PutKineticaFromJSON.REL_SUCCESS, 0);
        testRunner.assertTransferCount(PutKineticaFromJSON.REL_FAILURE, 0);
    }

    @Test
    public void testAllJsonSpecificPropertiesExist() {
        PutKineticaFromJSON processor = (PutKineticaFromJSON) testRunner.getProcessor();

        // Verify all JSON-specific properties are available
        assertTrue(processor.getSupportedPropertyDescriptors().contains(PutKineticaFromJSON.PROP_JSON_FORMAT));
        assertTrue(processor.getSupportedPropertyDescriptors().contains(PutKineticaFromJSON.PROP_SKIP_ERRORS));
    }

    @Test
    public void testBatchSizeProperty() {
        testRunner.setProperty(PutKineticaFromJSON.PROP_SERVER, "http://localhost:9191");
        testRunner.setProperty(PutKineticaFromJSON.PROP_TABLE, "test_table");

        // Small batch size
        testRunner.setProperty(PutKineticaFromJSON.PROP_BATCH_SIZE, "100");
        testRunner.assertValid();

        // Large batch size
        testRunner.setProperty(PutKineticaFromJSON.PROP_BATCH_SIZE, "50000");
        testRunner.assertValid();
    }
}
