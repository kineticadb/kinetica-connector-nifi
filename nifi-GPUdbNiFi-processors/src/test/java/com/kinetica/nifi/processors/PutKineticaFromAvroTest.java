package com.kinetica.nifi.processors;

import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;

/**
 * Unit tests for PutKineticaFromAvro processor.
 * Tests property validation and Avro configuration options.
 */
public class PutKineticaFromAvroTest {

    private TestRunner testRunner;

    @Before
    public void setUp() {
        testRunner = TestRunners.newTestRunner(PutKineticaFromAvro.class);
    }

    @Test
    public void testProcessorIsConfigurable() {
        assertNotNull(testRunner.getProcessor());
    }

    @Test
    public void testRequiredPropertiesValidation() {
        testRunner.assertNotValid();

        testRunner.setProperty(PutKineticaFromAvro.PROP_SERVER, "http://localhost:9191");
        testRunner.assertNotValid();

        testRunner.setProperty(PutKineticaFromAvro.PROP_TABLE, "test_table");
        testRunner.assertValid();
    }

    @Test
    public void testSkipErrorsProperty() {
        testRunner.setProperty(PutKineticaFromAvro.PROP_SERVER, "http://localhost:9191");
        testRunner.setProperty(PutKineticaFromAvro.PROP_TABLE, "test_table");

        // Skip errors (default)
        testRunner.setProperty(PutKineticaFromAvro.PROP_SKIP_ERRORS, "true");
        testRunner.assertValid();

        // Fail on first error
        testRunner.setProperty(PutKineticaFromAvro.PROP_SKIP_ERRORS, "false");
        testRunner.assertValid();
    }

    @Test
    public void testTypicalAvroConfiguration() {
        testRunner.setProperty(PutKineticaFromAvro.PROP_SERVER, "http://localhost:9191");
        testRunner.setProperty(PutKineticaFromAvro.PROP_TABLE, "my_table");
        testRunner.setProperty(PutKineticaFromAvro.PROP_SCHEMA, "id|Long|data,name|String|data,value|Float|data");
        testRunner.setProperty(PutKineticaFromAvro.PROP_SKIP_ERRORS, "true");
        testRunner.setProperty(PutKineticaFromAvro.PROP_BATCH_SIZE, "10000");

        testRunner.assertValid();
    }

    @Test
    public void testProcessorHasCorrectRelationships() {
        PutKineticaFromAvro processor = (PutKineticaFromAvro) testRunner.getProcessor();

        assertTrue(processor.getRelationships().contains(PutKineticaFromAvro.REL_SUCCESS));
        assertTrue(processor.getRelationships().contains(PutKineticaFromAvro.REL_FAILURE));
        assertEquals(2, processor.getRelationships().size());
    }

    @Test
    public void testNoInputDoesNothing() {
        testRunner.setProperty(PutKineticaFromAvro.PROP_SERVER, "http://localhost:9191");
        testRunner.setProperty(PutKineticaFromAvro.PROP_TABLE, "test_table");
        testRunner.setProperty(PutKineticaFromAvro.PROP_USERNAME, "admin");
        testRunner.setProperty(PutKineticaFromAvro.PROP_PASSWORD, "Kinetica1.");

        testRunner.run();

        testRunner.assertTransferCount(PutKineticaFromAvro.REL_SUCCESS, 0);
        testRunner.assertTransferCount(PutKineticaFromAvro.REL_FAILURE, 0);
    }

    @Test
    public void testAllAvroSpecificPropertiesExist() {
        PutKineticaFromAvro processor = (PutKineticaFromAvro) testRunner.getProcessor();

        // Verify all Avro-specific properties are available
        assertTrue(processor.getSupportedPropertyDescriptors().contains(PutKineticaFromAvro.PROP_SKIP_ERRORS));
    }

    @Test
    public void testBatchSizeProperty() {
        testRunner.setProperty(PutKineticaFromAvro.PROP_SERVER, "http://localhost:9191");
        testRunner.setProperty(PutKineticaFromAvro.PROP_TABLE, "test_table");

        // Small batch size
        testRunner.setProperty(PutKineticaFromAvro.PROP_BATCH_SIZE, "100");
        testRunner.assertValid();

        // Large batch size
        testRunner.setProperty(PutKineticaFromAvro.PROP_BATCH_SIZE, "50000");
        testRunner.assertValid();
    }

    @Test
    public void testSchemaProperty() {
        testRunner.setProperty(PutKineticaFromAvro.PROP_SERVER, "http://localhost:9191");
        testRunner.setProperty(PutKineticaFromAvro.PROP_TABLE, "test_table");

        // Schema with multiple columns
        testRunner.setProperty(PutKineticaFromAvro.PROP_SCHEMA,
                "id|Long|data|primary_key,x|Float|data,y|Float|data,name|String|data");
        testRunner.assertValid();
    }

    @Test
    public void testUpdateOnExistingPkProperty() {
        testRunner.setProperty(PutKineticaFromAvro.PROP_SERVER, "http://localhost:9191");
        testRunner.setProperty(PutKineticaFromAvro.PROP_TABLE, "test_table");

        // Update on existing PK
        testRunner.setProperty(PutKineticaFromAvro.PROP_UPDATE_ON_EXISTING_PK, "true");
        testRunner.assertValid();

        // Don't update on existing PK (default)
        testRunner.setProperty(PutKineticaFromAvro.PROP_UPDATE_ON_EXISTING_PK, "false");
        testRunner.assertValid();
    }

    @Test
    public void testDateFormatProperty() {
        testRunner.setProperty(PutKineticaFromAvro.PROP_SERVER, "http://localhost:9191");
        testRunner.setProperty(PutKineticaFromAvro.PROP_TABLE, "test_table");

        // Custom date format
        testRunner.setProperty(PutKineticaFromAvro.PROP_DATE_FORMAT, "yyyy-MM-dd HH:mm:ss");
        testRunner.assertValid();
    }

    @Test
    public void testTimezoneProperty() {
        testRunner.setProperty(PutKineticaFromAvro.PROP_SERVER, "http://localhost:9191");
        testRunner.setProperty(PutKineticaFromAvro.PROP_TABLE, "test_table");

        // UTC timezone
        testRunner.setProperty(PutKineticaFromAvro.PROP_TIMEZONE, "UTC");
        testRunner.assertValid();

        // EST timezone
        testRunner.setProperty(PutKineticaFromAvro.PROP_TIMEZONE, "EST");
        testRunner.assertValid();
    }
}
